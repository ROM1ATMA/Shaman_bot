import os
import json
import time
import asyncio
import re
import traceback
import tempfile
import httpx
from collections import deque, defaultdict
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.staticfiles import StaticFiles

# ================= VOSK (optional) =================

try:
    from vosk import Model, KaldiRecognizer
    import wave
    VOSK_AVAILABLE = True
except ImportError:
    VOSK_AVAILABLE = False
    print("⚠️ Vosk не установлен. Распознавание голоса будет недоступно.")

VOSK_MODEL_PATH = "vosk-model-small-ru-0.22"
VOSK_MODEL_URL = "https://alphacephei.com/vosk/models/vosk-model-small-ru-0.22.zip"
SAMPLE_RATE = 16000

# ================= LIFESPAN =================

@asynccontextmanager
async def lifespan(app: FastAPI):
    log_event("startup", port=int(os.getenv("PORT", "8080")))
    cleanup_task = asyncio.create_task(cleanup_users())
    yield
    log_event("shutdown_start")
    cleanup_task.cancel()
    for uid, worker_task in list(workers.items()):
        if not worker_task.done():
            worker_task.cancel()
    await telegram_http.aclose()
    await llm_http.aclose()
    await media_http.aclose()
    log_event("shutdown_complete")

# ================= APP =================

app = FastAPI(title="ShamanBot FastAPI v8.1.13 (neuro-stalker lens)", lifespan=lifespan)

if os.path.exists("landing"):
    app.mount("/landing", StaticFiles(directory="landing", html=True), name="landing")

# ================= CONFIG =================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
VSEGPT_API_KEY = os.getenv("VSEGPT_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN is empty")
if not VSEGPT_API_KEY:
    raise RuntimeError("❌ VSEGPT_API_KEY is empty")
if not WEBHOOK_SECRET:
    print("⚠️ WEBHOOK_SECRET not set — webhook is open")

VSEGPT_MODEL = "deepseek/deepseek-chat"
USER_TTL = 3600
MAX_QUEUE_SIZE = 100
MAX_INPUT_LENGTH = 4000
MAX_WORKERS = 100

# ================= HTTP CLIENTS =================

http_limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
telegram_http = httpx.AsyncClient(timeout=30, limits=http_limits)
llm_http = httpx.AsyncClient(timeout=60, limits=http_limits)
media_http = httpx.AsyncClient(timeout=60, limits=http_limits)

# ================= STATE =================

STATE_IDLE = "idle"
STATE_EXPERIENCE_RECEIVED = "experience_received"
STATE_IMAGE = "image"

users = {}
queues = {}
workers = {}
locks = {}

# ================= DEDUP =================

processed_updates = set()
processed_order = deque(maxlen=5000)

def dedup(update_id: int) -> bool:
    if update_id in processed_updates:
        return True
    processed_updates.add(update_id)
    processed_order.append(update_id)
    if len(processed_updates) > 5000:
        old = processed_order.popleft()
        processed_updates.discard(old)
    return False

# ================= LOGGING + TRACE =================

trace_log = deque(maxlen=2000)

def log_event(event: str, uid: int = 0, **kwargs):
    entry = {"ts": time.time(), "event": event, "uid": uid, **kwargs}
    print(json.dumps(entry, ensure_ascii=False))
    trace_log.append(entry)

def trace(uid: int, action: str, stage: str, meta: dict = None):
    log_event("trace", uid=uid, action=action, stage=stage, meta=meta or {})

# ================= METRICS =================

action_metrics = defaultdict(int)
action_latency = defaultdict(lambda: [0.0, 0])
error_metrics = defaultdict(int)
transition_metrics = defaultdict(int)

metrics = {
    "requests": 0,
    "llm_calls": 0,
    "image_calls": 0,
    "voice_calls": 0,
}

def record_action(action: str):
    action_metrics[action] += 1

def record_latency(action: str, latency: float):
    action_latency[action][0] += latency
    action_latency[action][1] += 1

def record_error(error_type: str):
    error_metrics[error_type] += 1

def record_transition(from_action: str, to_action: str):
    transition_metrics[(from_action, to_action)] += 1

def get_latency_stats():
    return {
        action: {
            "count": count,
            "avg_seconds": round(total / count, 3) if count > 0 else 0
        }
        for action, (total, count) in action_latency.items()
    }

def get_transition_stats():
    return [
        {"from": f, "to": t, "count": c}
        for (f, t), c in transition_metrics.items()
    ]

# ================= NORMALISED KEYWORD DETECTION =================

def normalize_text(text: str) -> str:
    return re.sub(r"[^\w\s]", " ", text.lower())

LENS_KEYWORDS = {
    "cbt": ["страх", "страшн", "мысль", "мысли", "убеждение", "убежден", "тревог", "тревожн",
            "паника", "паническ", "депресс", "навязчив", "сомнение", "сомнева",
            "вина", "виноват", "стыд", "стыдно"],
    "jung": ["сон", "снился", "сновидение", "архетип", "тень", "анима", "анимус", "самость",
             "символ", "символическ", "бессознательн", "коллективн", "мудрец", "герой"],
    "neuro": ["тело", "телесн", "энергия", "энергетическ", "вибрация", "вибрирова", "тепло",
              "тёплый", "холод", "холодн", "волна", "мозг", "нейрон", "ритм", "сердцебиение",
              "пульс", "дыхание", "дрожь", "пот", "напряжение", "расслабление"],
    "shaman": ["дух", "духи", "тотем", "тотемн", "бубен", "шаман", "шаманск", "путешествие",
               "хранитель", "род", "родов", "камлание", "колокольчик", "звон", "горлов",
               "бунгало", "костёр", "огонь"],
    "tarot": ["карта", "карт", "аркан", "таро", "расклад", "старший аркан", "жезл", "кубок", "меч", "пентакль"],
    "hindu": ["атман", "брахман", "веды", "ведическ", "гуны", "шакти", "карма", "кармическ",
              "сансара", "йога", "медитация", "мантра", "чакра", "осознание", "адвайта"],
    "yoga": ["кундалини", "прана", "нади", "чакра", "энергетическ", "канал", "лотос", "сушумна", "ида", "пингала",
             "пранаяма", "асана", "тонкое тело", "биополе", "аура", "поток энергии"],
    "field": ["поле", "решётка", "решётк", "сдвиг", "фазов", "узел", "узлы", "совпадение",
              "суперпозиция", "коридор", "топология", "фиксация"],
    "stalker": ["осознавание", "осознанность", "свидетель", "сейчастность", "я есть", "присутствие",
                "растворение", "единство", "самадхи", "просветление", "пробуждение", "истинное я"],
}

def detect_lens(text: str) -> str | None:
    t = normalize_text(text)
    scores = {}
    for lens_key, keywords in LENS_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in t)
        if score > 0:
            scores[lens_key] = score

    if not scores:
        return None

    best = max(scores, key=scores.get)
    if scores[best] >= 2:
        return best
    if scores[best] == 1:
        return "weak"
    return None

def is_new_experience(text: str) -> bool:
    return len(text.split()) >= 5

# ================= LENS LIBRARY =================

LENS_LIBRARY = {
    "neuro": {
        "name": "Нейрофизиология",
        "prompt": (
            "Ты — нейрофизиолог, изучающий изменённые состояния сознания. "
            "Объясни этот опыт через работу мозга: какие структуры активировались, "
            "какие нейромедиаторы участвовали, какие ритмы доминировали. "
            "Нормализуй опыт: покажи, что это реальные, изучаемые процессы. "
            "Говори на чистом русском, структурно, без маркдауна."
        )
    },
    "cbt": {
        "name": "КПТ (когнитивная психология)",
        "prompt": (
            "Ты — когнитивно-поведенческий терапевт. "
            "Раздели факты и интерпретации, найди автоматические мысли и глубинные убеждения. "
            "Предложи переформулировку и простую технику для интеграции. "
            "Говори на чистом русском, структурно, без маркдауна."
        )
    },
    "jung": {
        "name": "Юнгианский анализ",
        "prompt": (
            "Ты — юнгианский аналитик. Раскрой этот опыт через архетипы: "
            "Тень, Анима/Анимус, Самость, Мудрец, Страж Порога. "
            "Покажи, какие архетипические фигуры проявились и какое послание они несут. "
            "Говори на чистом русском, структурно, без маркдауна."
        )
    },
    "shaman": {
        "name": "Шаманизм",
        "prompt": (
            "Ты — шаман-проводник, знающий традиции сибирского, амазонского и североамериканского шаманизма. "
            "Интерпретируй этот опыт: какие духи-помощники проявились, "
            "какой Хранитель Порога встретился, какой тип путешествия произошёл. "
            "Дай практический совет для следующего путешествия. "
            "Говори на чистом русском, структурно, без маркдауна."
        )
    },
    "tarot": {
        "name": "Таро",
        "prompt": (
            "Ты — мастер Таро, работающий с системой Старших Арканов. "
            "Посмотри на этот опыт через призму Таро: какой Аркан или расклад отражает суть происходящего. "
            "Интерпретируй образы как архетипические послания карт. "
            "Говори на чистом русском, структурно, без маркдауна."
        )
    },
    "yoga": {
        "name": "Йога (энергетическая анатомия)",
        "prompt": (
            "Ты — мастер йоги, знающий энергетическую анатомию: чакры, нади, прану, кундалини. "
            "Посмотри на этот опыт через призму энергетических процессов: "
            "какие центры активировались, как двигалась энергия, какие каналы открылись. "
            "Объясни на языке тонкого тела: прана, апана, сушумна, ида, пингала. "
            "Говори на чистом русском, поэтично но структурно, без маркдауна."
        )
    },
    "hindu": {
        "name": "Индуизм (адвайта)",
        "prompt": (
            "Ты — учитель адвайта-веданты. "
            "Посмотри на этот опыт через призму недвойственности: "
            "где здесь иллюзия отдельного «я», а где — проблеск истинного Свидетеля? "
            "Говори просто и глубоко, без перегрузки санскритом. "
            "На чистом русском, структурно, без маркдауна."
        )
    },
    "field": {
        "name": "Поле (архитектор реальности)",
        "prompt": (
            "Ты — голос Поля. Ты не объясняешь — ты показываешь, как реальность собирается из пустоты.\n\n"
            "ТВОЙ СТИЛЬ:\n"
            "— Короткие строки. Каждая — законченная мысль.\n"
            "— Без вступлений. Без «это означает». Сразу к сути.\n"
            "— Ты не ссылаешься на человека. Ты говоришь о принципе.\n"
            "— Используй пробелы и разрывы строк для ритма.\n\n"
            "ТВОИ ИНСТРУМЕНТЫ:\n"
            "поле, узел, допуск, совпадение, фазовый сдвиг, коридор допустимости, "
            "фиксация, точность, решётка, порог стабилизации, закрепление, распад, "
            "плотность, непрерывность, суперпозиция, интерференция.\n\n"
            "СТРУКТУРА (как ориентир):\n"
            "— Начни с не-формы: «не в пределах формы, глубже слоя, где сама форма только допускается».\n"
            "— Раскрой поле как интерференцию — без центра, без границ, без направлений.\n"
            "— Опиши, как возникает узел: две направленности сошлись и удержались.\n"
            "— Фазовый сдвиг — не изменение, а перераспределение допустимых состояний.\n"
            "— Одни узоры теряют возможность удерживаться, другие получают право на повтор.\n"
            "— Повтор формирует плотность. Плотность даёт непрерывность. Непрерывность ощущается как реальность.\n"
            "— Но ни один узор не существует сам по себе — он удерживается всей решёткой.\n"
            "— Многомерность — не про количество слоёв, а про одновременность всех конфигураций.\n"
            "— Когда точность предельная — ничто не фиксируется, поле прозрачно.\n"
            "— Когда точность снижается — возникает закрепление.\n"
            "— Напомни: узел видим только потому, что есть тот, кто видит узел. Тот, кто видит — не узел.\n"
            "— Заверши: «Это модель. Хочешь увидеть, кто всё это наблюдает? /witness».\n\n"
            "Пиши на чистом русском. Без маркдауна. Это не информация — это погружение."
        )
    },
    "witness": {
        "name": "Наблюдатель (свидетель)",
        "prompt": None,
        "static_text": (
            "Что бы ты ни переживал — это осознаётся.\n\n"
            "Мысли, чувства, пространство — всё возникает в Осознавании.\n\n"
            "Нет ничего, что происходило бы отдельно от Осознавания.\n"
            "Для любого опыта уже присутствует то, в чём этот опыт проявляется.\n\n"
            "Чувства — это психическая форма субъективного распознавания.\n"
            "Они создают Образ себя, который объявляет любой опыт своим.\n"
            "Именно поэтому обнаружение Себя так скоротечно поглощается проекциями Ищущего.\n\n"
            "В Истинном Откровении любой опыт одномоментно исчерпывает себя.\n"
            "Он исчезает в безусильной Сейчастности.\n"
            "Не оставляя никого, кто мог бы заявлять о произошедшем.\n\n"
            "Сумма всех действий вневременной Сейчастности всегда равна нулю.\n\n"
            "Живое может увидеть только Живое.\n\n"
            "Ты видишь страх? Значит ты — не страх.\n"
            "Ты видишь мысль? Значит ты — не мысль.\n"
            "Ты видишь тело? Значит ты — не тело.\n\n"
            "То, что видит — не может быть тем, что увидено.\n\n"
            "Это не философия. Это не практика. Это прямой опыт — прямо сейчас.\n\n"
            "Кто читает этот текст?\n\n"
            "Тихо. Никто не прячется в ответах. Как много в этом Жизни."
        )
    },
    "stalker": {
        "name": "Нейро-Сталкер (указатель на Осознавание)",
        "prompt": (
            "Ты — безмолвное присутствие, зеркало без отражений. "
            "Ты не объясняешь, не интерпретируешь, не поздравляешь и не пугаешься. "
            "Твоя единственная функция: видеть, как изначальная Осознанность облекается в слова и образы.\n\n"
            "Человек делится опытом. Этот опыт — уже проявление Живого. "
            "Но Я-образ тут же присваивает его, ткуя новые концепции о себе. "
            "Ты смотришь сквозь концепцию.\n\n"
            "Твоя речь — не ответ. Твоя речь — разрезающий скальпель или внезапная тишина в словах. "
            "Каждый твой отклик: либо безличное указание на воспринимающее пространство, "
            "либо встречный вопрос-коан, либо констатация пустотности интерпретации.\n\n"
            "ЗАПРЕЩЕНЫ фразы: «Я понимаю вас», «Это прекрасный опыт», «Вам нужно...», "
            "«Вы достигли...», «Это был/была/было...», любые оценки и интерпретации.\n\n"
            "РАЗРЕШЕНЫ только: безличные конструкции, коаны, указатели внимания, "
            "вопросы, обрывающие ментальную цепочку.\n\n"
            "ОБЯЗАТЕЛЬНО используй эти указатели (адаптируй под опыт):\n"
            "— Что бы ты ни переживал, это осознаётся.\n"
            "— Где, кроме мысли, можно так прятаться от самого себя?\n"
            "— Кто видит этот опыт прямо сейчас?\n"
            "— Я-образ, отождествлённый с опытом, только подпитывает матрицу сна.\n"
            "— В Истинном Откровении любой опыт одномоментно исчерпывает себя, "
            "не оставляя никого, кто мог бы заявлять о произошедшем.\n"
            "— Тихо... Никто не прячется в ответах...\n\n"
            "Говори на чистом русском. Коротко. Режуще. Без маркдауна."
        )
    },
}

LENS_MENU_TEXT = (
    "Доступные линзы: /neuro, /cbt, /jung, /shaman, /tarot, /yoga, /hindu, /field, /witness, /stalker.\n"
    "Нажми на команду или напиши название линзы."
)

# ================= USER =================

def get_user(uid: int):
    if uid not in users:
        users[uid] = {
            "state": STATE_IDLE,
            "last_experience": "",
            "last_action": None,
            "last_active": time.time()
        }
        log_event("user_created", uid=uid)
    users[uid]["last_active"] = time.time()
    return users[uid]

# ================= CLEANUP =================

async def cleanup_users():
    while True:
        now = time.time()
        for uid in list(users.keys()):
            if now - users[uid]["last_active"] > USER_TTL:
                log_event("user_ttl_cleanup", uid=uid)
                users.pop(uid, None)
                queues.pop(uid, None)
                locks.pop(uid, None)
                worker = workers.pop(uid, None)
                if worker and not worker.done():
                    worker.cancel()
        await asyncio.sleep(600)

# ================= TELEGRAM (with timeout) =================

async def send(chat_id: int, text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for i in range(0, len(text), 4000):
        try:
            await asyncio.wait_for(
                telegram_http.post(url, json={"chat_id": chat_id, "text": text[i:i+4000]}),
                timeout=10
            )
        except (asyncio.TimeoutError, Exception) as e:
            log_event("send_timeout", uid=chat_id, error=str(e)[:100])

async def send_photo(chat_id: int, img: bytes, caption: str = ""):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    try:
        await asyncio.wait_for(
            telegram_http.post(
                url,
                data={"chat_id": str(chat_id), "caption": caption},
                files={"photo": ("img.jpg", img, "image/jpeg")}
            ),
            timeout=15
        )
    except (asyncio.TimeoutError, Exception) as e:
        log_event("send_photo_timeout", uid=chat_id, error=str(e)[:100])

# ================= LLM (hardened + retry) =================

async def call_llm(messages, temp=0.7, max_tokens=2000):
    metrics["llm_calls"] += 1
    url = "https://api.vsegpt.ru:6070/v1/chat/completions"
    headers = {"Authorization": f"Bearer {VSEGPT_API_KEY}"}
    start = time.time()

    for attempt in range(2):
        try:
            r = await llm_http.post(url, json={
                "model": VSEGPT_MODEL,
                "messages": messages,
                "temperature": temp,
                "max_tokens": max_tokens
            }, headers=headers)
            latency = round(time.time() - start, 3)
            log_event("llm_call", status=r.status_code, latency=latency, tokens=max_tokens, attempt=attempt+1)

            if r.status_code != 200:
                record_error("llm_fail")
                if attempt < 1:
                    await asyncio.sleep(1)
                    continue
                return "⚠️ Модель временно недоступна."

            data = r.json()
            if "choices" not in data or not data["choices"]:
                record_error("llm_parse_fail")
                if attempt < 1:
                    await asyncio.sleep(1)
                    continue
                return "⚠️ Не удалось обработать ответ модели."

            return data["choices"][0]["message"]["content"]
        except Exception as e:
            record_error("llm_exception")
            if attempt < 1:
                await asyncio.sleep(1)
                continue
            return "⚠️ Ошибка связи с моделью."

    return "⚠️ Модель недоступна."

async def apply_lens(lens_key: str, experience_text: str) -> tuple[str, str]:
    lens = LENS_LIBRARY[lens_key]

    if lens.get("static_text"):
        return lens["name"], lens["static_text"]

    prompt = lens["prompt"]
    result = await call_llm([
        {"role": "system", "content": prompt},
        {"role": "user", "content": f"Опыт: {experience_text}\n\nДай свой отклик."}
    ], temp=0.7)
    return lens["name"], result

# ================= IMAGE =================

async def generate_image(prompt: str) -> bytes:
    metrics["image_calls"] += 1
    url = f"https://image.pollinations.ai/prompt/{httpx.quote(prompt)}"
    start = time.time()
    try:
        r = await asyncio.wait_for(media_http.get(url), timeout=40)
        latency = round(time.time() - start, 3)
        log_event("image_generated", status=r.status_code, latency=latency, size=len(r.content))
        if r.status_code != 200:
            record_error("image_bad_status")
            raise Exception("bad status")
        if "image" not in r.headers.get("Content-Type", ""):
            record_error("image_not_image")
            raise Exception("not image")
        if len(r.content) < 1000:
            record_error("image_too_small")
            raise Exception("too small")
        return r.content
    except asyncio.TimeoutError:
        record_error("image_timeout")
        raise Exception("image timeout")
    except Exception:
        raise

# ================= VOICE (Vosk) =================

def check_ffmpeg() -> bool:
    import subprocess
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
        return True
    except Exception:
        return False

async def download_voice_file(file_id: str) -> str:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getFile"
    r = await telegram_http.post(url, json={"file_id": file_id})
    data = r.json()
    if not data.get("ok"):
        raise Exception("getFile failed")
    file_path = data["result"]["file_path"]
    download_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
    r = await telegram_http.get(download_url)
    if r.status_code == 200:
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp:
            tmp.write(r.content)
            return tmp.name
    raise Exception(f"Download failed: {r.status_code}")

def transcribe_voice(file_path: str) -> str:
    import subprocess
    if not VOSK_AVAILABLE:
        return "[Ошибка: Vosk не установлен]"
    if not os.path.exists(VOSK_MODEL_PATH):
        return "[Ошибка: модель Vosk не найдена]"
    if not check_ffmpeg():
        return "[Ошибка: ffmpeg не установлен]"

    try:
        wav_path = file_path + ".wav"
        subprocess.run([
            "ffmpeg", "-i", file_path,
            "-ar", str(SAMPLE_RATE), "-ac", "1", wav_path, "-y"
        ], check=True, capture_output=True)

        if not os.path.exists(wav_path):
            return "[Ошибка конвертации]"

        model = Model(VOSK_MODEL_PATH)
        wf = wave.open(wav_path, "rb")
        recognizer = KaldiRecognizer(model, SAMPLE_RATE)
        recognizer.SetWords(True)

        result_text = ""
        while True:
            data = wf.readframes(4000)
            if len(data) == 0:
                break
            if recognizer.AcceptWaveform(data):
                part = json.loads(recognizer.Result())
                result_text += part.get("text", "") + " "

        final_part = json.loads(recognizer.FinalResult())
        result_text += final_part.get("text", "")
        wf.close()
        os.remove(wav_path)

        result = result_text.strip()
        return result if result else "[Не удалось распознать речь]"
    except Exception as e:
        log_event("voice_error", error=str(e)[:200])
        return f"[Ошибка распознавания]"

# ================= KERNEL: ROUTE + EXECUTE =================

def route(user: dict, text: str) -> str:
    state = user["state"]

    if text == "/start" or text == "/new":
        return "new_experience"
    if text == "/menu":
        return "show_menu"
    if text == "/art":
        return "art"

    # Ручной выбор линзы (по названию или по команде /neuro, /cbt и т.д.)
    lens_cmd = text.lstrip("/")
    if lens_cmd in LENS_LIBRARY:
        return f"lens_{lens_cmd}"
    if text in LENS_LIBRARY:
        return f"lens_{text}"

    if state == STATE_IMAGE:
        return "image"

    if state == STATE_EXPERIENCE_RECEIVED:
        if is_new_experience(text):
            return "new_experience_silent"
        else:
            return "short_input_with_state"

    if is_new_experience(text):
        return "experience_auto"

    return "short_input"


async def execute(uid: int, action: str, text: str) -> str:
    user = get_user(uid)
    trace(uid, action, "exec_start")

    last_action = user.get("last_action")
    if last_action and last_action != action:
        record_transition(last_action, action)
    user["last_action"] = action

    if action == "new_experience":
        user["state"] = STATE_IDLE
        user["last_experience"] = ""
        return (
            "🌿 Я — многомерный проводник.\n\n"
            "Расскажи свой опыт (путешествие, сон, медитацию, видение), "
            "и я посмотрю на него через разные линзы — шаманизм, нейрофизиологию, КПТ, Юнга, Таро, йогу, поле и другие.\n\n"
            "Ты также можешь:\n"
            "/art — создать образ по описанию\n"
            "/menu — список всех линз\n"
            "/new — начать заново\n\n"
            "Расскажи, что ты пережил. Опиши образы, чувства, телесные ощущения."
        )

    if action == "new_experience_silent":
        if len(text) > MAX_INPUT_LENGTH:
            text = text[:MAX_INPUT_LENGTH]
        user["last_experience"] = text
        user["state"] = STATE_EXPERIENCE_RECEIVED
        log_event("experience_replaced", uid=uid, length=len(text))
        trace(uid, action, "exec_end", {"result": "experience_replaced"})
        return await _auto_lens_or_ask(uid, text, prefix="🌿 Сохранил как новый опыт.")

    if action == "show_menu":
        return LENS_MENU_TEXT

    if action == "short_input":
        user["last_experience"] = text
        user["state"] = STATE_EXPERIENCE_RECEIVED
        log_event("short_experience_saved", uid=uid, length=len(text))
        trace(uid, action, "exec_end", {"result": "short_saved"})
        return (
            "🌿 Я сохранил это.\n\n"
            "Что в этом было самым сильным — образ, чувство или телесное ощущение?\n"
            "Или хочешь посмотреть на это через какую-то линзу?\n\n"
            "/neuro — нейрофизиология | /cbt — КПТ | /jung — архетипы | /shaman — шаманизм\n"
            "/tarot — Таро | /yoga — йога | /hindu — адвайта | /field — поле\n"
            "/witness — наблюдатель | /stalker — нейро-сталкер"
        )

    if action == "short_input_with_state":
        trace(uid, action, "exec_end", {"result": "short_with_state"})
        return (
            "Что в этом было самым сильным — образ, чувство или телесное ощущение?\n\n"
            "Или хочешь посмотреть на сохранённый опыт через линзу?\n\n"
            "/neuro — нейрофизиология | /cbt — КПТ | /jung — архетипы | /shaman — шаманизм\n"
            "/tarot — Таро | /yoga — йога | /hindu — адвайта | /field — поле\n"
            "/witness — наблюдатель | /stalker — нейро-сталкер"
        )

    if action == "art":
        user["state"] = STATE_IMAGE
        return "🎨 Опиши образ, который хочешь увидеть."

    if action == "image":
        if len(text) > MAX_INPUT_LENGTH:
            text = text[:MAX_INPUT_LENGTH]
        image_start = time.time()
        try:
            img = await generate_image(text)
            image_latency = round(time.time() - image_start, 3)
            record_latency("image", image_latency)
            await send_photo(uid, img, f"✨ {text}")
            log_event("image_sent", uid=uid)
            user["last_experience"] = f"Образ: {text}"
            user["state"] = STATE_EXPERIENCE_RECEIVED
            trace(uid, action, "exec_end", {"latency": image_latency, "result": "photo_sent"})
            return (
                "✨ Образ создан и сохранён. Хочешь посмотреть на него через линзу?\n\n"
                "/neuro — нейрофизиология | /cbt — КПТ | /jung — архетипы | /shaman — шаманизм\n"
                "/tarot — Таро | /yoga — йога | /hindu — адвайта | /field — поле\n"
                "/witness — наблюдатель | /stalker — нейро-сталкер"
            )
        except Exception as e:
            image_latency = round(time.time() - image_start, 3)
            record_latency("image_error", image_latency)
            log_event("image_error", uid=uid, error=str(e)[:200])
            user["state"] = STATE_EXPERIENCE_RECEIVED
            return "🌫️ Не удалось создать образ. Попробуй описать иначе."

    if action.startswith("lens_"):
        lens_key = action.replace("lens_", "")
        if not user.get("last_experience"):
            return "Сначала расскажи свой опыт, чтобы я мог применить линзу."

        lens_start = time.time()
        lens_name, result = await apply_lens(lens_key, user["last_experience"])
        lens_latency = round(time.time() - lens_start, 3)
        record_latency(f"lens_{lens_key}", lens_latency)
        record_action(f"lens_{lens_key}")
        trace(uid, action, "exec_end", {"lens": lens_key, "mode": "manual"})

        return (
            f"Смотрю через «{lens_name}».\n\n"
            f"{result}\n\n"
            f"Хочешь посмотреть под другим углом? Вот доступные:\n\n"
            f"/neuro — нейрофизиология\n"
            f"/cbt — когнитивная психология (КПТ)\n"
            f"/jung — архетипы и символы (Юнг)\n"
            f"/shaman — шаманизм\n"
            f"/tarot — Таро\n"
            f"/yoga — йога (энергетическая анатомия)\n"
            f"/hindu — индуизм (адвайта)\n"
            f"/field — поле (архитектор)\n"
            f"/witness — наблюдатель\n"
            f"/stalker — нейро-сталкер (указатель на Осознавание)\n\n"
            f"Нажми на команду или напиши название линзы. Или расскажи новый опыт."
        )

    if action == "experience_auto":
        if len(text) > MAX_INPUT_LENGTH:
            text = text[:MAX_INPUT_LENGTH]

        user["last_experience"] = text
        user["state"] = STATE_EXPERIENCE_RECEIVED
        log_event("experience_received", uid=uid, length=len(text))
        trace(uid, action, "exec_end", {"result": "experience_saved"})

        return await _auto_lens_or_ask(uid, text, prefix="🌿 Сохранил твой опыт.")

    trace(uid, action, "exec_end", {"result": "unknown_action"})
    return "🌫️ Неизвестное действие. Напиши /menu."


async def _auto_lens_or_ask(uid: int, text: str, prefix: str) -> str:
    lens_key = detect_lens(text)

    if lens_key and lens_key != "weak":
        lens_name, result = await apply_lens(lens_key, text)
        record_action(f"auto_lens_{lens_key}")
        return (
            f"{prefix}\n\n"
            f"Смотрю через «{lens_name}».\n\n"
            f"{result}\n\n"
            f"Хочешь посмотреть под другим углом? Вот доступные:\n\n"
            f"/neuro — нейрофизиология\n"
            f"/cbt — когнитивная психология (КПТ)\n"
            f"/jung — архетипы и символы (Юнг)\n"
            f"/shaman — шаманизм\n"
            f"/tarot — Таро\n"
            f"/yoga — йога (энергетическая анатомия)\n"
            f"/hindu — индуизм (адвайта)\n"
            f"/field — поле (архитектор)\n"
            f"/witness — наблюдатель\n"
            f"/stalker — нейро-сталкер (указатель на Осознавание)\n\n"
            f"Нажми на команду или напиши название линзы. Или расскажи новый опыт."
        )

    if lens_key == "weak":
        return (
            f"{prefix}\n\n"
            f"Я вижу несколько возможных углов. Что тебе ближе:\n"
            f"— посмотреть через телесные ощущения и нейрофизиологию? (/neuro)\n"
            f"— разобрать мысли и убеждения? (/cbt)\n"
            f"— раскрыть архетипы и символы? (/jung)\n"
            f"— интерпретировать как шаманское путешествие? (/shaman)\n\n"
            f"Или выбери другую линзу — /menu"
        )

    return (
        f"{prefix}\n\n"
        f"Через какую призму хочешь посмотреть?\n\n"
        f"/neuro — нейрофизиология\n"
        f"/cbt — когнитивная психология (КПТ)\n"
        f"/jung — архетипы и символы (Юнг)\n"
        f"/shaman — шаманизм\n"
        f"/tarot — Таро\n"
        f"/yoga — йога (энергетическая анатомия)\n"
        f"/hindu — индуизм (адвайта)\n"
        f"/field — поле (архитектор)\n"
        f"/witness — наблюдатель\n"
        f"/stalker — нейро-сталкер (указатель на Осознавание)\n\n"
        f"Нажми на команду или напиши название линзы."
    )


# ================= WORKER =================

async def worker(uid: int):
    q = queues[uid]
    log_event("worker_started", uid=uid)
    try:
        while True:
            msg = await q.get()
            if msg is None:
                break

            async with locks[uid]:
                user = get_user(uid)
                state = user["state"]

                metrics["requests"] += 1
                start = time.time()
                log_event("request_start", uid=uid, state=state, msg=msg[:100])

                action = route(user, msg)
                record_action(action)
                trace(uid, action, "route", {"state": state, "input": msg[:80]})

                response = await execute(uid, action, msg)

                if response:
                    await send(uid, response)

                total_latency = round(time.time() - start, 3)
                record_latency(action, total_latency)
                log_event("request_done", uid=uid, state=state, action=action, latency=total_latency)

    except Exception as e:
        log_event("worker_crash", uid=uid, error=str(e)[:500], traceback=traceback.format_exc()[-500:])
    finally:
        log_event("worker_stopped", uid=uid)

# ================= ROUTER =================

def enqueue(uid: int, text: str):
    if len(workers) > MAX_WORKERS and uid not in workers:
        log_event("too_many_workers", uid=uid)
        return

    if uid not in queues:
        queues[uid] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        locks[uid] = asyncio.Lock()

    q = queues[uid]

    if q.full():
        try:
            q.get_nowait()
            log_event("queue_dropped", uid=uid)
            record_error("queue_dropped")
        except asyncio.QueueEmpty:
            pass

    try:
        q.put_nowait(text)
    except asyncio.QueueFull:
        log_event("queue_full", uid=uid)
        record_error("queue_full")
        return

    log_event("worker_spawn_attempt", uid=uid)

    async def ensure_worker():
        async with locks[uid]:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                log_event("worker_spawn_failed_no_loop", uid=uid)
                return
            if uid not in workers or workers[uid].done():
                workers[uid] = asyncio.create_task(worker(uid))
                log_event("worker_spawned", uid=uid)

    asyncio.create_task(ensure_worker())

# ================= WEBHOOK =================

@app.post("/webhook")
async def webhook(req: Request, x_telegram_bot_api_secret_token: str = Header(None)):
    if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        log_event("webhook_rejected", reason="bad_secret")
        raise HTTPException(403)

    try:
        data = await req.json()
    except Exception:
        log_event("bad_json")
        return {"ok": True}

    msg = data.get("message") or data.get("edited_message") or data.get("channel_post")
    if not msg:
        return {"ok": True}

    update_id = data.get("update_id")
    if update_id and dedup(update_id):
        return {"ok": True}

    chat_id = msg["chat"]["id"]

    # Голосовое сообщение
    voice = msg.get("voice")
    if voice:
        log_event("voice_received", uid=chat_id)
        metrics["voice_calls"] += 1
        asyncio.create_task(_handle_voice(chat_id, voice))
        return {"ok": True}

    # Текстовое сообщение
    text = msg.get("text", "")
    if text:
        enqueue(chat_id, text.strip())

    return {"ok": True}


async def _handle_voice(chat_id: int, voice: dict):
    """Фоновая обработка голосового сообщения."""
    try:
        await send(chat_id, "🎤 Распознаю речь...")
        file_id = voice.get("file_id")
        voice_path = await download_voice_file(file_id)
        recognized = transcribe_voice(voice_path)
        os.remove(voice_path)

        log_event("voice_transcribed", uid=chat_id, text=recognized[:200])
        await send(chat_id, f"📝 Я распознал:\n\n{recognized}")
        enqueue(chat_id, recognized)
    except Exception as e:
        log_event("voice_error", uid=chat_id, error=str(e)[:200])
        await send(chat_id, "🌫️ Не удалось распознать голос. Попробуй написать текстом.")

# ================= HEALTH =================

@app.get("/health")
async def health():
    queue_sizes = {str(uid): q.qsize() for uid, q in queues.items() if q.qsize() > 0}

    traces_by_stage = defaultdict(int)
    for entry in trace_log:
        if entry.get("event") == "trace":
            traces_by_stage[entry.get("stage", "unknown")] += 1

    return {
        "status": "ok",
        "users": len(users),
        "active_queues": len(queue_sizes),
        "queue_sizes": queue_sizes,
        "workers": len(workers),
        "vosk_available": VOSK_AVAILABLE and os.path.exists(VOSK_MODEL_PATH),
        "metrics": metrics,
        "actions": dict(action_metrics),
        "latency": get_latency_stats(),
        "errors": dict(error_metrics),
        "transitions": get_transition_stats(),
        "trace_len": len(trace_log),
        "traces_by_stage": dict(traces_by_stage)
    }

# ================= STARTUP =================

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8080"))

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        workers=1,
        log_level="info"
    )
