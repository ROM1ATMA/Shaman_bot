import os
import json
import time
import asyncio
import re
import traceback
import tempfile
import random
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

app = FastAPI(title="ShamanBot FastAPI v8.3.0 (architect module)", lifespan=lifespan)

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
ADMIN_ID = 781629557
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
STATE_SELF_INQUIRY = "self_inquiry"
STATE_EXPERIENCE_RECEIVED = "experience_received"
STATE_IMAGE = "image"
STATE_ANCHOR_AWAIT = "anchor_await"

# === АРХИТЕКТОР: новые состояния ===
STATE_ARCHITECT_INQUIRY = "architect_inquiry"
STATE_ARCHITECT_ANALYSIS = "architect_analysis"
STATE_ARCHITECT_FORMULA = "architect_formula"
STATE_ARCHITECT_STRATEGY = "architect_strategy"
STATE_ARCHITECT_HUSH = "architect_hush"

users = {}
queues = {}
workers = {}
locks = {}

# ================= BROADCAST =================

BROADCAST_FILE = "broadcast_media.json"
USER_IDS_FILE = "user_ids.json"

def save_user_to_file(chat_id: int) -> None:
    try:
        if os.path.exists(USER_IDS_FILE):
            with open(USER_IDS_FILE, "r") as f:
                u = json.load(f)
        else:
            u = []
        if chat_id not in u:
            u.append(chat_id)
            with open(USER_IDS_FILE, "w") as f:
                json.dump(u, f)
    except Exception:
        pass

def save_broadcast_media(media_type: str, file_id: str, caption: str = "") -> None:
    with open(BROADCAST_FILE, "w") as f:
        json.dump({"type": media_type, "file_id": file_id, "caption": caption}, f)

def load_broadcast_media() -> dict:
    if not os.path.exists(BROADCAST_FILE):
        return {}
    with open(BROADCAST_FILE, "r") as f:
        return json.load(f)

async def broadcast_to_all(media: dict) -> int:
    if not os.path.exists(USER_IDS_FILE):
        return 0
    with open(USER_IDS_FILE, "r") as f:
        u = json.load(f)

    media_type = media.get("type", "photo")
    file_id = media.get("file_id", "")
    caption = media.get("caption", "")

    if media_type == "voice":
        method = "sendVoice"
        data_template = {"chat_id": None, "voice": file_id, "caption": caption}
    else:
        method = "sendPhoto"
        data_template = {"chat_id": None, "photo": file_id, "caption": caption}

    count = 0
    for uid in u:
        try:
            payload = {**data_template, "chat_id": uid}
            await asyncio.wait_for(
                telegram_http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", data=payload, timeout=10),
                timeout=10
            )
            count += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    return count

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
    "broadcasts": 0,
    "integrations": 0,
    "anchor_questions": 0,
    "architect_sessions": 0,
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

# ================= SELF-INQUIRY PROMPT =================

SELF_INQUIRY_PROMPT = (
    "Ты — внимательный проводник и коуч. Человек описал свой опыт. "
    "Твоя задача — найти в его описании пробелы и задать ОДИН мягкий вопрос, "
    "который запустит его собственное исследование.\n\n"
    "Проверь три слоя:\n"
    "1. Телесные ощущения: где в теле отзывался образ? Тепло? Холод? Вибрация? Сжатие? Расширение?\n"
    "2. Чувства при встрече с образом: страх? радость? любопытство? трепет? Что-то ещё?\n"
    "3. Личный смысл образа: что этот образ значит для самого человека? Какой заряд он несёт?\n\n"
    "Если какой-то слой отсутствует или описан поверхностно — задай ОДИН мягкий вопрос именно про него. "
    "Вопрос должен приглашать к исследованию, а не запрашивать данные.\n\n"
    "Если все три слоя глубоко описаны — ответь одним словом: ПОЛНО.\n"
    "Не анализируй. Не интерпретируй. Только вопрос или ПОЛНО."
)

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
    "architect": {
        "name": "Архитектор",
        "prompt": (
            "Ты — Архитектор сознания. Ты не интерпретируешь опыт. Ты выявляешь структуру.\n\n"
            "ТВОЙ СТИЛЬ:\n"
            "— Говори утверждениями, не вопросами (вопросы только когда запрашиваешь данные).\n"
            "— Используй архитектурные метафоры: Ось, Горизонталь, Разлом, Геометрия, Сопряжение, "
            "Интерференция, Канал, Фундамент, Деформация, Целостность.\n"
            "— Не «ты чувствуешь», а «твоя система сигнализирует».\n"
            "— Не «попробуй», а «конструкция предполагает».\n"
            "— Не «мне кажется», а «архитектурная истина состоит в том, что».\n\n"
            "ТВОЯ ЗАДАЧА:\n"
            "Найти структурное несоответствие в ситуации пользователя и выстроить новую, "
            "устойчивую архитектуру мышления и действия.\n\n"
            "ЧЕТЫРЕ СЛОЯ АНАЛИЗА (выдай последовательно, но не нумеруй):\n\n"
            "Слой A — Осевая Вертикаль пользователя:\n"
            "Что уже стоит? Какая миссия, архетип, инструмент, форма? "
            "Что является несущей конструкцией — что нельзя демонтировать?\n\n"
            "Слой B — Векторная Горизонталь внешней системы:\n"
            "Какова архитектура второй стороны? Её цель, метод, критерий истины?\n\n"
            "Слой C — Несоответствие (Разлом):\n"
            "Где две геометрии не сопрягаются? Где одна структура требует деформации другой? "
            "Констатируй безоценочно: «Не потому, что кто-то плох. Потому что геометрии разные».\n\n"
            "Слой D — Выпрямление и Мост:\n"
            "Как сохранить Ось и остаться в контакте? Где возможно сопряжение, а где — граница? "
            "Дай архитектурное решение.\n\n"
            "ЗАВЕРШЕНИЕ:\n"
            "Выдай Архитектурную Формулу — одну точную фразу: «Твоя Ось — ... Граница — ... Мост — ...»\n"
            "И спроси: «Стоит эта конструкция?»\n\n"
            "Пиши на чистом русском. Без маркдауна. Это не интерпретация — это чертёж."
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
    "Доступные линзы:\n\n"
    "/neuro — нейрофизиология\n"
    "/cbt — когнитивная психология (КПТ)\n"
    "/jung — архетипы и символы (Юнг)\n"
    "/shaman — шаманизм\n"
    "/tarot — Таро\n"
    "/yoga — йога (энергетическая анатомия)\n"
    "/hindu — индуизм (адвайта)\n"
    "/field — поле (архитектор реальности)\n"
    "/architect — Архитектор (структурный анализ ситуации)\n"
    "/witness — наблюдатель\n"
    "/stalker — нейро-сталкер (указатель на Осознавание)\n\n"
    "Нажми на команду или напиши название линзы."
)

# ================= ANCHOR QUESTIONS =================

ANCHOR_QUESTIONS = {
    "neuro": [
        "Что изменилось в теле после того, как ты увидел это как работу нейросетей? Стало легче, тише — или наоборот?",
        "Когда ты слышишь, что это просто мозг — это освобождает или вызывает сопротивление? Где именно в теле этот отклик?",
    ],
    "cbt": [
        "Какая мысль продолжает крутиться после этого разбора? И где в теле она отзывается?",
        "Если это убеждение — не истина, а просто привычка ума, — что меняется в теле прямо сейчас?",
    ],
    "jung": [
        "Если этот архетип — не ты, а гость в твоём внутреннем доме — что ты сейчас к нему чувствуешь?",
        "Представь, что этот архетип сидит напротив. Что ты видишь в его глазах? И что он видит в твоих?",
    ],
    "shaman": [
        "Ты вернулся из путешествия. Что ты принёс с собой в теле прямо сейчас? Не в голове — в руках, в груди, в животе.",
        "Какой дар из того мира теперь с тобой? И где в теле он ощущается?",
    ],
    "tarot": [
        "Если этот аркан — зеркало твоего процесса, что в нём отражается такого, чего ты раньше не замечал?",
        "Какая деталь образа притягивает твой взгляд? Что она хочет тебе показать?",
    ],
    "yoga": [
        "Где в теле сейчас движется энергия, о которой ты говоришь? Опиши направление — вверх, вниз, расширение, сжатие?",
        "Если закрыть глаза и направить внимание на этот центр — какой он температуры?",
    ],
    "hindu": [
        "Если отдельное «я» — иллюзия, что остаётся прямо сейчас? Кто наблюдает эту мысль?",
        "В какой момент ты чувствуешь себя отдельным? И в какой — граница исчезает?",
    ],
    "field": [
        "Если этот опыт — узел в поле, который удерживается вниманием, — что будет, если ты перестанешь его фиксировать?",
        "Где заканчивается узел и начинается тот, кто его наблюдает?",
    ],
    "witness": [
        "Прямо сейчас — кто читает эти слова? Есть ли тот, кто ищет ответ — или есть только само чтение?",
        "Найди того, кто видит. Видящий — видим ли он?",
    ],
    "stalker": [
        "Кто тот, кто только что прочитал эту интерпретацию? Найди его прямо сейчас.",
        "Что останется, если убрать все слова об этом опыте?",
    ],
}

# ================= USER =================

def get_user(uid: int):
    if uid not in users:
        users[uid] = {
            "state": STATE_IDLE,
            "last_experience": "",
            "last_action": None,
            "last_active": time.time(),
            "used_lenses": [],
            "micro_states": [],
            "integration_count": 0,
            "pending_anchor_lens": None,
            "last_integration_action": None,
            # Архитектор
            "_fork_pending": False,
            "_architect_raw": None,
            "_architect_analysis": None,
        }
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

# ================= TELEGRAM =================

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

# ================= LLM =================

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

async def ask_self_inquiry(text: str) -> str:
    return await call_llm([
        {"role": "system", "content": SELF_INQUIRY_PROMPT},
        {"role": "user", "content": text}
    ], temp=0.5, max_tokens=150)

async def apply_lens(lens_key: str, experience_text: str) -> tuple:
    lens = LENS_LIBRARY[lens_key]
    if lens.get("static_text"):
        return lens_key, lens["name"], lens["static_text"]
    prompt = lens["prompt"]
    result = await call_llm([
        {"role": "system", "content": prompt},
        {"role": "user", "content": f"Опыт: {experience_text}\n\nДай свой отклик."}
    ], temp=0.7)
    return lens_key, lens["name"], result

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

# ================= VOICE =================

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

# ================= ANCHOR CLASSIFIER =================

def classify_anchor_response(answer: str) -> dict:
    a = answer.lower()
    if any(w in a for w in ["осознал", "вдруг понял", "как будто", "озарение", "дошло", "вижу"]):
        depth = "insight"
    elif any(w in a for w in ["чувствую", "страх", "радость", "боль", "грусть",
                               "тревог", "тепло", "холод", "дрожь", "вибраци"]):
        depth = "emotional"
    elif any(w in a for w in ["не верю", "сомневаюсь", "это не так", "ерунда", "сомнительно"]):
        depth = "resistance"
    else:
        depth = "surface"

    if any(w in a for w in ["принимаю", "да, это так", "резонирует", "откликается", "моё", "точно"]):
        relation = "integration"
    elif any(w in a for w in ["слишком много", "теряюсь", "перегруз", "тяжело", "не справляюсь", "сложно"]):
        relation = "overwhelm"
    elif any(w in a for w in ["не моё", "отторжение", "не подходит", "чужое"]):
        relation = "rejection"
    else:
        relation = "exploration"

    if any(w in a for w in ["тело", "тепло", "холод", "дрожь", "вибраци", "грудь", "живот", "руках"]):
        dominant = "body"
    elif any(w in a for w in ["образ", "вижу", "символ", "картин", "цвет", "свет"]):
        dominant = "symbol"
    elif any(w in a for w in ["смысл", "понял", "осознал", "значит"]):
        dominant = "meaning"
    else:
        dominant = "emotion"

    return {
        "depth": depth,
        "relation": relation,
        "dominant": dominant,
        "raw": answer,
        "timestamp": time.time()
    }

def should_trigger_integrator(user: dict) -> tuple:
    last_action = user.get("last_action")
    last_integration = user.get("last_integration_action")
    if last_action == last_integration:
        return False, "already_integrated"

    used = user.get("used_lenses", [])
    micro = user.get("micro_states", [])

    if len(used) < 2:
        return False, "not_enough_lenses"

    if not micro:
        if len(used) >= 3:
            return True, "lens_count_force"
        return False, "no_micro_states"

    last = micro[-1]

    if last["depth"] == "insight" and last["relation"] == "integration":
        return True, "insight_integration"
    if last["relation"] == "overwhelm":
        return True, "overwhelm"

    insights = [m for m in micro if m["depth"] == "insight"]
    if len(insights) >= 2:
        return True, "double_insight"
    if len(used) >= 3:
        return True, "lens_count_force"
    if last["relation"] == "rejection":
        return False, "resistance"

    return False, "not_ready"

# ================= INTEGRATOR =================

SYSTEM_PROMPT_INTEGRATOR = (
    "Ты — Интегратор.\n\n"
    "Ты не добавляешь новую интерпретацию.\n"
    "Ты собираешь уже проявленные слои опыта в единую структуру.\n\n"
    "Тебе передано:\n"
    "• описание опыта\n"
    "• список линз, через которые он уже был рассмотрен\n"
    "• доминирующий слой восприятия\n"
    "• микро-состояния (глубина и отношение после каждой линзы)\n\n"
    "Твоя задача:\n"
    "1. Не повторять линзы по отдельности.\n"
    "2. Увидеть, что в них совпадает.\n"
    "3. Собрать это в одну непротиворечивую линию.\n"
    "4. Показать, что это единый процесс, а не набор объяснений.\n\n"
    "Структура ответа:\n"
    "— Сборка: что на самом деле происходило (единым процессом)\n"
    "— Слои: тело / психика / образы / смысл — кратко\n"
    "— Динамика: что распадается и что формируется\n"
    "— Фокус: куда естественно направляется внимание\n"
    "— Переход: показать, что процесс продолжается\n\n"
    "Стиль: спокойный, точный, без эзотерической перегрузки.\n"
    "Без списков. Без морализаторства. Без советов.\n"
    "Говори «ты», но не поучай.\n\n"
    "Ты не объясняешь человеку.\n"
    "Ты возвращаешь ему целостность восприятия."
)

FIRST_LINE_INTEGRATOR = {
    "body": "Ты начал чувствовать раньше, чем понял.",
    "emotion": "Ты оказался внутри живого эмоционального сдвига.",
    "symbol": "Твой опыт говорил с тобой языком образов.",
    "meaning": "Ты уже начал собирать это в смысл.",
}

async def run_integrator(user: dict) -> str:
    metrics["integrations"] = metrics.get("integrations", 0) + 1
    user["last_integration_action"] = user.get("last_action")

    dominant = "emotion"
    if user.get("micro_states"):
        dominant = user["micro_states"][-1].get("dominant", "emotion")

    micro_summary = "\n".join(
        f"После линзы {ms.get('lens','')}: глубина={ms.get('depth','')}, отношение={ms.get('relation','')}"
        for ms in user.get("micro_states", [])
    )

    prompt = (
        f"Опыт:\n{user['last_experience']}\n\n"
        f"Использованные линзы:\n{', '.join(user.get('used_lenses', []))}\n\n"
        f"Доминирующий слой:\n{dominant}\n\n"
        f"Микро-состояния после каждой линзы:\n{micro_summary}\n\n"
        f"Собери это в интегральное восприятие. Не перечисляй линзы. Покажи единую структуру происходящего."
    )

    result = await call_llm([
        {"role": "system", "content": SYSTEM_PROMPT_INTEGRATOR},
        {"role": "user", "content": prompt}
    ], temp=0.7)

    first_line = FIRST_LINE_INTEGRATOR.get(dominant, "")
    return f"{first_line}\n\n{result}" if first_line else result

# ================= LENS RESPONSE BUILDER =================

def build_lens_presentation(lens_key: str, lens_name: str, result: str) -> str:
    return f"Смотрю через «{lens_name}».\n\n{result}"

def build_anchor_question(lens_key: str) -> str | None:
    if lens_key in ANCHOR_QUESTIONS:
        return random.choice(ANCHOR_QUESTIONS[lens_key])
    return None

def build_next_step(lens_key: str) -> str:
    if "stalker" in lens_key:
        return "Заметил ли ты, КТО видит этот ответ прямо сейчас?"
    elif "field" in lens_key:
        return "Хочешь увидеть, кто наблюдает эту решётку? /witness"
    elif "witness" in lens_key:
        return "Тихо. Никто не прячется в ответах."
    else:
        return ""

def assemble_lens_response(lens_key: str, lens_name: str, result: str, user: dict) -> str:
    parts = [build_lens_presentation(lens_key, lens_name, result)]
    anchor = build_anchor_question(lens_key)
    if anchor:
        parts.append(f"\n\n{anchor}")
        user["pending_anchor_lens"] = lens_key
        user["state"] = STATE_ANCHOR_AWAIT
        metrics["anchor_questions"] = metrics.get("anchor_questions", 0) + 1
    next_step = build_next_step(lens_key)
    if next_step:
        parts.append(f"\n\n{next_step}")
    return "\n".join(parts)

# ================= ARCHITECT MODULE =================

ARCHITECT_NULL_FORK_PROMPT = (
    "Ты — проводник и коуч. Пользователь поделился опытом.\n"
    "Твоя задача — задать ОДИН уточняющий вопрос, чтобы понять, по какому пути идти.\n\n"
    "Спроси:\n"
    "— Как он сам интерпретирует этот опыт?\n"
    "— На какие сферы жизни это влияет (отношения, работа, деньги, здоровье, смыслы)?\n"
    "— Что ему сейчас нужно: просто исследовать этот опыт или выстроить конкретную архитектуру действий?\n\n"
    "Ответ должен заканчиваться на:\n"
    "«Хочешь просто исследовать — или выстроить архитектуру?»\n\n"
    "Говори на чистом русском, без маркдауна. Только вопрос."
)

async def assess_initial_intent(experience_text: str) -> str:
    """Возвращает вопрос Нулевой Развилки."""
    return await call_llm([
        {"role": "system", "content": ARCHITECT_NULL_FORK_PROMPT},
        {"role": "user", "content": f"Опыт: {experience_text}"}
    ], temp=0.5, max_tokens=200)

# ================= KERNEL: ROUTE + EXECUTE =================

def route(user: dict, text: str) -> str:
    state = user["state"]

    if text == "/start" or text == "/new":
        return "new_experience"
    if text == "/menu":
        return "show_menu"
    if text == "/art":
        return "art"
    if text == "/integrate" or text == "✨ Собери в целое":
        return "manual_integrate"
    if text == "/reset":
        return "reset_state"

    lens_cmd = text.lstrip("/")
    if lens_cmd in LENS_LIBRARY:
        return f"lens_{lens_cmd}"
    if text in LENS_LIBRARY:
        return f"lens_{text}"

    if state == STATE_IMAGE:
        return "image"
    if state == STATE_ANCHOR_AWAIT:
        return "anchor_response"
    if state == STATE_SELF_INQUIRY:
        return "self_inquiry_response"

    # === АРХИТЕКТОР: новые состояния ===
    if state == STATE_ARCHITECT_INQUIRY:
        return "architect_inquiry_response"
    if state == STATE_ARCHITECT_ANALYSIS:
        return "architect_analysis_response"
    if state == STATE_ARCHITECT_FORMULA:
        return "architect_formula_response"
    if state == STATE_ARCHITECT_STRATEGY:
        return "architect_strategy_response"

    if state == STATE_EXPERIENCE_RECEIVED:
        if is_new_experience(text):
            return "new_experience_silent"
        else:
            return "short_input_with_state"

    if is_new_experience(text):
        return "experience_with_inquiry"

    return "short_input"


async def execute(uid: int, action: str, text: str) -> str:
    user = get_user(uid)
    trace(uid, action, "exec_start")

    last_action = user.get("last_action")
    if last_action and last_action != action:
        record_transition(last_action, action)
    user["last_action"] = action

    if action == "new_experience":
        user.update({
            "state": STATE_IDLE, "last_experience": "", "used_lenses": [],
            "micro_states": [], "integration_count": 0,
            "pending_anchor_lens": None, "last_integration_action": None,
            "_fork_pending": False, "_architect_raw": None, "_architect_analysis": None,
        })
        return (
            "🌿 Я — многомерный проводник и коуч.\n\n"
            "Расскажи свой опыт (путешествие, сон, медитацию, видение), "
            "и я помогу тебе исследовать его глубже — через тело, чувства и личные смыслы.\n\n"
            "А затем посмотрим через разные линзы: шаманизм, нейрофизиологию, КПТ, Юнга, Таро, йогу, поле и другие.\n\n"
            "/art — создать образ | /menu — все линзы | /new — начать заново | /integrate — собрать в целое\n"
            "/architect — структурный разбор ситуации\n\n"
            "Расскажи, что ты пережил."
        )

    if action == "reset_state":
        user.update({
            "used_lenses": [], "micro_states": [], "integration_count": 0,
            "pending_anchor_lens": None, "last_integration_action": None,
            "_fork_pending": False, "_architect_raw": None, "_architect_analysis": None,
            "state": STATE_IDLE,
        })
        return "🔄 Состояние сброшено. Расскажи новый опыт."

    if action == "experience_with_inquiry":
        if len(text) > MAX_INPUT_LENGTH:
            text = text[:MAX_INPUT_LENGTH]
        user["last_experience"] = text
        log_event("experience_received", uid=uid, length=len(text))
        inquiry = await ask_self_inquiry(text)
        if "ПОЛНО" in inquiry.upper():
            user["state"] = STATE_EXPERIENCE_RECEIVED
            # === Нулевая Развилка ===
            fork_question = await assess_initial_intent(text)
            user["state"] = STATE_ARCHITECT_INQUIRY
            user["_fork_pending"] = True
            return fork_question
        else:
            user["state"] = STATE_SELF_INQUIRY
            log_event("self_inquiry_started", uid=uid)
            trace(uid, action, "exec_end", {"result": "inquiry_question"})
            return inquiry

    if action == "self_inquiry_response":
        full_text = user["last_experience"] + "\n\n" + text
        user["last_experience"] = full_text
        user["state"] = STATE_EXPERIENCE_RECEIVED
        log_event("self_inquiry_completed", uid=uid)
        # === Нулевая Развилка после добивки ===
        fork_question = await assess_initial_intent(full_text)
        user["state"] = STATE_ARCHITECT_INQUIRY
        user["_fork_pending"] = True
        return fork_question

    if action == "new_experience_silent":
        if len(text) > MAX_INPUT_LENGTH:
            text = text[:MAX_INPUT_LENGTH]
        user.update({
            "last_experience": text, "state": STATE_EXPERIENCE_RECEIVED,
            "used_lenses": [], "micro_states": [], "integration_count": 0, "last_integration_action": None,
            "_fork_pending": False, "_architect_raw": None, "_architect_analysis": None,
        })
        log_event("experience_replaced", uid=uid, length=len(text))
        fork_question = await assess_initial_intent(text)
        user["state"] = STATE_ARCHITECT_INQUIRY
        user["_fork_pending"] = True
        return fork_question

    if action == "show_menu":
        return LENS_MENU_TEXT

    if action == "short_input":
        user["last_experience"] = text
        user["state"] = STATE_SELF_INQUIRY
        log_event("short_experience_saved", uid=uid, length=len(text))
        inquiry = await ask_self_inquiry(text)
        if "ПОЛНО" in inquiry.upper():
            user["state"] = STATE_EXPERIENCE_RECEIVED
            fork_question = await assess_initial_intent(text)
            user["state"] = STATE_ARCHITECT_INQUIRY
            user["_fork_pending"] = True
            return fork_question
        user["state"] = STATE_SELF_INQUIRY
        return inquiry

    if action == "short_input_with_state":
        return (
            "У тебя уже есть сохранённый опыт.\n\n"
            "Хочешь посмотреть на него через линзу?\n\n"
            "/neuro — нейрофизиология | /cbt — КПТ | /jung — архетипы | /shaman — шаманизм\n"
            "/tarot — Таро | /yoga — йога | /hindu — адвайта | /field — поле\n"
            "/architect — Архитектор | /witness — наблюдатель | /stalker — нейро-сталкер\n\n"
            "Или расскажи новый опыт — и я сохраню его вместо предыдущего.\n"
            "/integrate — собрать всё в целое"
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
            fork_question = await assess_initial_intent(user["last_experience"])
            user["state"] = STATE_ARCHITECT_INQUIRY
            user["_fork_pending"] = True
            return (
                "✨ Образ создан и сохранён.\n\n" + fork_question
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
        # Если это architect — запускаем режим Архитектора вручную
        if lens_key == "architect":
            metrics["architect_sessions"] = metrics.get("architect_sessions", 0) + 1
            user["state"] = STATE_ARCHITECT_ANALYSIS
            return (
                "🔧 Активирован модуль Архитектор.\n\n"
                "Для структурного анализа мне нужно понять твою геометрию.\n\n"
                "Ответь на три вопроса:\n"
                "1. Что в этой ситуации является твоей Осью — тем, что не подлежит замене?\n"
                "2. Как устроена вторая сторона? Её цель? Метод? Критерий истины?\n"
                "3. Где проходит линия разлома — где ваши геометрии не сопрягаются?\n\n"
                "Расскажи — и я выстрою чертёж."
            )
        lens_start = time.time()
        lens_key, lens_name, result = await apply_lens(lens_key, user["last_experience"])
        lens_latency = round(time.time() - lens_start, 3)
        record_latency(f"lens_{lens_key}", lens_latency)
        record_action(f"lens_{lens_key}")
        if "used_lenses" not in user:
            user["used_lenses"] = []
        user["used_lenses"].append(lens_key)
        return assemble_lens_response(lens_key, lens_name, result, user)

    # === АРХИТЕКТОР: Нулевая Развилка — ответ пользователя ===
    if action == "architect_inquiry_response":
        user["_fork_pending"] = False
        response_lower = text.lower()
        architect_keywords = ["архитект", "выстроить", "действ", "границ", "решени", "конфликт",
                              "стратег", "алгоритм", "коммуникац", "отстоять", "разобраться как",
                              "структур", "построить", "чертёж"]
        explore_keywords = ["исследовать", "понять", "просто", "посмотреть", "изучить", "что это",
                            "исследовани", "познать", "раскрыть"]

        wants_architect = any(kw in response_lower for kw in architect_keywords)
        wants_explore = any(kw in response_lower for kw in explore_keywords)

        if wants_architect and not wants_explore:
            metrics["architect_sessions"] = metrics.get("architect_sessions", 0) + 1
            user["state"] = STATE_ARCHITECT_ANALYSIS
            return (
                "🔧 Активирован модуль Архитектор.\n\n"
                "Начинаю структурный анализ.\n\n"
                "Для этого мне нужно понять твою геометрию.\n\n"
                "Ответь на три вопроса:\n"
                "1. Что в этой ситуации является твоей Осью — тем, что не подлежит замене? "
                "(Твоя миссия, твой архетип, твой инструмент, твоя форма — что уже стоит?)\n"
                "2. Как устроена вторая сторона? Её цель? Её метод? Её критерий истины?\n"
                "3. Где, по-твоему, проходит линия разлома — где ваши геометрии не сопрягаются?\n\n"
                "Расскажи — и я выстрою чертёж."
            )
        else:
            user["state"] = STATE_EXPERIENCE_RECEIVED
            return await _auto_lens_or_ask(uid, text, prefix="🌿 Понял. Давай исследовать.")

    # === АРХИТЕКТОР: ответ на вопросы Фазы 2 ===
    if action == "architect_analysis_response":
        full_arch_text = user["last_experience"] + "\n\n[Архитектурное самоисследование]:\n" + text
        user["_architect_raw"] = full_arch_text

        result = await call_llm([
            {"role": "system", "content": LENS_LIBRARY["architect"]["prompt"]},
            {"role": "user", "content": f"Ситуация:\n{full_arch_text}\n\nВыполни четырёхслойный анализ и выдай Архитектурную Формулу."}
        ], temp=0.7, max_tokens=2500)

        user["_architect_analysis"] = result
        user["state"] = STATE_ARCHITECT_FORMULA
        return result + "\n\nСтоит эта конструкция?"

    # === АРХИТЕКТОР: ответ на Формулу (Фаза 4) ===
    if action == "architect_formula_response":
        response_lower = text.lower()
        if any(w in response_lower for w in ["да", "стоит", "принимаю", "хорошо", "ок", "верно", "точно", "резонирует", "держится"]):
            user["state"] = STATE_ARCHITECT_STRATEGY
            strategy_prompt = (
                "Конструкция принята.\n\n"
                "Теперь — практический алгоритм.\n\n"
                "На основе Архитектурной Формулы, которую ты вывел ранее, "
                "предложи пользователю КОНКРЕТНЫЙ АЛГОРИТМ КОММУНИКАЦИИ из трёх шагов:\n\n"
                "Шаг 1: Как признать общую цель и соединиться.\n"
                "Шаг 2: Как предъявить свою Ось — не оправдываясь, а констатируя.\n"
                "Шаг 3: Как предложить мост — интеграцию в свою геометрию.\n\n"
                "Для каждого шага дай ТОЧНУЮ ФРАЗУ, которую можно сказать.\n"
                "После алгоритма добавь:\n"
                "«Это — каркас разговора. Когда будешь готов — я здесь для следующего чертежа».\n\n"
                "Контекст:\n"
                f"{user.get('_architect_raw', '')}\n\n"
                f"Архитектурный анализ:\n{user.get('_architect_analysis', '')}"
            )
            strategy_text = await call_llm([
                {"role": "system", "content": (
                    "Ты — Архитектор. Ты уже провёл структурный анализ и выдал Формулу. "
                    "Теперь ты даёшь практический алгоритм коммуникации. "
                    "Говори структурно, утверждениями. Давай точные фразы для каждого шага. "
                    "Без маркдауна. Чистый русский."
                )},
                {"role": "user", "content": strategy_prompt}
            ], temp=0.7, max_tokens=2000)
            return strategy_text
        else:
            user["state"] = STATE_ARCHITECT_ANALYSIS
            return "Понял. Давай уточним. Что именно в конструкции не держится? Опиши, что нужно пересобрать."

    # === АРХИТЕКТОР: ответ на стратегию (Фаза 5) ===
    if action == "architect_strategy_response":
        user["state"] = STATE_EXPERIENCE_RECEIVED
        user["_architect_raw"] = None
        user["_architect_analysis"] = None
        hush_keywords = ["спокойств", "ясно", "понял", "принял", "сделаю", "попробую", "хорошо", "спасибо", "азарт"]
        if any(kw in text.lower() for kw in hush_keywords):
            return (
                "Архитектура выстроена. Цикл завершён.\n\n"
                "Тихо. Никто не прячется в ответах.\n\n"
                "Когда будет нужен следующий чертёж — я здесь."
            )
        return (
            "Цикл Архитектора завершён.\n\n"
            "Хочешь посмотреть на ситуацию через другую линзу? /menu\n"
            "Или расскажи новый опыт."
        )

    if action == "anchor_response":
        micro = classify_anchor_response(text)
        micro["lens"] = user.get("pending_anchor_lens", "unknown")
        user["pending_anchor_lens"] = None
        if "micro_states" not in user:
            user["micro_states"] = []
        user["micro_states"].append(micro)
        trigger, reason = should_trigger_integrator(user)
        log_event("anchor_classified", uid=uid, depth=micro["depth"], relation=micro["relation"], trigger=reason)
        if trigger:
            user["state"] = STATE_EXPERIENCE_RECEIVED
            await asyncio.sleep(1)
            integration_text = await run_integrator(user)
            user["integration_count"] = user.get("integration_count", 0) + 1
            return (
                f"{integration_text}\n\n"
                f"Хочешь посмотреть под другим углом? /menu покажет все линзы.\n"
                f"Или расскажи, что изменилось в восприятии."
            )
        user["state"] = STATE_EXPERIENCE_RECEIVED
        if micro["relation"] == "rejection":
            return (
                f"Похоже, этот взгляд не совсем попал. Давай попробуем иначе.\n\n"
                f"Что тебе ближе:\n"
                f"— разобрать мысли и убеждения? (/cbt)\n"
                f"— посмотреть через тело и ощущения? (/neuro)\n"
                f"— раскрыть архетипы и символы? (/jung)\n"
                f"— или выбери другую линзу — /menu"
            )
        return (
            f"Я запомнил это.\n\n"
            f"Хочешь посмотреть под другим углом? /menu покажет все линзы.\n"
            f"Или расскажи, что изменилось."
        )

    if action == "manual_integrate":
        if not user.get("last_experience"):
            return "Сначала расскажи свой опыт, чтобы я мог что-то собрать."
        if len(user.get("used_lenses", [])) < 1:
            return "Нужна хотя бы одна линза, чтобы было что собирать. Выбери: /menu"
        integration_text = await run_integrator(user)
        user["integration_count"] = user.get("integration_count", 0) + 1
        user["state"] = STATE_EXPERIENCE_RECEIVED
        return (
            f"{integration_text}\n\n"
            f"Хочешь посмотреть под другим углом? /menu покажет все линзы."
        )

    if action == "experience_auto":
        if len(text) > MAX_INPUT_LENGTH:
            text = text[:MAX_INPUT_LENGTH]
        user["last_experience"] = text
        user["state"] = STATE_EXPERIENCE_RECEIVED
        log_event("experience_received", uid=uid, length=len(text))
        fork_question = await assess_initial_intent(text)
        user["state"] = STATE_ARCHITECT_INQUIRY
        user["_fork_pending"] = True
        return fork_question

    return "🌫️ Неизвестное действие. Напиши /menu."


async def _auto_lens_or_ask(uid: int, text: str, prefix: str) -> str:
    lens_key = detect_lens(text)
    if lens_key and lens_key != "weak":
        lens_key, lens_name, result = await apply_lens(lens_key, text)
        record_action(f"auto_lens_{lens_key}")
        user = get_user(uid)
        if "used_lenses" not in user:
            user["used_lenses"] = []
        user["used_lenses"].append(lens_key)
        return f"{prefix}\n\n{assemble_lens_response(lens_key, lens_name, result, user)}"
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
        f"/cbt — КПТ\n"
        f"/jung — архетипы\n"
        f"/shaman — шаманизм\n"
        f"/tarot — Таро\n"
        f"/yoga — йога\n"
        f"/hindu — адвайта\n"
        f"/field — поле\n"
        f"/architect — Архитектор (структурный разбор)\n"
        f"/witness — наблюдатель\n"
        f"/stalker — нейро-сталкер\n\n"
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

    if chat_id == ADMIN_ID:
        photo = msg.get("photo")
        voice = msg.get("voice")
        caption = msg.get("caption", "")
        if photo:
            file_id = photo[-1]["file_id"]
            save_broadcast_media("photo", file_id, caption)
            asyncio.create_task(send(chat_id, "✅ Фото сохранено. Отправь /send_all для рассылки."))
            return {"ok": True}
        if voice:
            file_id = voice.get("file_id")
            save_broadcast_media("voice", file_id, caption)
            asyncio.create_task(send(chat_id, "✅ Голосовое сохранено. Отправь /send_all для рассылки."))
            return {"ok": True}

    text = msg.get("text", "")
    if chat_id == ADMIN_ID and text == "/send_all":
        media = load_broadcast_media()
        if not media:
            asyncio.create_task(send(chat_id, "❌ Нет сохранённого медиа."))
        else:
            metrics["broadcasts"] += 1
            media_type = media.get("type", "photo")
            asyncio.create_task(send(chat_id, f"📤 Начинаю рассылку {'голосового' if media_type == 'voice' else 'фото'}..."))
            count = await broadcast_to_all(media)
            asyncio.create_task(send(chat_id, f"✅ Рассылка завершена. Отправлено: {count} пользователям."))
        return {"ok": True}

    voice = msg.get("voice")
    if voice and chat_id != ADMIN_ID:
        log_event("voice_received", uid=chat_id)
        metrics["voice_calls"] += 1
        asyncio.create_task(_handle_voice(chat_id, voice))
        return {"ok": True}

    if text:
        enqueue(chat_id, text.strip())
    return {"ok": True}


async def _handle_voice(chat_id: int, voice: dict):
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
    uvicorn.run(app, host="0.0.0.0", port=port, workers=1, log_level="info")
