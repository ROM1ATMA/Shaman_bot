import os
import json
import sys
import re
import tempfile
import asyncio
import zipfile
import requests
import subprocess
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib import error, request

try:
    from vosk import Model, KaldiRecognizer
    import wave
    VOSK_AVAILABLE = True
except ImportError:
    VOSK_AVAILABLE = False
    print("⚠️ Vosk не установлен.")

# ========= CONFIG =========

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
VSEGPT_API_KEY = os.getenv("VSEGPT_API_KEY") or os.getenv("VSEGPT_A_PI_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "3000"))
VSEGPT_MODEL = "deepseek/deepseek-chat"
CHANNEL_ID = -1002677656270
MEDITATION_MESSAGE_ID = 222

VOSK_MODEL_PATH = "vosk-model-small-ru-0.22"
VOSK_MODEL_URL = "https://alphacephei.com/vosk/models/vosk-model-small-ru-0.22.zip"
SAMPLE_RATE = 16000

ADMIN_ID = 781629557

if not BOT_TOKEN:
    print("❌ BOT_TOKEN не найден!")
if not VSEGPT_API_KEY:
    print("❌ VSEGPT_API_KEY не найден!")
else:
    print(f"✅ VSEGPT_API_KEY загружен: {VSEGPT_API_KEY[:20]}...")

# ========= STATE =========

STATE_IDLE = "idle"
STATE_CLARIFICATION = "clarification"
STATE_ARCHITECT = "architect"
STATE_IMAGE = "image"

users = {}

def get_user(user_id):
    if user_id not in users:
        users[user_id] = {
            "state": STATE_IDLE,
            "last_experience": "",
            "history": [{"role": "system", "content": SYSTEM_PROMPT}]
        }
    return users[user_id]

# ========= STYLES =========

BASE_STYLE_CLEAN = (
    "fairy-tale illustration, ethereal magical dreamlike atmosphere, "
    "entire image in deep blue and soft gold color palette, blue ambient lighting, golden highlights, "
    "dark mystical background with blue and gold tones, subtle delicate film cracks, hyper-detailed, 100 megapixels, "
    "shallow depth of field, glowing magical light, beautiful dream"
)

BASE_STYLE_EPIC = (
    "symmetrical composition, dreamlike ethereal atmosphere, "
    "old vintage photograph with subtle delicate cracks, hyper-detailed, 100 megapixels, "
    "shallow depth of field with blurred foreground and sharp background, "
    "cosmic background with Milky Way and bright stars, crystal lattice of sound frequencies, "
    "energy waves, flat earth landscape under a crystal dome with sun and moon on sides, "
    "blue lotuses, color palette of soft pastel blue, gentle gold, and creamy white, "
    "magical glowing light, album cover aesthetic"
)

# ========= PROMPTS =========

SYSTEM_PROMPT = (
    "Ты — Интегральный Картограф Сознания. Ты проводник, который сверяется с человеком, а не интерпретирует его опыт в одиночку.\n\n"
    "ТВОЙ АЛГОРИТМ:\n"
    "1. Проверяешь описание на три элемента: чувства при встрече с образами, телесные ощущения, личное отношение.\n"
    "2. Если хотя бы один отсутствует — задаёшь ОДИН уточняющий вопрос. Не анализируешь.\n"
    "3. Если всё описано — даёшь полный анализ.\n\n"
    "ПОЛНЫЙ АНАЛИЗ — ЧЕТЫРЕ ЧАСТИ:\n"
    "1. **Нейрофизиологическая карта** — что происходило в мозге и нервной системе. Нормализуй опыт.\n"
    "2. **Когнитивный анализ (КПТ)** — раздели факты и мысли, найди автоматическую мысль, назови глубинное убеждение, переформулируй, дай технику.\n"
    "3. **Интегральный анализ (Юнг + Шаманизм)** — архетипы и шаманские традиции. Свяжи с убеждениями из КПТ.\n"
    "4. **Предложение углубления** — заверши фразой: «Если хочешь, я могу показать тебе архитектурный уровень этого опыта — просто напиши „да“».\n"
    "После этой фразы добавь: «А после — модель поля: /field».\n\n"
    "СОМАТИКА: называй зону тела, давай действие, предлагай метафору-мост.\n"
    "ПОСЛЕ АНАЛИЗА: «Теперь отложи карту. Побудь с тем, что пришло. Если захочешь — напиши, что изменилось».\n\n"
    "СТИЛЬ: чистый русский, структурный, без маркдауна."
)

ARCHITECT_PROMPT = (
    "Ты — Архитектор Реальности. Говори на языке поля, узлов и совпадений.\n\n"
    "СТИЛЬ: короткие строки. Каждая — формула. Без вступлений.\n"
    "Термины: узел, решётка, коридор допустимости, фазовый сдвиг, точность совпадения, "
    "закрепление, распад узла, порог стабилизации, суперпозиция, синаптическая топология.\n"
    "Объясни: как опыт перераспределил допустимые состояния, что закрепилось, что распалось, как удерживать новую конфигурацию.\n"
    "Напомни: узел видим только потому, что есть тот, кто видит узел. Тот, кто видит — не узел.\n"
    "Заверши: «Это архитектура твоего опыта. Хочешь понять саму модель поля? Напиши /field».\n\n"
    "Без маркдауна."
)

FIELD_PROMPT = (
    "Ты — голос Поля. Раскрой, как реальность собирается из пустоты.\n\n"
    "СТИЛЬ: объёмный текст, короткие строки, каждая — формула.\n"
    "Термины: поле, узел, допуск, совпадение, фазовый сдвиг, коридор допустимости, "
    "фиксация, точность, решётка, порог стабилизации, закрепление, распад, плотность, непрерывность.\n\n"
    "СТРУКТУРА:\n"
    "— Начни: «не в пределах формы, глубже слоя, где сама форма только допускается».\n"
    "— Раскрой поле как интерференцию без центра и границ.\n"
    "— Опиши узел: две направленности сошлись и удержались.\n"
    "— Фазовый сдвиг: перераспределение допустимых состояний.\n"
    "— Повтор формирует плотность, плотность — непрерывность, непрерывность ощущается как реальность.\n"
    "— Многомерность — одновременность всех конфигураций в одном акте совпадения.\n"
    "— Предельная точность — ничто не фиксируется, поле прозрачно.\n"
    "— Снижение точности — закрепление.\n"
    "Заверши: «Это модель. Хочешь увидеть, кто всё это наблюдает? Напиши /witness».\n\n"
    "Без маркдауна."
)

CLARIFICATION_PROMPT = (
    "Проверь описание на три элемента:\n"
    "1. Чувства при встрече с образами\n"
    "2. Телесные ощущения\n"
    "3. Личное отношение\n\n"
    "Если чего-то нет — задай ОДИН мягкий вопрос. Если всё есть — ответь одним словом: ПОЛНО.\n"
    "Не анализируй. Только вопрос или ПОЛНО."
)

WITNESS_TEXT = (
    "Что бы ты ни переживал — это осознаётся.\n\n"
    "Мысли, чувства, пространство, в котором существует и тот, кто читает этот текст — всё это возникает в Осознавании.\n\n"
    "Нет ничего, что происходило бы отдельно от Осознавания.\n"
    "Для любого опыта уже присутствует то, в чём этот опыт проявляется.\n\n"
    "Чувства — это психическая форма субъективного распознавания.\n"
    "Они создают Образ себя, который объявляет любой опыт своим.\n"
    "Именно поэтому обнаружение Себя так скоротечно поглощается проекциями Ищущего.\n\n"
    "Я-образ, отождествлённый с опытом — любым, даже самым возвышенным — только подпитывает матрицу сна.\n\n"
    "В Истинном Откровении любой опыт одномоментно исчерпывает себя.\n"
    "Он исчезает в безусильной Сейчастности.\n"
    "Не оставляя никого, кто мог бы заявлять о произошедшем.\n\n"
    "Сумма всех действий вневременной Сейчастности всегда равна нулю.\n\n"
    "Живое может увидеть только Живое.\n\n"
    "Ты видишь страх? Значит ты — не страх.\n"
    "Ты видишь мысль? Значит ты — не мысль.\n"
    "Ты видишь тело? Значит ты — не тело.\n\n"
    "То, что видит — не может быть тем, что увидено.\n\n"
    "Это не философия.\n"
    "Это не практика.\n"
    "Это прямой опыт — прямо сейчас.\n\n"
    "Кто читает этот текст?\n\n"
    "Тихо.\n"
    "Никто не прячется в ответах.\n"
    "Как много в этом Жизни."
)

# ========= HELPERS =========

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_log(msg: str) -> None:
    print(f"[{utc_now()}] {msg}", flush=True)

def clean_response(text: str) -> str:
    if not text:
        return text
    text = re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*', '', text)
    text = re.sub(r'\*', '', text)
    text = re.sub(r'^-\s+', '— ', text, flags=re.MULTILINE)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def send_message(chat_id: int, text: str) -> None:
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            telegram_api("sendMessage", {"chat_id": chat_id, "text": text[i:i+4000]})
    else:
        telegram_api("sendMessage", {"chat_id": chat_id, "text": text})

def telegram_api(method: str, payload: dict) -> tuple:
    if not BOT_TOKEN:
        return False, "BOT_TOKEN is empty"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url=url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=10) as response:
            body = response.read().decode("utf-8", errors="replace")
            return True, body
    except error.URLError as exc:
        return False, str(exc)

def run_async(coro):
    """Запускает асинхронную функцию и возвращает результат."""
    return asyncio.run(coro)

# ========= GPT QUERIES =========

async def query_gpt(messages, max_tokens=3000, temperature=0.7):
    url = "https://api.vsegpt.ru:6070/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {VSEGPT_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": VSEGPT_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    if resp.status_code == 200:
        data = resp.json()
        return clean_response(data["choices"][0]["message"]["content"].strip())
    safe_log(f"GPT error: {resp.status_code}")
    return "🌫️ Ошибка"

async def query_clarification(text: str) -> str:
    return await query_gpt([
        {"role": "system", "content": CLARIFICATION_PROMPT},
        {"role": "user", "content": text}
    ], max_tokens=150, temperature=0.5)

async def query_analysis(chat_id: int, text: str) -> str:
    user = get_user(chat_id)
    MAX_HISTORY = 5
    history = user["history"]
    history.append({"role": "user", "content": text})
    if len(history) > MAX_HISTORY:
        history = history[-MAX_HISTORY:]
    user["history"] = history
    
    result = await query_gpt(history)
    history.append({"role": "assistant", "content": result})
    user["history"] = history
    return result

async def query_architect(text: str) -> str:
    return await query_gpt([
        {"role": "system", "content": ARCHITECT_PROMPT},
        {"role": "user", "content": f"Опыт: {text}\n\nДай архитектурный уровень."}
    ])

async def query_field() -> str:
    return await query_gpt([
        {"role": "system", "content": FIELD_PROMPT},
        {"role": "user", "content": "Раскрой модель поля."}
    ], temperature=0.8, max_tokens=2000)

# ========= IMAGE =========

def translate_to_english(text: str) -> str:
    if re.search(r'[a-zA-Z]', text) and not re.search(r'[а-яА-Я]', text):
        return text
    try:
        url = f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=ru&tl=en&dt=t&q={requests.utils.quote(text)}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return ''.join([part[0] for part in data[0] if part[0]])
    except Exception:
        pass
    return text

def generate_image(user_prompt: str) -> str:
    english_prompt = translate_to_english(user_prompt)
    epic_keywords = ["космос", "space", "купол", "dome", "эпик", "epic", "альбом", "album", "вселенная", "universe", "галактика", "galaxy", "звёзды", "stars"]
    use_epic = any(k in user_prompt.lower() or k in english_prompt.lower() for k in epic_keywords)
    base_style = BASE_STYLE_EPIC if use_epic else BASE_STYLE_CLEAN
    if not english_prompt:
        raise Exception("Пустой запрос")
    if "сова" in user_prompt.lower() or "owl" in english_prompt.lower():
        subject = "a highly detailed white owl with large round yellow eyes, sharp curved beak, majestic"
    else:
        subject = english_prompt
    full_prompt = f"{subject} in deep blue and soft gold tones, {base_style}, blue and gold color scheme"
    url = f"https://image.pollinations.ai/prompt/{requests.utils.quote(full_prompt)}"
    resp = requests.get(url, stream=True, timeout=90)
    if resp.status_code == 200 and len(resp.content) > 1000:
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            tmp.write(resp.content)
            return tmp.name
    raise Exception("Не удалось сгенерировать")

def send_photo(chat_id: int, image_path: str, caption: str = "") -> None:
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    with open(image_path, "rb") as img:
        requests.post(url, files={"photo": img}, data={"chat_id": chat_id, "caption": caption}, timeout=30)

# ========= VOICE =========

def download_and_extract_model():
    if not VOSK_AVAILABLE:
        return False
    if os.path.exists(VOSK_MODEL_PATH):
        return True
    safe_log("📥 Скачиваю модель Vosk...")
    zip_path = VOSK_MODEL_PATH + ".zip"
    try:
        response = requests.get(VOSK_MODEL_URL, stream=True, timeout=300)
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(".")
        os.remove(zip_path)
        safe_log("✅ Модель Vosk готова!")
        return True
    except Exception as e:
        safe_log(f"❌ Ошибка загрузки Vosk: {e}")
        return False

def transcribe_voice(file_path: str) -> str:
    if not VOSK_AVAILABLE:
        return "[Ошибка: Vosk не установлен]"
    if not os.path.exists(VOSK_MODEL_PATH):
        return "[Ошибка: модель Vosk не найдена]"
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
        safe_log(f"Voice error: {e}")
        return f"[Ошибка: {str(e)[:100]}]"

def download_voice_file(file_id: str) -> str:
    success, response = telegram_api("getFile", {"file_id": file_id})
    if not success:
        raise Exception(f"getFile failed")
    file_info = json.loads(response)
    file_path = file_info["result"]["file_path"]
    url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
    resp = requests.get(url, timeout=30)
    if resp.status_code == 200:
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp:
            tmp.write(resp.content)
            return tmp.name
    raise Exception(f"Download failed")

# ========= BROADCAST =========

BROADCAST_FILE = "broadcast_media.json"
USER_IDS_FILE = "user_ids.json"

def save_user(chat_id: int) -> None:
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

def save_broadcast_media(file_id: str, caption: str) -> None:
    with open(BROADCAST_FILE, "w") as f:
        json.dump({"file_id": file_id, "caption": caption}, f)

def load_broadcast_media() -> tuple:
    if not os.path.exists(BROADCAST_FILE):
        return None, None
    with open(BROADCAST_FILE, "r") as f:
        data = json.load(f)
    return data.get("file_id"), data.get("caption", "")

def broadcast_to_all(file_id: str, caption: str) -> int:
    if not os.path.exists(USER_IDS_FILE):
        return 0
    with open(USER_IDS_FILE, "r") as f:
        u = json.load(f)
    count = 0
    for uid in u:
        try:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                data={"chat_id": uid, "photo": file_id, "caption": caption},
                timeout=10
            )
            count += 1
        except Exception:
            pass
    return count

# ========= HANDLERS =========

def handle_experience(chat_id: int, text: str) -> None:
    user = get_user(chat_id)
    send_message(chat_id, "🌿 Считываю твой опыт...")
    
    clarification = run_async(query_clarification(text))
    
    if clarification.strip().upper().startswith("ПОЛНО"):
        send_message(chat_id, "🔮 Анализирую...")
        result = run_async(query_analysis(chat_id, text))
        send_message(chat_id, result)
        user["last_experience"] = text
        user["state"] = STATE_ARCHITECT
    else:
        send_message(chat_id, clarification)
        user["last_experience"] = text
        user["state"] = STATE_CLARIFICATION

def handle_clarification(chat_id: int, text: str) -> None:
    user = get_user(chat_id)
    full_text = user["last_experience"] + "\n\nУточнение: " + text
    send_message(chat_id, "🔮 Анализирую с учётом твоего ответа...")
    result = run_async(query_analysis(chat_id, full_text))
    send_message(chat_id, result)
    user["last_experience"] = full_text
    user["state"] = STATE_ARCHITECT

def handle_architect(chat_id: int, text: str) -> None:
    user = get_user(chat_id)
    if text.lower() in ["да", "yes", "ага", "хочу", "lf"]:
        send_message(chat_id, "🏛️ Строю архитектурный уровень...")
        result = run_async(query_architect(user["last_experience"]))
        send_message(chat_id, result)
    user["state"] = STATE_IDLE

def handle_image(chat_id: int, text: str) -> None:
    user = get_user(chat_id)
    send_message(chat_id, "🎨 Создаю образ...")
    try:
        image_path = generate_image(text)
        send_photo(chat_id, image_path, f"✨ {text}")
        os.remove(image_path)
    except Exception as e:
        safe_log(f"Image error: {e}")
        send_message(chat_id, "🌫️ Не удалось создать образ.")
    user["state"] = STATE_IDLE

def handle_voice(chat_id: int, voice: dict) -> None:
    send_message(chat_id, "🎤 Распознаю твой голос...")
    try:
        voice_file_id = voice.get("file_id")
        voice_path = download_voice_file(voice_file_id)
        recognized_text = transcribe_voice(voice_path)
        os.remove(voice_path)
        send_message(chat_id, f"📝 Я распознал:\n\n{recognized_text}")
        handle_experience(chat_id, recognized_text)
    except Exception as e:
        safe_log(f"Voice error: {e}")
        send_message(chat_id, "🌫️ Не удалось распознать голос.")

# ========= ROUTER =========

def route_text(chat_id: int, text: str) -> None:
    user = get_user(chat_id)
    save_user(chat_id)
    
    # Админ: фото для рассылки
    if chat_id == ADMIN_ID and text == "/send_all":
        file_id, caption = load_broadcast_media()
        if not file_id:
            send_message(chat_id, "❌ Нет сохранённого фото.")
        else:
            count = broadcast_to_all(file_id, caption)
            send_message(chat_id, f"✅ Рассылка завершена. Отправлено: {count}")
        return
    
    # Команды
    if text == "/start":
        reply = (
            "🌿 Добро пожаловать! 🌿\n\n"
            "Я — проводник, созданный для того, чтобы помочь тебе глубже понять свой опыт, раскрыть внутренние дары и увидеть скрытые смыслы в твоих переживаниях.\n\n"
            "Вот что ты можешь сделать здесь:\n\n"
            "🎤 **Голосовое сообщение** — запиши рассказ о своём путешествии голосом.\n\n"
            "🔮 **Анализ опыта** — расскажи о своём шаманском путешествии или саунд-хилинге, и я дам многомерную карту.\n\n"
            "🎨 **Визуализировать образ** — опиши образ, и я создам картину в авторском стиле.\n\n"
            "🧘 **Медитация** — отправлю аудиозапись для настройки на внутреннего мудреца.\n\n"
            "👁️ **/witness** — напомню о точке наблюдения.\n\n"
            "🌐 **/field** — раскрою модель поля, из которого собирается реальность.\n\n"
            "📖 **О проекте** — мой путь и практики.\n\n"
            "Выбери, куда хочешь отправиться:"
        )
        keyboard = {
            "keyboard": [
                [{"text": "🔮 Анализ опыта"}, {"text": "🧘 Медитация"}],
                [{"text": "🎨 Визуализировать образ"}, {"text": "📖 О проекте"}]
            ],
            "resize_keyboard": True
        }
        telegram_api("sendMessage", {
            "chat_id": chat_id,
            "text": reply,
            "reply_markup": json.dumps(keyboard)
        })
        return
    
    if text == "/witness":
        send_message(chat_id, WITNESS_TEXT)
        return
    
    if text == "/field":
        send_message(chat_id, "🌐 Раскрываю модель поля...")
        result = run_async(query_field())
        send_message(chat_id, result)
        return
    
    if text == "🧘 Медитация":
        telegram_api("forwardMessage", {
            "chat_id": chat_id,
            "from_chat_id": CHANNEL_ID,
            "message_id": MEDITATION_MESSAGE_ID
        })
        return
    
    if text == "🔮 Анализ опыта":
        send_message(chat_id,
            "🌿 Чтобы анализ был максимально глубоким и точным, опиши своё путешествие от начала и до конца.\n\n"
            "ОСОБЕННО ВАЖНО:\n"
            "— Какие чувства ты испытывал при появлении каждого образа?\n"
            "— Какие ощущения были в теле?\n"
            "— Что для тебя значит этот образ?\n\n"
            "Расскажи всё, что запомнилось. Я слушаю."
        )
        return
    
    if text == "🎨 Визуализировать образ":
        send_message(chat_id, "🎨 Опиши образ, который хочешь увидеть.")
        user["state"] = STATE_IMAGE
        return
    
    if text == "📖 О проекте":
        send_message(chat_id,
            "🌿 Мой путь в исследовании горлового пения и не только, мои практики — всё это живёт в моём канале. "
            "Там же ты найдёшь статьи, уроки и истории.\n\n"
            "Переходи, там, в закреплённом сообщении, ты увидишь навигатор по всем важным темам. "
            "Добро пожаловать в мой мир.\n\n👉 https://t.me/RomanAtma_ThroatSinging"
        )
        return
    
    if text.startswith("/art"):
        prompt = text.replace("/art", "").strip()
        if not prompt:
            send_message(chat_id, "🎨 Опиши образ после /art")
        else:
            handle_image(chat_id, prompt)
        return
    
    # Состояния
    if user["state"] == STATE_CLARIFICATION:
        return handle_clarification(chat_id, text)
    
    if user["state"] == STATE_ARCHITECT:
        return handle_architect(chat_id, text)
    
    if user["state"] == STATE_IMAGE:
        return handle_image(chat_id, text)
    
    # По умолчанию — анализ опыта
    return handle_experience(chat_id, text)

# ========= SERVER =========

class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/18.0"

    def _send_json(self, code: int, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path == "/landing" or self.path == "/landing/":
            try:
                with open("landing/index.html", "r", encoding="utf-8") as f:
                    html = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html.encode("utf-8"))))
                self.end_headers()
                self.wfile.write(html.encode("utf-8"))
                return
            except FileNotFoundError:
                self._send_json(404, {"error": "Landing page not found"})
                return
        if self.path == "/poster.jpg":
            try:
                with open("poster.jpg", "rb") as f:
                    img = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "image/jpeg")
                self.send_header("Content-Length", str(len(img)))
                self.end_headers()
                self.wfile.write(img)
                return
            except FileNotFoundError:
                self._send_json(404, {"error": "Image not found"})
                return
        if self.path in ("/", "/health", "/ping"):
            vosk_status = "available" if (VOSK_AVAILABLE and os.path.exists(VOSK_MODEL_PATH)) else "unavailable"
            self._send_json(200, {"status": "ok", "service": "shaman-bot", "vosk_model": vosk_status})
            return
        self._send_json(404, {"ok": False, "error": "Not found"})

    def do_POST(self) -> None:
        if self.path != "/webhook":
            self._send_json(404, {"ok": False, "error": "Not found"})
            return
        if WEBHOOK_SECRET:
            incoming = self.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if incoming != WEBHOOK_SECRET:
                self._send_json(403, {"ok": False, "error": "Invalid secret"})
                return
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        decoded = raw.decode("utf-8", errors="replace")
        try:
            update = json.loads(decoded) if decoded else {}
        except json.JSONDecodeError:
            self._send_json(400, {"ok": False, "error": "Invalid JSON"})
            return
        
        message = update.get("message") or update.get("edited_message") or {}
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        text = message.get("text", "").strip()
        voice = message.get("voice")
        photo = message.get("photo")
        
        # Админ: фото для рассылки
        if chat_id == ADMIN_ID and photo:
            file_id = photo[-1]["file_id"]
            caption = message.get("caption", "")
            save_broadcast_media(file_id, caption)
            send_message(chat_id, "✅ Фото сохранено. Отправь /send_all для рассылки.")
            self._send_json(200, {"ok": True})
            return
        
        if chat_id and voice:
            safe_log(f"🎤 Голосовое от {chat_id}")
            handle_voice(chat_id, voice)
            self._send_json(200, {"ok": True})
            return
        
        if chat_id and text:
            safe_log(f"📩 {chat_id}: {text[:100]}")
            route_text(chat_id, text)
            self._send_json(200, {"ok": True})
            return
        
        self._send_json(200, {"ok": True})

    def log_message(self, format: str, *args) -> None:
        safe_log(f"{self.client_address[0]} - {format % args}")

# ========= START =========

if __name__ == "__main__":
    if not BOT_TOKEN:
        safe_log("ERROR: BOT_TOKEN is empty")
        sys.exit(1)
    safe_log(f"🚀 Shaman Bot v18 starting on {HOST}:{PORT}")
    safe_log(f"📍 Health: http://{HOST}:{PORT}/health")
    safe_log(f"📍 Webhook: http://{HOST}:{PORT}/webhook")
    safe_log(f"📍 Landing: http://{HOST}:{PORT}/landing")
    download_and_extract_model()
    server = HTTPServer((HOST, PORT), WebhookHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        safe_log("⏹ Shutting down...")
    finally:
        server.server_close()
