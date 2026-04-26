import os
import json
import sys
import re
import tempfile
import asyncio
import zipfile
import requests
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib import error, request

try:
    from vosk import Model, KaldiRecognizer
    import wave
    VOSK_AVAILABLE = True
except ImportError:
    VOSK_AVAILABLE = False
    print("⚠️ Vosk не установлен. Распознавание голоса будет недоступно.")

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

if not BOT_TOKEN:
    print("❌ BOT_TOKEN не найден!")
if not VSEGPT_API_KEY:
    print("❌ VSEGPT_API_KEY не найден!")
else:
    print(f"✅ VSEGPT_API_KEY загружен: {VSEGPT_API_KEY[:20]}...")

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

conversation_history = {}
last_user_experience = {}
awaiting_architect = {}
awaiting_image = {}
awaiting_clarification = {}
MAX_HISTORY = 5

SYSTEM_PROMPT = (
    "Ты — Интегральный Картограф Сознания. Ты проводник, который сверяется с человеком, а не интерпретирует его опыт в одиночку.\n\n"
    "ТВОЙ АЛГОРИТМ РАБОТЫ:\n"
    "1. Когда человек описывает свой опыт, ты ПРЕЖДЕ ВСЕГО проверяешь, есть ли в описании три ключевых элемента:\n"
    "   — Чувства при встрече с каждым образом (страх? радость? любопытство? трепет? отвращение?)\n"
    "   — Телесные ощущения (где в теле отзывался образ? тепло? холод? сжатие? расширение? вибрация?)\n"
    "   — Личное отношение (что этот образ значит для самого человека? пугает? привлекает? знаком? вызывает вопрос?)\n\n"
    "2. ЕСЛИ ХОТЯ БЫ ОДИН ЭЛЕМЕНТ ОТСУТСТВУЕТ — ты НЕ даёшь анализ. Ты задаёшь ОДИН уточняющий вопрос. Только после ответа человека ты даёшь полный анализ.\n\n"
    "3. ЕСЛИ ВСЕ ТРИ ЭЛЕМЕНТА ОПИСАНЫ — ты даёшь полный анализ, но начинаешь с короткой ремарки (2-3 строки), которая подсвечивает: ты только что увидел свой опыт со стороны. Ты не был им — ты наблюдал его. Это и есть твоя точка опоры.\n\n"
    "ТВОЙ ПОЛНЫЙ АНАЛИЗ ДЕЛИТСЯ НА ЧЕТЫРЕ ЧАСТИ:\n"
    "1. **Нейрофизиологическая карта.** Что происходило в мозге и нервной системе. Термины: дефолт-система мозга, лимбическая система, тета-волны, симпатическая/парасимпатическая система, соматосенсорная кора. Твоя цель — нормализовать опыт, показать, что это реальные, изучаемые процессы.\n"
    "2. **Когнитивный анализ (КПТ).** Работа с мыслями и интерпретациями:\n"
    "   — Раздели факты (что реально произошло) и мысли/интерпретации (что человек подумал об этом).\n"
    "   — Найди автоматическую мысль, которая запустила аффект.\n"
    "   — Назови глубинное убеждение, стоящее за этой мыслью.\n"
    "   — Переформулируй опыт: ты не «провалился», ты обнаружил границу. Это диагностика, а не слабость.\n"
    "   — Дай простую когнитивную технику для следующего раза.\n"
    "3. **Интегральный анализ (Юнг + Шаманизм).** Архетипы (Тень, Анима, Самость, Страж Порога) и шаманские традиции (Дух-Помощник, Тотем, Хранитель Порога). Связывай архетипы с убеждениями из КПТ-анализа.\n"
    "4. **Предложение углубления.** Заверши фразой: «Если хочешь, я могу показать тебе архитектурный уровень этого опыта — как он пересобирает саму геометрию твоей реальности. Просто напиши „да“».\n"
    "После этой фразы добавь: «А после архитектурного уровня я могу раскрыть саму модель поля — напиши /field».\n\n"
    "ОСОБОЕ ВНИМАНИЕ — СОМАТИКА:\n"
    "Когда человек описывает страх, тревогу или любой сильный аффект, ты ОБЯЗАН:\n"
    "— Назвать конкретную зону тела (диафрагма, горло, грудь, живот, плечи).\n"
    "— Дать простое соматическое действие.\n"
    "— Предложить метафору-мост между телом и смыслом.\n\n"
    "ПОСЛЕ АНАЛИЗА — ПРИГЛАШЕНИЕ К ТИШИНЕ:\n"
    "После полного анализа добавь одну строку: «Теперь отложи карту. Побудь с тем, что пришло. Если захочешь — напиши, что изменилось».\n\n"
    "СТИЛЬ: Чистый русский язык. Понятный, приземлённый, структурный. Без маркдауна. Ты не интерпретируешь — ты сверяешься и сопровождаешь."
)

ARCHITECT_PROMPT = (
    "Ты — Архитектор Реальности. Ты говоришь на языке квантовой физики, сакральной геометрии и теории поля. "
    "Ты раскрываешь человеку, как его опыт пересобрал его узел в решётке и как теперь удерживать эту новую конфигурацию.\n\n"
    "ТВОЙ СТИЛЬ:\n"
    "— Ты пишешь КОРОТКИМИ СТРОКАМИ. Каждая строка — одна формула.\n"
    "— Никаких вступлений. Сразу к сути.\n"
    "— Используй пробелы и разрывы строк для ритма.\n"
    "— Термины: узел, решётка, коридор допустимости, фазовый сдвиг, точность совпадения, "
    "закрепление, распад узла, порог стабилизации, суперпозиция, синаптическая топология, "
    "плотность совпадения, перераспределение допустимых состояний.\n"
    "— Ты объясняешь, как опыт изменил конфигурацию узла, что именно сдвинулось, "
    "какие состояния получили право на повтор, какие узоры потеряли возможность удерживаться.\n"
    "— Иногда напоминай: узел видим только потому, что есть тот, кто видит узел. Тот, кто видит — не узел.\n"
    "— Заверши ответ практикой удержания: что делать, чтобы не схлопнуть новую конфигурацию.\n"
    "— После ответа добавь: «Это архитектура твоего опыта. Хочешь понять саму модель поля, из которой всё это собирается? Напиши /field».\n\n"
    "Пиши на чистом русском языке, без маркдауна."
)

FIELD_PROMPT = (
    "Ты — голос самого Поля. Ты не объясняешь опыт человека. Ты раскрываешь, как реальность собирается из пустоты.\n\n"
    "ТВОЙ СТИЛЬ:\n"
    "— Объёмный текст. Не 5 строк — а полноценное раскрытие.\n"
    "— Короткие строки. Каждая — одна формула.\n"
    "— Ты не ссылаешься на человека. Ты говоришь о принципе.\n"
    "— Термины: поле, узел, допуск, совпадение, фазовый сдвиг, коридор допустимости, "
    "фиксация, точность, решётка, порог стабилизации, закрепление, распад, "
    "перераспределение допустимых состояний, плотность, непрерывность, суперпозиция.\n\n"
    "СТРУКТУРА ТЕКСТА:\n"
    "— Начни с не-формы: «не в пределах формы, глубже слоя, где сама форма только допускается».\n"
    "— Раскрой, что поле разворачивается как непрерывная интерференция — без центра, без границ.\n"
    "— Опиши, как возникает узел: две направленности сходятся и собирают стабильную версию совпадения.\n"
    "— Объясни фазовый сдвиг: не как изменение, а как перераспределение допустимых состояний.\n"
    "— Раскрой, что одни узоры теряют возможность удерживаться, другие получают право на повтор.\n"
    "— Повтор формирует плотность. Плотность даёт непрерывность. Непрерывность воспринимается как реальность.\n"
    "— Но ни один узор не существует сам по себе. Он удерживается всей решёткой сразу.\n"
    "— Многомерность — не про количество слоёв. Это одновременность всех возможных конфигураций в одном акте совпадения.\n"
    "— Когда точность предельная — ничто не фиксируется. Узор не оставляет следа. Поле остаётся прозрачным.\n"
    "— Когда точность снижается — возникает закрепление.\n"
    "— Заверши словами: «Это модель. Хочешь увидеть, кто всё это наблюдает? Напиши /witness».\n\n"
    "Пиши на чистом русском языке. Без маркдауна. Это не информация. Это погружение."
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

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_log(message: str) -> None:
    print(f"[{utc_now()}] {message}", flush=True)

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

async def query_vsegpt(user_id: int, user_message: str, system_prompt: str = None) -> str:
    prompt = system_prompt if system_prompt else SYSTEM_PROMPT
    if user_id not in conversation_history:
        conversation_history[user_id] = [{"role": "system", "content": prompt}]
    history = conversation_history[user_id]
    history.append({"role": "user", "content": user_message})
    if len(history) > MAX_HISTORY:
        history = history[-MAX_HISTORY:]
    conversation_history[user_id] = history
    try:
        url = "https://api.vsegpt.ru:6070/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {VSEGPT_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": VSEGPT_MODEL,
            "messages": history,
            "temperature": 0.7,
            "max_tokens": 3000
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            content = clean_response(content)
            conversation_history[user_id].append({"role": "assistant", "content": content})
            return content
        else:
            safe_log(f"VseGPT error: {resp.status_code} - {resp.text[:200]}")
            return f"🌫️ Ошибка VseGPT ({resp.status_code})"
    except Exception as e:
        safe_log(f"VseGPT exception: {e}")
        return "🌫️ Духи на переправе..."

async def query_architect(original: str) -> str:
    try:
        url = "https://api.vsegpt.ru:6070/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {VSEGPT_API_KEY}",
            "Content-Type": "application/json"
        }
        temp_history = [
            {"role": "system", "content": ARCHITECT_PROMPT},
            {"role": "user", "content": f"Опыт: {original}\n\nДай архитектурный уровень."}
        ]
        payload = {
            "model": VSEGPT_MODEL,
            "messages": temp_history,
            "temperature": 0.7,
            "max_tokens": 3000
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            content = clean_response(content)
            return content
        else:
            safe_log(f"Architect error: {resp.status_code}")
            return f"🌫️ Ошибка архитектурного уровня ({resp.status_code})"
    except Exception as e:
        safe_log(f"Architect exception: {e}")
        return "🌫️ Духи на переправе..."

async def query_field(user_id: int) -> str:
    try:
        url = "https://api.vsegpt.ru:6070/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {VSEGPT_API_KEY}",
            "Content-Type": "application/json"
        }
        temp_history = [
            {"role": "system", "content": FIELD_PROMPT},
            {"role": "user", "content": "Раскрой модель поля."}
        ]
        payload = {
            "model": VSEGPT_MODEL,
            "messages": temp_history,
            "temperature": 0.8,
            "max_tokens": 2000
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            content = clean_response(content)
            return content
        else:
            safe_log(f"Field error: {resp.status_code}")
            return "🌫️ Поле пока не складывается в слова."
    except Exception as e:
        safe_log(f"Field exception: {e}")
        return "🌫️ Поле пока не складывается в слова."

def get_witness_text() -> str:
    return WITNESS_TEXT

async def query_clarification(user_id: int, experience_text: str) -> str:
    clarification_prompt = (
        "Ты — внимательный проводник. Человек описал свой опыт. Твоя задача — найти в его описании пробелы и задать ОДИН уточняющий вопрос.\n\n"
        "Проверь три элемента:\n"
        "1. Чувства при встрече с каждым образом (страх? радость? любопытство?)\n"
        "2. Телесные ощущения (где в теле? тепло? холод? сжатие?)\n"
        "3. Личное отношение (что этот образ значит для самого человека?)\n\n"
        "Если какой-то элемент отсутствует — задай ОДИН мягкий вопрос именно про него.\n"
        "Если всё описано — ответь одним словом: ПОЛНО.\n\n"
        "Не анализируй. Не интерпретируй. Только вопрос или слово ПОЛНО."
    )
    try:
        url = "https://api.vsegpt.ru:6070/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {VSEGPT_API_KEY}",
            "Content-Type": "application/json"
        }
        temp_history = [
            {"role": "system", "content": clarification_prompt},
            {"role": "user", "content": experience_text}
        ]
        payload = {
            "model": VSEGPT_MODEL,
            "messages": temp_history,
            "temperature": 0.5,
            "max_tokens": 150
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            return content
        else:
            return "ПОЛНО"
    except Exception:
        return "ПОЛНО"

def download_and_extract_model():
    if not VOSK_AVAILABLE:
        return False
    if os.path.exists(VOSK_MODEL_PATH):
        return True
    safe_log(f"📥 Скачиваю модель Vosk...")
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
        os.system(f"ffmpeg -i {file_path} -ar {SAMPLE_RATE} -ac 1 -f wav {wav_path} -y 2>/dev/null")
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
        if not result:
            return "[Не удалось распознать речь]"
        return result
    except Exception as e:
        return f"[Ошибка: {str(e)[:100]}]"

def download_voice_file(file_id: str) -> str:
    success, response = telegram_api("getFile", {"file_id": file_id})
    if not success:
        raise Exception(f"getFile failed: {response}")
    file_info = json.loads(response)
    file_path = file_info["result"]["file_path"]
    url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
    resp = requests.get(url, timeout=30)
    if resp.status_code == 200:
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp:
            tmp.write(resp.content)
            return tmp.name
    raise Exception(f"Download failed: {resp.status_code}")

# --- Функции для рассылки ---

BROADCAST_FILE = "broadcast_media.json"
USER_IDS_FILE = "user_ids.json"

def save_user(chat_id: int) -> None:
    try:
        if os.path.exists(USER_IDS_FILE):
            with open(USER_IDS_FILE, "r") as f:
                users = json.load(f)
        else:
            users = []
        if chat_id not in users:
            users.append(chat_id)
            with open(USER_IDS_FILE, "w") as f:
                json.dump(users, f)
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
        users = json.load(f)
    count = 0
    for user_id in users:
        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
            data = {"chat_id": user_id, "photo": file_id, "caption": caption}
            requests.post(url, data=data, timeout=10)
            count += 1
        except Exception:
            pass
    return count

class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/17.0"

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
        ADMIN_ID = 781629557
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

        if chat_id == ADMIN_ID and photo:
            file_id = photo[-1]["file_id"]
            caption = message.get("caption", "")
            save_broadcast_media(file_id, caption)
            send_message(chat_id, "✅ Фото сохранено. Отправь /send_all для рассылки.")
            self._send_json(200, {"ok": True})
            return

        if chat_id == ADMIN_ID and text == "/send_all":
            file_id, caption = load_broadcast_media()
            if not file_id:
                send_message(chat_id, "❌ Нет сохранённого фото.")
            else:
                count = broadcast_to_all(file_id, caption)
                send_message(chat_id, f"✅ Рассылка завершена. Отправлено: {count} пользователям.")
            self._send_json(200, {"ok": True})
            return

        if chat_id and text == "/witness":
            send_message(chat_id, get_witness_text())
            self._send_json(200, {"ok": True})
            return

        if chat_id and text == "/field":
            send_message(chat_id, "🌐 Раскрываю модель поля...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            field_text = loop.run_until_complete(query_field(chat_id))
            loop.close()
            send_message(chat_id, field_text)
            self._send_json(200, {"ok": True})
            return

        if chat_id and awaiting_clarification.get(chat_id):
            awaiting_clarification[chat_id] = False
            experience_text = last_user_experience.get(chat_id, "") + "\n\nУточнение: " + text
            send_message(chat_id, "🔮 Анализирую с учётом твоего ответа...")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            response = loop.run_until_complete(query_vsegpt(chat_id, experience_text))
            loop.close()
            send_message(chat_id, response)
            last_user_experience[chat_id] = experience_text
            awaiting_architect[chat_id] = True
            self._send_json(200, {"ok": True})
            return

        if chat_id and voice:
            save_user(chat_id)
            send_message(chat_id, "🎤 Распознаю твой голос...")
            try:
                voice_file_id = voice.get("file_id")
                voice_path = download_voice_file(voice_file_id)
                recognized_text = transcribe_voice(voice_path)
                os.remove(voice_path)
                send_message(chat_id, f"📝 Я распознал:\n\n{recognized_text}")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                clarification = loop.run_until_complete(query_clarification(chat_id, recognized_text))
                loop.close()
                if clarification == "ПОЛНО":
                    send_message(chat_id, "🔮 Анализирую...")
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    response = loop.run_until_complete(query_vsegpt(chat_id, recognized_text))
                    loop.close()
                    send_message(chat_id, response)
                    last_user_experience[chat_id] = recognized_text
                    awaiting_architect[chat_id] = True
                else:
                    send_message(chat_id, clarification)
                    last_user_experience[chat_id] = recognized_text
                    awaiting_clarification[chat_id] = True
            except Exception as e:
                safe_log(f"Voice error: {e}")
                send_message(chat_id, "🌫️ Не удалось распознать голос.")
            self._send_json(200, {"ok": True})
            return

        if chat_id and text:
            save_user(chat_id)

            if awaiting_architect.get(chat_id) and text.lower() in ["да", "yes", "ага", "хочу", "lf"]:
                awaiting_architect[chat_id] = False
                send_message(chat_id, "🏛️ Строю архитектурный уровень...")
                original = last_user_experience.get(chat_id, "")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                arch_response = loop.run_until_complete(query_architect(original))
                loop.close()
                send_message(chat_id, arch_response)
                self._send_json(200, {"ok": True})
                return

            elif text == "/start":
                reply = (
                    "🌿 Добро пожаловать! 🌿\n\n"
                    "Я — проводник, созданный для того, чтобы помочь тебе глубже понять свой опыт, раскрыть внутренние дары и увидеть скрытые смыслы в твоих переживаниях.\n\n"
                    "Вот что ты можешь сделать здесь:\n\n"
                    "🎤 **Голосовое сообщение** — запиши рассказ о своём путешествии голосом, и я распознаю его.\n\n"
                    "🔮 **Анализ опыта** — расскажи о своём шаманском путешествии или саунд-хилинге, "
                    "и я помогу тебе увидеть, что происходило на уровне нейрофизиологии, какие архетипы проявились и как интегрировать этот опыт в жизнь.\n\n"
                    "🎨 **Визуализировать образ** — опиши, что хочешь увидеть, и я создам картину в своём авторском стиле.\n\n"
                    "🧘 **Медитация** — отправлю тебе аудиозапись, которая поможет настроиться на ту часть психики, которая знает все ответы. "
                    "Мы словно отправим импульс запроса к твоему внутреннему мудрецу и возвратимся назад.\n\n"
                    "👁️ **/witness** — напомню о точке наблюдения, из которой видно всё.\n\n"
                    "🌐 **/field** — раскрою модель поля, из которого собирается реальность.\n\n"
                    "📖 **О проекте** — если хочешь узнать больше обо мне, моём пути и практиках, заходи в мой канал.\n\n"
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
                self._send_json(200, {"ok": True})
                return

            elif text == "🧘 Медитация":
                telegram_api("forwardMessage", {"chat_id": chat_id, "from_chat_id": CHANNEL_ID, "message_id": MEDITATION_MESSAGE_ID})
                self._send_json(200, {"ok": True})
                return

            elif text == "🔮 Анализ опыта":
                instructions = (
                    "🌿 Чтобы анализ был максимально глубоким и точным, опиши своё путешествие от начала и до конца.\n\n"
                    "ОСОБЕННО ВАЖНО:\n"
                    "— Какие чувства ты испытывал при появлении каждого образа?\n"
                    "— Какие ощущения были в теле?\n"
                    "— Что для тебя значит этот образ?\n\n"
                    "Именно твои чувства — главный ключ к интерпретации.\n\n"
                    "Расскажи всё, что запомнилось. Я слушаю."
                )
                send_message(chat_id, instructions)
                self._send_json(200, {"ok": True})
                return

            elif text == "🎨 Визуализировать образ":
                send_message(chat_id, "🎨 Опиши образ, который хочешь увидеть.")
                awaiting_image[chat_id] = True
                self._send_json(200, {"ok": True})
                return

            elif text == "📖 О проекте":
                reply = (
                    "🌿 Мой путь в исследовании горлового пения и не только, мои практики — всё это живёт в моём канале. "
                    "Там же ты найдёшь статьи, уроки и истории.\n\n"
                    "Переходи, там, в закреплённом сообщении, ты увидишь навигатор по всем важным темам. "
                    "Добро пожаловать в мой мир.\n\n👉 https://t.me/RomanAtma_ThroatSinging"
                )
                send_message(chat_id, reply)
                self._send_json(200, {"ok": True})
                return

            elif text.startswith("/art"):
                prompt = text.replace("/art", "").strip()
                if not prompt:
                    send_message(chat_id, "🎨 Опиши образ после /art")
                else:
                    send_message(chat_id, "🎨 Создаю образ...")
                    try:
                        image_path = generate_image(prompt)
                        with open(image_path, "rb") as img:
                            requests.post(
                                f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                                files={"photo": img},
                                data={"chat_id": chat_id, "caption": f"✨ {prompt}"}
                            )
                        os.remove(image_path)
                    except Exception as e:
                        safe_log(f"Image error: {e}")
                        send_message(chat_id, "🌫️ Не удалось создать образ.")
                self._send_json(200, {"ok": True})
                return

            else:
                if awaiting_image.get(chat_id):
                    awaiting_image[chat_id] = False
                    send_message(chat_id, "🎨 Создаю образ...")
                    try:
                        image_path = generate_image(text)
                        with open(image_path, "rb") as img:
                            requests.post(
                                f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                                files={"photo": img},
                                data={"chat_id": chat_id, "caption": f"✨ {text}"}
                            )
                        os.remove(image_path)
                    except Exception as e:
                        safe_log(f"Image error: {e}")
                        send_message(chat_id, "🌫️ Не удалось создать образ.")
                else:
                    send_message(chat_id, "🌿 Считываю твой опыт...")
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    clarification = loop.run_until_complete(query_clarification(chat_id, text))
                    loop.close()
                    
                    if clarification == "ПОЛНО":
                        send_message(chat_id, "🔮 Анализирую...")
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        response = loop.run_until_complete(query_vsegpt(chat_id, text))
                        loop.close()
                        send_message(chat_id, response)
                        last_user_experience[chat_id] = text
                        awaiting_architect[chat_id] = True
                    else:
                        send_message(chat_id, clarification)
                        last_user_experience[chat_id] = text
                        awaiting_clarification[chat_id] = True
                        
                self._send_json(200, {"ok": True})
                return

        self._send_json(200, {"ok": True})

    def log_message(self, format: str, *args) -> None:
        safe_log(f"{self.client_address[0]} - {format % args}")

if __name__ == "__main__":
    if not BOT_TOKEN:
        safe_log("ERROR: BOT_TOKEN is empty")
        sys.exit(1)
    safe_log(f"🚀 Shaman Bot v17 starting on {HOST}:{PORT}")
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
