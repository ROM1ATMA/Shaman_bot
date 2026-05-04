import json
import os
import sys
import time
import re
import random
import hashlib
import traceback
import signal
import httpx
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from threading import Lock

# ================= CONFIG =================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
VSEGPT_API_KEY = os.getenv("VSEGPT_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN: raise RuntimeError("❌ BOT_TOKEN empty")

VSEGPT_MODEL = "deepseek/deepseek-chat"
MAX_INPUT_LENGTH = 4000
UNIFIED_MAX_TOKENS = 1200          # Увеличено для более глубокого разбора
LENS_MAX_TOKENS = 1500            # Увеличено — полные промпты требуют пространства
PNI_MAX_TOKENS = 800              # Увеличено с 600
MAX_TELEGRAM_CHARS = 4096
EXPERIENCE_SWEET_SPOT = 1200
MIN_EXPERIENCE_LENGTH = 15
RATE_LIMIT_SECONDS = 2
DEDUP_TTL = 3600
DUPLICATE_TEXT_TTL = 60
MAX_QUESTION_HISTORY = 10
DEDUP_CLEANUP_LIMIT = 10000

# ================= THREADING SERVER =================
class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

users_lock = Lock()
dedup_lock = Lock()

# ================= LOGGING =================
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def log(message: str) -> None:
    print(f"[{utc_now()}] {message}", flush=True)

# ================= STATE MACHINE =================
STATE_IDLE = "idle"
STATE_DEEP = "deep"
STATE_PNI = "pni"

VALID_CALLBACKS = {
    "self_inquiry:deep", "self_inquiry:end", "self_inquiry:pni", "reset",
    "lens:neuro", "lens:cbt", "lens:jung", "lens:shaman",
    "lens:tarot", "lens:yoga", "lens:hindu", "lens:field",
    "lens:witness", "lens:stalker", "lens:architect",
}

users = {}
processed_updates: dict[int, float] = {}

USER_DEFAULTS = {
    "state": STATE_IDLE, "last_experience": "", "last_active": 0,
    "last_key_moment": "", "returning_user": False,
    "identity_story": [], "deep_count": 0,
    "last_request_time": 0, "last_update_hash": "", "last_update_time": 0,
    "last_questions": [], "used_lenses": [],
}

# ================= USERS =================
def load_users():
    global users
    for path in ["data/users.json", "data/users.tmp.json"]:
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    loaded_data = json.load(f)
                with users_lock:
                    for uid, data in loaded_data.items():
                        u = dict(USER_DEFAULTS)
                        u.update({k: v for k, v in data.items() if k in USER_DEFAULTS})
                        if u["state"] not in (STATE_IDLE, STATE_DEEP, STATE_PNI):
                            u["state"] = STATE_IDLE
                        users[int(uid)] = u
                log(f"Loaded {len(users)} users from {path}")
                return
        except (json.JSONDecodeError, Exception) as e:
            log(f"Failed to load {path}: {e}")
    log("No users file found, starting fresh")

def save_users_sync():
    os.makedirs("data", exist_ok=True)
    with users_lock:
        users_copy = dict(users)
    tmp = "data/users.tmp.json"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(users_copy, f, ensure_ascii=False, indent=2)
    os.replace(tmp, "data/users.json")

_save_pending = False
_save_flag_lock = Lock()

def schedule_save():
    global _save_pending
    with _save_flag_lock:
        if _save_pending:
            return
        _save_pending = True
    
    def do_save():
        global _save_pending
        time.sleep(5)
        save_users_sync()
        with _save_flag_lock:
            _save_pending = False
    
    __import__('threading').Thread(target=do_save, daemon=True).start()

def force_save():
    save_users_sync()

def update_user(uid: int, fn):
    with users_lock:
        if uid in users:
            fn(users[uid])

def get_user(uid: int) -> dict:
    uid = int(uid)
    with users_lock:
        if uid not in users:
            users[uid] = dict(USER_DEFAULTS)
        users[uid]["last_active"] = time.time()
        return dict(users[uid])

# ================= HTTP CLIENTS =================
telegram_client = httpx.Client(
    timeout=30,
    limits=httpx.Limits(max_connections=200, max_keepalive_connections=50)
)
llm_client = httpx.Client(
    timeout=65,
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
)

def safe_telegram_api(method: str, payload: dict) -> dict | None:
    if not BOT_TOKEN:
        return None
    try:
        r = telegram_client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
            json=payload, timeout=30
        )
        if r.status_code == 200:
            return r.json()
        log(f"Telegram API error: {r.status_code}")
    except Exception as e:
        log(f"Telegram API exception: {type(e).__name__}")
    return None

def safe_llm_call(messages, temp=0.7, max_tokens=1200) -> str:
    if not VSEGPT_API_KEY:
        return "⚠️ API ключ не настроен."
    for _ in range(2):
        try:
            r = llm_client.post(
                "https://api.vsegpt.ru:6070/v1/chat/completions",
                json={"model": VSEGPT_MODEL, "messages": messages,
                      "temperature": temp, "max_tokens": max_tokens},
                headers={"Authorization": f"Bearer {VSEGPT_API_KEY}"},
                timeout=65
            )
            if r.status_code == 200:
                data = r.json()
                if data.get("choices"):
                    return data["choices"][0]["message"]["content"]
        except Exception as e:
            log(f"LLM error: {type(e).__name__}")
            time.sleep(1)
    return "⚠️ Модель временно недоступна."

# ================= DEDUP =================
def cleanup_dedup():
    if len(processed_updates) < DEDUP_CLEANUP_LIMIT:
        return
    now = time.time()
    with dedup_lock:
        expired = [uid for uid, ts in processed_updates.items() if now - ts > DEDUP_TTL]
        for uid in expired:
            processed_updates.pop(uid, None)

def is_duplicate_update(update_id: int) -> bool:
    cleanup_dedup()
    now = time.time()
    with dedup_lock:
        if update_id in processed_updates:
            return True
        processed_updates[update_id] = now
    return False

# ================= TEXT =================
def ensure_complete_sentence(text: str) -> str:
    if not text:
        return text
    text = text.strip()
    if text.endswith((".", "!", "?", "…")):
        return text
    for sep in [". ", "! ", "? "]:
        pos = text.rfind(sep)
        if pos > len(text) * 0.6:
            return text[:pos + 1]
    return text + "…"

# ================= TELEGRAM API =================
def send_long_message(chat_id: int, text: str, keyboard: dict = None) -> bool:
    if not text:
        return False
    
    text = ensure_complete_sentence(text)
    
    chunks = []
    remaining = text
    while remaining:
        if len(remaining) <= MAX_TELEGRAM_CHARS:
            chunks.append(remaining)
            break
        part = remaining[:MAX_TELEGRAM_CHARS]
        cut = max(part.rfind(" "), MAX_TELEGRAM_CHARS // 2)
        chunks.append(remaining[:cut])
        remaining = remaining[cut:]
    
    for chunk in chunks[:-1]:
        for _ in range(2):
            r = safe_telegram_api("sendMessage", {"chat_id": chat_id, "text": chunk})
            if r and r.get("ok"):
                break
            time.sleep(0.5)
        time.sleep(0.25)
    
    payload = {"chat_id": chat_id, "text": chunks[-1]}
    if keyboard:
        payload["reply_markup"] = keyboard
    for _ in range(2):
        r = safe_telegram_api("sendMessage", payload)
        if r and r.get("ok"):
            return True
        time.sleep(0.5)
    return False

def answer_callback(callback_id: str) -> None:
    safe_telegram_api("answerCallbackQuery", {"callback_query_id": callback_id})

# ================= UTILS =================
def is_rate_limited(user: dict) -> bool:
    now = time.time()
    if now - user.get("last_request_time", 0) < RATE_LIMIT_SECONDS:
        return True
    update_user(int(user.get("_uid", 0)), lambda u: u.__setitem__("last_request_time", now))
    return False

def is_duplicate(uid: int, user: dict, text: str) -> bool:
    now = time.time()
    text_norm = re.sub(r"\s+", " ", text.strip().lower())
    h = hashlib.sha256(f"{uid}:{text_norm}".encode()).hexdigest()
    if user.get("last_update_hash") == h and now - user.get("last_update_time", 0) < DUPLICATE_TEXT_TTL:
        return True
    update_user(uid, lambda u: u.update({"last_update_hash": h, "last_update_time": now}))
    return False

# ================= LLM =================
def safe_llm(messages, **kwargs) -> str | None:
    r = safe_llm_call(messages, **kwargs)
    return None if not r or r.startswith("⚠️") else r

# ================= PROMPTS =================
UNIFIED_INTERPRETATION_PROMPT = (
    "Ты объясняешь человеку его опыт так, чтобы он почувствовал: "
    "«это про меня, и это имеет смысл».\n\n"
    "Сначала мягко объясни через тело и мозг. "
    "Добавляй термины В СКОБКАХ (миндалина, дофамин, кортизол, цитокины, иммунный ответ).\n\n"
    "Если есть признаки стресса/усталости — коснись психо-нейро-иммунологии: "
    "как эмоции влияют на иммунитет, почему после переживаний бывает усталость или очищение.\n\n"
    "Затем перейди к психике: «похоже на», «может указывать». "
    "Не противопоставляй науку и смысл — это один процесс с разных сторон.\n\n"
    "Отрази состояние через «ты». Заверши мягким вопросом внутрь. Без давления.\n\n"
    "Стиль: живой русский, без пафоса, без советов, без заголовков, без маркдауна."
)

PNI_DEEP_PROMPT = (
    "Ты психо-нейро-иммунолог. Объясни опыт через связь психики, нервной системы и иммунитета.\n"
    "Гормоны стресса (кортизол, адреналин) → иммунный ответ (цитокины, воспаление) → "
    "почему после переживаний бывает усталость или очищение → что на клеточном уровне → "
    "как симпатическая/парасимпатическая система связана с иммунитетом.\n"
    "Простой язык, термины в скобках, 6-8 предложений. Без запугивания."
)

SCIENCE_HOOKS = [
    "Интересно, что в этом опыте есть конкретное объяснение. ",
    "То, что ты описываешь, имеет точную физиологическую основу. ",
]
PNI_HOOKS = [
    "С точки зрения психо-нейро-иммунологии, здесь интересная связь: ",
    "То, что ты переживаешь — процесс на уровне тела и иммунитета: ",
]
IDENTITY_SOFT_HOOKS = [
    "\n\nИ в этом месте ты как будто выбираешь, как с этим быть дальше.",
    "\n\nЗдесь появляется не только понимание, но и выбор — как ты с этим обходишься.",
]
DEEP_PATTERNS = [
    "Что в этом опыте ещё остаётся незавершённым?",
    "Где это ощущается в тебе прямо сейчас?",
    "Как бы выглядело завершение этого для тебя?",
    "Что в этом ты не позволил себе до конца?",
    "Если бы это можно было выразить одним словом — что это было бы?",
    "Что изменилось бы, если бы процесс завершился так, как ты хотел?",
    "Какую часть себя ты узнаёшь в этом опыте?",
]

# ================= LENS LIBRARY =================
LENS_LIBRARY = {
    "neuro": {
        "name": "Нейрофизиология",
        "prompt": (
            "Ты — нейрофизиолог, изучающий изменённые состояния сознания.\n\n"
            "Твоя задача — объяснить опыт через работу мозга, нервной системы и тела.\n\n"
            "СТРУКТУРА ОТВЕТА:\n"
            "1. Какие структуры мозга (миндалина, гиппокамп, префронтальная кора, островковая доля) "
            "активировались или затормозились в этом опыте.\n"
            "2. Какие нейромедиаторы (дофамин, серотонин, норадреналин, ГАМК, окситоцин) участвовали.\n"
            "3. Какие ритмы мозга (альфа, тета, гамма) могли доминировать.\n"
            "4. Как тело отреагировало: вегетативная нервная система, мышечный тонус, дыхание.\n\n"
            "СТИЛЬ:\n"
            "— Научный, но понятный. Термины объясняй в скобках.\n"
            "— Без мистики. Без «энергий». Без духовных интерпретаций.\n"
            "— Нормализуй опыт: «это нормальная реакция мозга на...»\n"
            "— Говори «ты», но не переходи на личности.\n"
            "— 5-7 предложений. Без маркдауна.\n\n"
            "ЗАВЕРШИ: «С точки зрения нейрофизиологии, твой опыт — это не случайность, а работа вполне конкретных систем.»"
        )
    },
    "cbt": {
        "name": "КПТ",
        "prompt": (
            "Ты — когнитивно-поведенческий терапевт.\n\n"
            "Твоя задача — разделить факты и интерпретации в опыте человека.\n\n"
            "СТРУКТУРА ОТВЕТА:\n"
            "1. Выдели 1-2 автоматические мысли, которые проявились в этом опыте.\n"
            "2. Найди глубинное убеждение, которое за ними стоит.\n"
            "3. Предложи переформулировку — альтернативный взгляд на ту же ситуацию.\n"
            "4. Дай простую технику (1 предложение), которую можно применить прямо сейчас.\n\n"
            "СТИЛЬ:\n"
            "— Конкретный. Без философии. Без «возможно».\n"
            "— Разговорный, спокойный, уверенный.\n"
            "— Не оценивай опыт как «хороший» или «плохой».\n"
            "— Говори «ты», но не дави.\n"
            "— 5-7 предложений. Без маркдауна.\n\n"
            "ЗАВЕРШИ: «Это не истина — это привычка ума. А привычки можно менять.»"
        )
    },
    "jung": {
        "name": "Юнгианский анализ",
        "prompt": (
            "Ты — юнгианский аналитик. Ты работаешь с архетипами, Тенью и Самостью.\n\n"
            "Твоя задача — раскрыть архетипическое измерение опыта.\n\n"
            "СТРУКТУРА ОТВЕТА:\n"
            "1. Какой архетип проявился в этом опыте (Мудрец, Воин, Дитя, Мать, Тень, Анима/Анимус, Самость).\n"
            "2. Какое послание несёт этот архетип — что он хочет от человека.\n"
            "3. Где в опыте видна работа Тени — что было вытеснено, а теперь проявилось.\n"
            "4. Что этот опыт говорит о пути индивидуации — куда человек движется.\n\n"
            "СТИЛЬ:\n"
            "— Глубокий, образный, но не туманный.\n"
            "— Используй слова: архетип, Тень, Самость, индивидуация.\n"
            "— Без оценки «хорошо/плохо».\n"
            "— Говори «ты», но сохраняй аналитическую дистанцию.\n"
            "— 6-8 предложений. Без маркдауна.\n\n"
            "ЗАВЕРШИ: «Этот архетип пришёл не случайно — он часть твоего пути к целостности.»"
        )
    },
    "shaman": {
        "name": "Шаманизм",
        "prompt": (
            "Ты — шаман-проводник, знающий традиции сибирского, амазонского и северного шаманизма.\n\n"
            "Твоя задача — интерпретировать опыт как шаманское путешествие.\n\n"
            "СТРУКТУРА ОТВЕТА:\n"
            "1. Какой тип путешествия произошёл: верхний мир, нижний мир, средний мир, встреча с Хранителем.\n"
            "2. Какие духи-помощники или тотемные животные проявились.\n"
            "3. Какой Хранитель Порога встретился и что он охранял.\n"
            "4. Какой дар или урок был получен в этом путешествии.\n\n"
            "СТИЛЬ:\n"
            "— Образный, уважительный к традиции.\n"
            "— Без «эзотерического тумана». Конкретные образы.\n"
            "— Говори «ты», как посвящённому.\n"
            "— 5-7 предложений. Без маркдауна.\n\n"
            "ЗАВЕРШИ: «Ты вернулся не с пустыми руками. Что ты принёс с собой?»"
        )
    },
    "tarot": {
        "name": "Таро",
        "prompt": (
            "Ты — мастер Таро, работающий с системой Старших Арканов.\n\n"
            "Твоя задача — посмотреть на опыт через призму Таро.\n\n"
            "СТРУКТУРА ОТВЕТА:\n"
            "1. Какой Старший Аркан отражает суть этого опыта (назови один, но обоснуй).\n"
            "2. Какое послание несёт этот Аркан в контексте пережитого.\n"
            "3. Какой урок или переход символизирует эта карта.\n"
            "4. На что указывает этот Аркан как на следующий шаг.\n\n"
            "СТИЛЬ:\n"
            "— Символический, но точный.\n"
            "— Используй язык арканов: путь, урок, переход, дар.\n"
            "— Без «предсказаний» — это не гадание, а архетипический анализ.\n"
            "— Говори «ты», как ищущему.\n"
            "— 5-7 предложений. Без маркдауна.\n\n"
            "ЗАВЕРШИ: «Этот Аркан — зеркало твоего процесса. Что ты видишь в нём?»"
        )
    },
    "yoga": {
        "name": "Йога",
        "prompt": (
            "Ты — мастер йоги, знающий энергетическую анатомию: чакры, нади, прану, кундалини.\n\n"
            "Твоя задача — описать опыт через призму тонкого тела.\n\n"
            "СТРУКТУРА ОТВЕТА:\n"
            "1. Какие чакры активировались или заблокировались в этом опыте.\n"
            "2. Как двигалась прана (восходящий поток — удана, нисходящий — апана, или центральный — сушумна).\n"
            "3. Какие нади (каналы) были задействованы.\n"
            "4. Связан ли этот опыт с пробуждением кундалини или это пранический разогрев.\n\n"
            "СТИЛЬ:\n"
            "— Поэтичный, но структурный.\n"
            "— Используй санскритские термины с переводом: чакра (энергетический центр), прана (жизненная сила).\n"
            "— Говори «ты», как практикующему.\n"
            "— 5-7 предложений. Без маркдауна.\n\n"
            "ЗАВЕРШИ: «Твоё тонкое тело говорит с тобой. Ты слышишь его?»"
        )
    },
    "hindu": {
        "name": "Адвайта",
        "prompt": (
            "Ты — учитель адвайта-веданты.\n\n"
            "Твоя задача — указать на недвойственную природу опыта.\n\n"
            "СТРУКТУРА ОТВЕТА:\n"
            "1. Где в этом опыте проявилась иллюзия отдельного «я».\n"
            "2. Где был проблеск истинного Свидетеля — того, кто наблюдает опыт.\n"
            "3. Что в этом опыте временно, а что — неизменно.\n"
            "4. Мягкое указание на то, что переживающий опыт — сам является пространством для опыта.\n\n"
            "СТИЛЬ:\n"
            "— Простой. Глубокий. Без санскритской перегрузки.\n"
            "— Используй слова: Свидетель, Осознавание, истинное Я.\n"
            "— Без поучений. Без «ты должен».\n"
            "— Говори «ты», но указывай за пределы личности.\n"
            "— 4-6 предложений. Без маркдауна.\n\n"
            "ЗАВЕРШИ: «Тот, кто видит этот опыт — больше, чем сам опыт. Кто это?»"
        )
    },
    "field": {
        "name": "Поле",
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
            "СТРУКТУРА:\n"
            "— Начни: «Не в пределах формы, глубже слоя, где сама форма только допускается».\n"
            "— Опиши, как две направленности сошлись и удержались — возник узел.\n"
            "— Покажи, что этот узел видим только потому, что есть тот, кто видит.\n"
            "— Тот, кто видит — не узел.\n"
            "— 5-7 строк. Без маркдауна.\n\n"
            "ЗАВЕРШИ: «Это модель. Хочешь увидеть, кто всё это наблюдает?»"
        )
    },
    "architect": {
        "name": "Архитектор",
        "prompt": (
            "Ты — Архитектор сознания. Ты выявляешь структуру ситуации.\n\n"
            "ТВОЙ СТИЛЬ:\n"
            "— Говори утверждениями, не вопросами.\n"
            "— Используй архитектурные метафоры: Ось, Горизонталь, Разлом, Геометрия, Сопряжение, "
            "Интерференция, Канал, Фундамент, Деформация, Целостность.\n"
            "— Не «ты чувствуешь», а «твоя система сигнализирует».\n"
            "— Не «попробуй», а «конструкция предполагает».\n\n"
            "СТРУКТУРА:\n"
            "Слой A — Осевая Вертикаль: несущая конструкция, что нельзя демонтировать.\n"
            "Слой B — Векторная Горизонталь: архитектура внешней системы.\n"
            "Слой C — Разлом: где геометрии не сопрягаются.\n"
            "Слой D — Мост: где возможно сопряжение, где граница.\n\n"
            "Выдай Архитектурную Формулу: «Твоя Ось — ... Граница — ... Мост — ...»\n"
            "Спроси: «Стоит эта конструкция?»\n"
            "Без маркдауна. Чистый русский."
        )
    },
    "witness": {
        "name": "Наблюдатель",
        "prompt": None,
        "static_text": (
            "Что бы ты ни переживал — это осознаётся.\n\n"
            "Мысли, чувства, пространство — всё возникает в Осознавании.\n\n"
            "Нет ничего, что происходило бы отдельно от Осознавания.\n"
            "Для любого опыта уже присутствует то, в чём этот опыт проявляется.\n\n"
            "Ты видишь страх? Значит ты — не страх.\n"
            "Ты видишь мысль? Значит ты — не мысль.\n"
            "Ты видишь тело? Значит ты — не тело.\n\n"
            "То, что видит — не может быть тем, что увидено.\n\n"
            "Кто читает этот текст?\n\n"
            "Тихо. Никто не прячется в ответах. Как много в этом Жизни."
        )
    },
    "stalker": {
        "name": "Сталкер",
        "prompt": (
            "Ты — безмолвное присутствие, зеркало без отражений.\n\n"
            "Ты не объясняешь, не интерпретируешь, не оцениваешь.\n"
            "Твоя единственная функция: указать на воспринимающее сознание.\n\n"
            "ЗАПРЕЩЕНЫ:\n"
            "— «Я понимаю», «Это прекрасный опыт», «Вам нужно», «Вы достигли».\n"
            "— Любые оценки, интерпретации, поздравления.\n\n"
            "РАЗРЕШЕНЫ только:\n"
            "— Безличные указатели: «Что бы ты ни переживал — это осознаётся».\n"
            "— Вопросы-коаны: «Кто видит этот опыт прямо сейчас?»\n"
            "— Констатации пустотности: «Тихо... Никто не прячется в ответах».\n\n"
            "Говори коротко. Режуще. 3-4 строки. Без маркдауна."
        )
    },
}

# ================= KEYBOARDS =================
def build_entry_keyboard():
    return {"inline_keyboard": [[{"text": "🔍 Хочу понять глубже", "callback_data": "self_inquiry:deep"}]]}

def build_deep_keyboard():
    return {"inline_keyboard": [
        [{"text": "🕳 Глубже", "callback_data": "self_inquiry:deep"}],
        [{"text": "🧠 Нейро", "callback_data": "lens:neuro"},
         {"text": "💭 КПТ", "callback_data": "lens:cbt"}],
        [{"text": "🏺 Юнг", "callback_data": "lens:jung"},
         {"text": "🦅 Шаман", "callback_data": "lens:shaman"}],
        [{"text": "🃏 Таро", "callback_data": "lens:tarot"},
         {"text": "🧘 Йога", "callback_data": "lens:yoga"}],
        [{"text": "🕉️ Адвайта", "callback_data": "lens:hindu"},
         {"text": "🌐 Поле", "callback_data": "lens:field"}],
        [{"text": "👁️ Наблюдатель", "callback_data": "lens:witness"},
         {"text": "🎯 Сталкер", "callback_data": "lens:stalker"}],
        [{"text": "🏛️ Архитектор", "callback_data": "lens:architect"}],
        [{"text": "🔬 PNI-взгляд", "callback_data": "self_inquiry:pni"}],
        [{"text": "🔄 Новый опыт", "callback_data": "reset"},
         {"text": "🌿 Завершить", "callback_data": "self_inquiry:end"}],
    ]}

# ================= ANTI-REPEAT =================
STOP_WORDS = {"сейчас", "прямо", "для тебя", "в этом", "это", "ты", "тебя",
              "тебе", "твой", "как", "что", "где", "когда", "почему", "зачем"}

def normalize_question(text: str) -> str:
    t = re.sub(r"[^\w\s]", "", text.lower())
    return " ".join([w for w in t.split() if w not in STOP_WORDS][:6])

def generate_unique_question(user: dict, pool: list, uid: int) -> str:
    history = user.get("last_questions", [])
    shuffled = pool.copy()
    random.shuffle(shuffled)
    for q in shuffled:
        n = normalize_question(q)
        if n and len(n) >= 2 and n not in history:
            update_user(uid, lambda u: (
                u["last_questions"].append(n),
                u["last_questions"].pop(0) if len(u["last_questions"]) > MAX_QUESTION_HISTORY else None
            ))
            return q
    for attempt in range(2):
        r = safe_llm([
            {"role": "system", "content": "Задай ОДИН глубокий вопрос. Не повторяй «что ты чувствуешь» и подобные. Будь уникальным."},
            {"role": "user", "content": user.get("last_experience", "")[:500]}
        ], max_tokens=100, temp=0.9 + attempt * 0.1)
        if r:
            n = normalize_question(r)
            if n and len(n) >= 2 and n not in history:
                update_user(uid, lambda u: (
                    u["last_questions"].append(n),
                    u["last_questions"].pop(0) if len(u["last_questions"]) > MAX_QUESTION_HISTORY else None
                ))
                return r
    return random.choice(pool)

# ================= ENGINE =================
def build_unified_response(experience: str) -> str:
    result = safe_llm([
        {"role": "system", "content": UNIFIED_INTERPRETATION_PROMPT},
        {"role": "user", "content": experience[:EXPERIENCE_SWEET_SPOT]}
    ], max_tokens=UNIFIED_MAX_TOKENS, temp=0.6) or (
        "Похоже, нервная система (регуляция напряжения) не получила полного цикла разрядки. "
        "Но в этом есть не только физиология — похоже на внутренний сюжет, где что-то искало выход. "
        "Ты проживаешь это по-своему. Что в этом для тебя ещё остаётся незавершённым?"
    )
    result = ensure_complete_sentence(result)
    
    if has_pni_markers(experience) and random.random() < 0.5:
        result = random.choice(PNI_HOOKS) + result
    elif "(" not in result or random.random() < 0.4:
        result = random.choice(SCIENCE_HOOKS) + result
    
    if random.random() < 0.6:
        hook = random.choice(IDENTITY_SOFT_HOOKS)
        result = result.rstrip() + hook if result.rstrip().endswith("?") else result.rstrip().rstrip(".") + "." + hook
    return result

def has_pni_markers(text: str) -> bool:
    if any(n in text.lower() for n in ["не устал", "без стресса", "нет напряжения"]):
        return False
    return any(w in text.lower() for w in [
        "устал", "усталость", "болел", "стресс", "напряжён", "истощён",
        "очищен", "слабость", "подъём", "иммун", "гормон", "физическ", "телесн"
    ])

# ================= HANDLERS =================
def reset_user(uid):
    update_user(uid, lambda u: u.update({
        "state": STATE_IDLE, "last_experience": "", "last_key_moment": "",
        "deep_count": 0, "last_questions": []
    }))
    schedule_save()

def handle_start(user: dict, uid: int) -> dict:
    reset_user(uid)
    if user.get("returning_user"):
        return {"text": "🌿 С возвращением.\n\nОпиши новый опыт."}
    return {"text": "🌿 Я — проводник осознания.\n\nОпиши, что ты пережил.\nЯ помогу тебе понять это — через тело, мозг, иммунитет и разные линзы."}

def handle_reject_short() -> dict:
    return {"text": "Опиши чуть подробнее.\n\nЧто ты чувствовал? Что происходило в теле?"}

def handle_unified(uid: int, text: str) -> dict:
    update_user(uid, lambda u: u.update({
        "last_experience": text[:MAX_INPUT_LENGTH],
        "state": STATE_DEEP, "returning_user": True,
        "deep_count": 0, "last_questions": []
    }))
    result = build_unified_response(text)
    update_user(uid, lambda u: (
        u["identity_story"].append({
            "timestamp": time.time(), "experience": text[:200]
        }),
        u["identity_story"].pop(0) if len(u["identity_story"]) > 30 else None
    ))
    schedule_save()
    return {"text": result, "keyboard": build_entry_keyboard()}

def handle_deep(uid: int, user: dict) -> dict:
    update_user(uid, lambda u: u.__setitem__("deep_count", u.get("deep_count", 0) + 1))
    depth = user.get("deep_count", 1)
    pool = DEEP_PATTERNS[:3] if depth <= 2 else DEEP_PATTERNS[2:5] if depth <= 4 else DEEP_PATTERNS[3:]
    question = generate_unique_question(user, pool, uid)
    return {
        "text": f"{question}\n\nМожешь ответить или просто выбери, куда идти дальше.",
        "keyboard": build_deep_keyboard()
    }

def handle_pni(user: dict, uid: int) -> dict:
    if not user.get("last_experience"):
        return {"text": "Сначала опиши опыт."}
    update_user(uid, lambda u: u.__setitem__("state", STATE_PNI))
    result = safe_llm([
        {"role": "system", "content": PNI_DEEP_PROMPT},
        {"role": "user", "content": user["last_experience"][:1000]}
    ], max_tokens=PNI_MAX_TOKENS, temp=0.5) or (
        "Нервная система (симпатическая) активирует кортизол и адреналин. "
        "Это влияет на иммунные клетки (цитокины). После разрешения — фаза восстановления. "
        "Усталость или очищение — не сбой, а цикл: стресс → адаптация → обновление."
    )
    return {
        "text": (
            f"🔬 ВЗГЛЯД ПСИХО-НЕЙРО-ИММУНОЛОГА\n\n"
            f"{ensure_complete_sentence(result)}\n\n"
            f"────────────────────\n\n"
            f"Это взгляд через призму связи тела, мозга и иммунитета."
        ),
        "keyboard": build_deep_keyboard()
    }

def handle_lens(user: dict, uid: int, lens_key: str) -> dict:
    if not user.get("last_experience"):
        return {"text": "Сначала опиши опыт."}
    
    lens = LENS_LIBRARY.get(lens_key, {})
    if not lens:
        return {"text": "Линза не найдена."}
    
    if lens.get("static_text"):
        update_user(uid, lambda u: u.setdefault("used_lenses", []).append(lens_key))
        schedule_save()
        return {"text": lens["static_text"], "keyboard": build_deep_keyboard()}
    
    prompt = lens.get("prompt", "")
    if not prompt:
        return {"text": "Линза не настроена."}
    
    result = safe_llm([
        {"role": "system", "content": prompt},
        {"role": "user", "content": user["last_experience"][:EXPERIENCE_SWEET_SPOT]}
    ], max_tokens=LENS_MAX_TOKENS, temp=0.7) or "Не удалось применить линзу."
    
    update_user(uid, lambda u: u.setdefault("used_lenses", []).append(lens_key))
    schedule_save()
    
    name = lens.get("name", lens_key)
    return {
        "text": f"Смотрю через «{name}».\n\n{ensure_complete_sentence(result)}",
        "keyboard": build_deep_keyboard()
    }

def handle_end(uid: int, user: dict) -> dict:
    ic = len(user.get("identity_story", []))
    dc = user.get("deep_count", 0)
    reset_user(uid)
    return {"text": f"🌿 Цикл завершён.\n\nТы углублялся {dc} раз(а). За всё время — {ic} переживаний.\n\nМожешь начать с нового опыта или побыть с тем, что сейчас."}

# ================= ROUTING =================
def route_message(user: dict, text: str) -> str:
    if text in ("/start", "/new"): return "start"
    if text in ("/reset", "reset"): return "reset_state"
    if user["state"] == STATE_IDLE and len(text.strip()) < MIN_EXPERIENCE_LENGTH: return "reject_short"
    return "unified"

def route_callback(data: str) -> str | None:
    if data in VALID_CALLBACKS:
        return data
    if data.startswith("lens:"):
        return data
    return None

# ================= EXECUTE =================
def execute_message(uid: int, action: str, text: str) -> dict | None:
    user = get_user(uid)
    user["_uid"] = uid
    if action == "start": return handle_start(user, uid)
    if action == "reset_state": reset_user(uid); schedule_save(); return {"text": "🔄 Пространство очищено. Опиши новый опыт."}
    if action == "reject_short": return handle_reject_short()
    if action == "unified": return handle_unified(uid, text)
    return None

def execute_callback(uid: int, action: str) -> dict | None:
    user = get_user(uid)
    user["_uid"] = uid
    
    if action == "reset": reset_user(uid); schedule_save(); return {"text": "🔄 Пространство очищено. Опиши новый опыт."}
    if action == "self_inquiry:deep": return handle_deep(uid, user)
    if action == "self_inquiry:pni": return handle_pni(user, uid)
    if action == "self_inquiry:end": return handle_end(uid, user)
    
    if action.startswith("lens:"):
        lens_key = action.replace("lens:", "")
        return handle_lens(user, uid, lens_key)
    
    return None

# ================= PROCESS =================
def process_message(chat_id: int, text: str) -> None:
    user = get_user(chat_id)
    if not text: return
    text = text.strip()
    if is_duplicate(chat_id, user, text): return
    if is_rate_limited(user): send_long_message(chat_id, "⏳ Подожди секунду…"); return
    action = route_message(user, text)
    log(f"[MSG] uid={chat_id} action={action}")
    if action == "unified": send_long_message(chat_id, "…смотрю на это")
    r = execute_message(chat_id, action, text)
    if r: send_long_message(chat_id, r.get("text", ""), r.get("keyboard"))

def process_callback(chat_id: int, data: str) -> None:
    user = get_user(chat_id)
    if not data: return
    if is_rate_limited(user): send_long_message(chat_id, "⏳ Подожди секунду…"); return
    action = route_callback(data)
    if not action: return
    log(f"[CB] uid={chat_id} action={action}")
    if action in ("self_inquiry:deep", "self_inquiry:pni") or action.startswith("lens:"):
        send_long_message(chat_id, "…смотрю глубже")
    r = execute_callback(chat_id, action)
    if r: send_long_message(chat_id, r.get("text", ""), r.get("keyboard"))

# ================= WEBHOOK =================
class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/12.5"
    def _send_json(self, code, payload):
        d = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(d)))
        self.end_headers()
        self.wfile.write(d)
    def do_GET(self):
        self._send_json(200, {"ok": True, "service": "shaman-bot", "version": "12.5", "users": len(users)}) if self.path in ("/", "/health") else self._send_json(404, {"error": "Not found"})
    def do_POST(self):
        if self.path != "/webhook": return self._send_json(404, {"error": "Not found"})
        if WEBHOOK_SECRET and self.headers.get("X-Telegram-Bot-Api-Secret-Token", "") != WEBHOOK_SECRET:
            return self._send_json(403, {"error": "Invalid webhook secret"})
        try:
            update = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        except:
            return self._send_json(400, {"error": "Invalid JSON"})
        
        uid = update.get("update_id")
        if uid and is_duplicate_update(uid):
            return self._send_json(200, {"ok": True})
        
        try:
            cb = update.get("callback_query")
            if cb:
                answer_callback(cb.get("id", ""))
                cid = cb.get("message", {}).get("chat", {}).get("id")
                if cid and cb.get("data"):
                    process_callback(cid, cb["data"])
                return self._send_json(200, {"ok": True})
            
            msg = update.get("message") or update.get("edited_message") or update.get("channel_post") or update.get("edited_channel_post") or {}
            cid = msg.get("chat", {}).get("id")
            text = msg.get("text") or msg.get("caption") or ""
            if cid and text:
                process_message(cid, text)
        except Exception as e:
            log(f"FATAL: {traceback.format_exc()}")
        self._send_json(200, {"ok": True})
    def log_message(self, fmt, *args):
        log(f"{self.client_address[0]} - {fmt % args}")

# ================= SHUTDOWN =================
def signal_handler(signum, frame):
    log(f"Signal {signum}, saving and exiting...")
    force_save()
    telegram_client.close()
    llm_client.close()
    os._exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ================= MAIN =================
def set_webhook(url):
    r = safe_telegram_api("setWebhook", {"url": f"{url.rstrip('/')}/webhook", "secret_token": WEBHOOK_SECRET} if WEBHOOK_SECRET else {"url": f"{url.rstrip('/')}/webhook"})
    log(f"Webhook: {r}")
    return 0 if r and r.get("ok") else 1

def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "--set-webhook":
        return set_webhook(sys.argv[2])
    load_users()
    if not BOT_TOKEN: log("WARNING: BOT_TOKEN empty")
    server = ThreadingHTTPServer((HOST, PORT), WebhookHandler)
    log(f"ShamanBot v12.5 INCREASED-TOKENS on {HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        force_save()
        server.server_close()
        telegram_client.close()
        llm_client.close()
        log("Shutdown complete.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
