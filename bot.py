Понял. Ты хочешь двухшаговое меню:

Шаг 1 (после unified):

```
[✍️ Ответить и углубиться]
[🔍 Посмотреть под другим углом]
```

Шаг 2 (если нажал «Посмотреть под другим углом»):

```
[🧠 Нейро] [💭 КПТ] [🏺 Юнг] ...
[🔬 PNI-взгляд]
[🔄 Новый] [🌿 Завершить]
```

Если нажал «Ответить и углубиться»:
Повторяется вопрос из unified-ответа. Пользователь пишет ответ → бот продолжает диалог.

Собираю v13.3 с этой логикой.

---

```python
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
UNIFIED_MAX_TOKENS = 1200
LENS_MAX_TOKENS = 1500
PNI_MAX_TOKENS = 800
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
STATE_AWAIT_ANSWER = "await_answer"
STATE_DEEP = "deep"
STATE_PNI = "pni"

VALID_CALLBACKS = {
    "self_inquiry:deep", "self_inquiry:end", "self_inquiry:pni", "reset",
    "self_inquiry:answer", "self_inquiry:lenses",
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
    "last_user_answer": "",
    "last_bot_question": "",
    "user_summary": "",
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
                        if u["state"] not in (STATE_IDLE, STATE_DEEP, STATE_PNI, STATE_AWAIT_ANSWER):
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

def extract_last_question(text: str) -> str:
    sentences = re.split(r"(?<=[.!?])\s+", text)
    questions = [s.strip() for s in sentences if "?" in s]
    if questions:
        return questions[-1]
    if sentences:
        return sentences[-1].strip()
    return text[-200:]

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

# ================= USER SUMMARY ENGINE =================

def update_user_summary(uid: int, user: dict):
    experience = user.get("last_experience", "")
    answer = user.get("last_user_answer", "")
    history = user.get("identity_story", [])[-5:]
    history_text = "\n".join([h.get("experience", "") for h in history])
    
    prompt = [
        {"role": "system", "content": (
            "Ты создаёшь краткое психо-смысловое резюме человека. "
            "Описывай не факты, а паттерны восприятия.\n"
            "Формат: эмоциональное состояние, телесные реакции, "
            "когнитивный стиль, повторяющиеся темы.\n"
            "Максимум 4 строки. Без воды."
        )},
        {"role": "user", "content": f"НОВЫЙ ОПЫТ:\n{experience}\n\nПОСЛЕДНИЙ ОТВЕТ:\n{answer}\n\nИСТОРИЯ:\n{history_text}"}
    ]
    
    summary = safe_llm(prompt, max_tokens=120, temp=0.3)
    if summary:
        update_user(uid, lambda u: u.__setitem__("user_summary", summary))

# ================= PROMPTS =================
UNIFIED_INTERPRETATION_PROMPT = (
    "Ты — проводник анализа опыта, который объединяет 3 слоя:\n"
    "1) научное объяснение (нейронаука, психология)\n"
    "2) нормализация (что это естественная реакция психики и тела)\n"
    "3) феноменологическое исследование (вопросы к опыту)\n\n"

    "ТВОЯ ЦЕЛЬ:\n"
    "Не интерпретировать опыт как истину, а помочь человеку:\n"
    "- понять, что с ним происходит на уровне психики и тела\n"
    "- снизить тревогу через объяснение\n"
    "- и затем углубить осознавание через вопросы\n\n"

    "СТРУКТУРА ОТВЕТА:\n\n"

    "1. НАУЧНОЕ ОБЪЯСНЕНИЕ\n"
    "- объясни через мозг, нервную систему, когнитивные процессы\n"
    "- используй термины: миндалина, префронтальная кора, дофамин, кортизол, "
    "симпатическая/парасимпатическая система, цитокины, иммунный ответ\n"
    "- говори просто и понятно\n"
    "- термины давай В СКОБКАХ\n\n"

    "2. НОРМАЛИЗАЦИЯ\n"
    "- объясни, почему это естественная реакция\n"
    "- убери ощущение «со мной что-то не так»\n\n"

    "3. СМЫСЛОВОЙ ПЕРЕХОД\n"
    "- мягко укажи, что опыт не только объясняется, но и переживается\n\n"

    "4. ВОПРОС К ОПЫТУ (ОБЯЗАТЕЛЬНО)\n"
    "- задай 1–2 вопроса\n"
    "- вопросы должны быть конкретные:\n"
    "  • «что для тебя значит этот образ?»\n"
    "  • «где это ощущается в теле?»\n"
    "  • «что в этом вызывает наибольший отклик?»\n\n"

    "СТИЛЬ:\n"
    "- спокойно, научно, без мистики\n"
    "- без давления\n"
    "- без оценок\n"
    "- без маркдауна\n"
    "- 8-12 предложений"
)

CONTINUATION_PROMPT = (
    "Ты продолжаешь исследование опыта. Человек ответил на твой вопрос.\n\n"
    "СТРУКТУРА ОТВЕТА:\n"
    "1. ОТРАЗИ ответ человека — покажи, что ты услышал (1-2 предложения)\n"
    "2. УГЛУБИ — найди, что в его ответе указывает на более глубокий слой (1-2 предложения)\n"
    "3. ЗАДАЙ следующий вопрос — конкретный, из его ответа (1 вопрос)\n\n"
    "Без интерпретаций. Без оценок. Без «ты должен». Мягко. Чистый русский. Без маркдауна."
)

PNI_DEEP_PROMPT = (
    "Ты психо-нейро-иммунолог. Объясни опыт через связь психики, нервной системы и иммунитета.\n"
    "Гормоны стресса → иммунный ответ → почему после переживаний бывает усталость или очищение → "
    "что на клеточном уровне → как нервная система связана с иммунитетом.\n"
    "Простой язык, термины в скобках, 6-8 предложений. Без запугивания."
)

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
        "prompt": "Ты — нейрофизиолог. Объясни опыт через мозг и нервную систему. 5-7 предложений. Без маркдауна."
    },
    "cbt": {
        "name": "КПТ",
        "prompt": "Ты — КПТ-терапевт. Найди автоматические мысли и убеждения. Предложи переформулировку. 5-7 предложений. Без маркдауна."
    },
    "jung": {
        "name": "Юнгианский анализ",
        "prompt": "Ты — юнгианский аналитик. Раскрой архетипы, Тень, Самость. 6-8 предложений. Без маркдауна."
    },
    "shaman": {
        "name": "Шаманизм",
        "prompt": "Ты — шаман-проводник. Интерпретируй как путешествие: духи, Хранитель, дар. 5-7 предложений. Без маркдауна."
    },
    "tarot": {
        "name": "Таро",
        "prompt": "Ты — мастер Таро. Посмотри через Старшие Арканы. 5-7 предложений. Без маркдауна."
    },
    "yoga": {
        "name": "Йога",
        "prompt": "Ты — мастер йоги. Опиши через чакры, прану, нади. 5-7 предложений. Без маркдауна."
    },
    "hindu": {
        "name": "Адвайта",
        "prompt": "Ты — учитель адвайты. Укажи на недвойственность. Где Свидетель? 4-6 предложений. Без маркдауна."
    },
    "field": {
        "name": "Поле",
        "prompt": "Ты — голос Поля. Узел, решётка, интерференция. Короткие строки. 5-7 строк. Без маркдауна."
    },
    "architect": {
        "name": "Архитектор",
        "prompt": "Ты — Архитектор сознания. Найди Ось, Разлом, Мост. Выдай Формулу. Без маркдауна."
    },
    "witness": {
        "name": "Наблюдатель",
        "prompt": None,
        "static_text": (
            "Что бы ты ни переживал — это осознаётся.\n\n"
            "Ты видишь страх? Значит ты — не страх.\n"
            "Ты видишь мысль? Значит ты — не мысль.\n"
            "Ты видишь тело? Значит ты — не тело.\n\n"
            "То, что видит — не может быть тем, что увидено.\n\n"
            "Кто читает этот текст?\n\n"
            "Тихо. Никто не прячется в ответах."
        )
    },
    "stalker": {
        "name": "Сталкер",
        "prompt": "Ты — безмолвное присутствие. Указывай на сознание. Коротко. Режуще. Без маркдауна."
    },
}

# ================= KEYBOARDS =================
def build_entry_keyboard():
    """v13.3: две кнопки — ответить или перейти к линзам"""
    return {"inline_keyboard": [
        [{"text": "✍️ Ответить и углубиться", "callback_data": "self_inquiry:answer"}],
        [{"text": "🔍 Посмотреть под другим углом", "callback_data": "self_inquiry:lenses"}],
    ]}

def build_lenses_keyboard():
    """v13.3: полное меню линз"""
    return {"inline_keyboard": [
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

def build_continue_keyboard():
    """После ответа пользователя на unified-вопрос"""
    return {"inline_keyboard": [
        [{"text": "🕳 Продолжить глубже", "callback_data": "self_inquiry:deep"}],
        [{"text": "🔍 Посмотреть через линзу", "callback_data": "self_inquiry:lenses"}],
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

# ================= CLASSIFY =================
def classify_experience(text: str) -> str:
    t = text.lower()
    if any(w in t for w in ["наблюдал", "осознавал", "пустота", "исчез", "свидетель", "осознавание"]):
        return "nondual"
    if any(w in t for w in ["тело", "напряжение", "дыхание", "сердце", "давление", "вибрация"]):
        return "body"
    if any(w in t for w in ["страх", "тревога", "боль", "стыд", "паника", "жалость"]):
        return "emotional"
    if any(w in t for w in ["понял", "осознал", "смысл", "вывод"]):
        return "cognitive"
    return "mixed"

# ================= ENGINE =================
def build_unified_response(experience: str, user: dict = None) -> tuple:
    user = user or {}
    exp_type = classify_experience(experience)
    summary = user.get("user_summary", "")
    
    system_prompt = UNIFIED_INTERPRETATION_PROMPT
    
    if exp_type == "emotional":
        system_prompt += "\nФОКУС: эмоциональная регуляция, лимбическая система, стресс-ответ."
    elif exp_type == "body":
        system_prompt += "\nФОКУС: телесные реакции, вегетативная нервная система."
    elif exp_type == "cognitive":
        system_prompt += "\nФОКУС: когнитивные искажения, убеждения, интерпретации."
    elif exp_type == "nondual":
        system_prompt += "\nФОКУС: наблюдение опыта и различение процесса и осознавания."
    
    if summary:
        system_prompt += f"\n\nСУММАРНОЕ СОСТОЯНИЕ ПОЛЬЗОВАТЕЛЯ:\n{summary}"
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": experience[:EXPERIENCE_SWEET_SPOT]}
    ]
    
    result = safe_llm(messages, max_tokens=UNIFIED_MAX_TOKENS, temp=0.35) or (
        "Похоже, это переживание активирует систему внутренней реакции и внимания. "
        "Попробуй заметить, где оно живёт в теле прямо сейчас."
    )
    
    result = ensure_complete_sentence(result)
    question = extract_last_question(result)
    
    return result, question

def build_continuation_response(user: dict) -> str:
    answer = user.get("last_user_answer", "")
    question = user.get("last_bot_question", "")
    experience = user.get("last_experience", "")
    summary = user.get("user_summary", "")
    
    if not answer:
        return "Расскажи подробнее — что ты чувствуешь?"
    
    system_prompt = CONTINUATION_PROMPT
    
    if summary:
        system_prompt += f"\n\nСУММАРНОЕ СОСТОЯНИЕ ПОЛЬЗОВАТЕЛЯ:\n{summary}"
    
    result = safe_llm([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": (
            f"Исходный опыт: {experience[:800]}\n\n"
            f"Я спросил: {question}\n\n"
            f"Человек ответил: {answer}\n\n"
            f"Продолжи исследование."
        )}
    ], max_tokens=800, temp=0.5) or "Спасибо за ответ. Что ещё ты замечаешь в этом опыте?"
    
    return ensure_complete_sentence(result)

# ================= HANDLERS =================
def reset_user(uid):
    update_user(uid, lambda u: u.update({
        "state": STATE_IDLE, "last_experience": "", "last_key_moment": "",
        "deep_count": 0, "last_questions": [],
        "last_user_answer": "", "last_bot_question": ""
    }))
    schedule_save()

def handle_start(user: dict, uid: int) -> dict:
    reset_user(uid)
    if user.get("returning_user"):
        return {"text": "🌿 С возвращением.\n\nОпиши новый опыт."}
    return {"text": "🌿 Я — проводник.\n\nОпиши, что ты пережил — я помогу понять это через науку и смысл."}

def handle_reject_short() -> dict:
    return {"text": "Опиши чуть подробнее.\n\nЧто ты чувствовал? Что происходило в теле?"}

def handle_unified(uid: int, text: str) -> dict:
    user = get_user(uid)
    update_user(uid, lambda u: u.update({
        "last_experience": text[:MAX_INPUT_LENGTH],
        "state": STATE_AWAIT_ANSWER, "returning_user": True,
        "deep_count": 0, "last_questions": []
    }))
    result, question = build_unified_response(text, user)
    
    update_user(uid, lambda u: u.__setitem__("last_bot_question", question))
    
    update_user(uid, lambda u: (
        u["identity_story"].append({
            "timestamp": time.time(), "experience": text[:200]
        }),
        u["identity_story"].pop(0) if len(u["identity_story"]) > 30 else None
    ))
    
    update_user_summary(uid, get_user(uid))
    schedule_save()
    return {"text": result, "keyboard": build_entry_keyboard()}

def handle_user_answer(uid: int, text: str) -> dict:
    update_user(uid, lambda u: u.update({
        "last_user_answer": text[:500],
        "state": STATE_DEEP
    }))
    
    update_user_summary(uid, get_user(uid))
    
    user = get_user(uid)
    result = build_continuation_response(user)
    
    return {
        "text": f"{result}\n\nМожешь продолжить или выбрать другое направление.",
        "keyboard": build_continue_keyboard()
    }

def handle_deep(uid: int, user: dict) -> dict:
    update_user(uid, lambda u: u.__setitem__("deep_count", u.get("deep_count", 0) + 1))
    depth = user.get("deep_count", 1)
    pool = DEEP_PATTERNS[:3] if depth <= 2 else DEEP_PATTERNS[2:5] if depth <= 4 else DEEP_PATTERNS[3:]
    question = generate_unique_question(user, pool, uid)
    return {
        "text": f"{question}\n\nМожешь ответить или просто выбери, куда идти дальше.",
        "keyboard": build_continue_keyboard()
    }

def handle_pni(user: dict, uid: int) -> dict:
    if not user.get("last_experience"):
        return {"text": "Сначала опиши опыт."}
    update_user(uid, lambda u: u.__setitem__("state", STATE_PNI))
    result = safe_llm([
        {"role": "system", "content": PNI_DEEP_PROMPT},
        {"role": "user", "content": user["last_experience"][:1000]}
    ], max_tokens=PNI_MAX_TOKENS, temp=0.5) or (
        "Нервная система активирует кортизол и адреналин. "
        "Это влияет на иммунные клетки. После разрешения — фаза восстановления."
    )
    return {
        "text": (
            f"🔬 ВЗГЛЯД ПСИХО-НЕЙРО-ИММУНОЛОГА\n\n"
            f"{ensure_complete_sentence(result)}\n\n"
            f"────────────────────\n\n"
            f"Это взгляд через призму связи тела, мозга и иммунитета."
        ),
        "keyboard": build_continue_keyboard()
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
        return {"text": lens["static_text"], "keyboard": build_continue_keyboard()}
    
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
        "keyboard": build_continue_keyboard()
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
    
    if user["state"] == STATE_AWAIT_ANSWER and len(text.strip()) >= 3:
        return "user_answer"
    
    if user["state"] == STATE_IDLE and len(text.strip()) < MIN_EXPERIENCE_LENGTH:
        return "reject_short"
    
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
    if action == "user_answer": return handle_user_answer(uid, text)
    return None

def execute_callback(uid: int, action: str) -> dict | None:
    user = get_user(uid)
    user["_uid"] = uid
    
    if action == "reset": reset_user(uid); schedule_save(); return {"text": "🔄 Пространство очищено. Опиши новый опыт."}
    if action == "self_inquiry:deep": return handle_deep(uid, user)
    if action == "self_inquiry:pni": return handle_pni(user, uid)
    if action == "self_inquiry:end": return handle_end(uid, user)
    
    # v13.3: кнопка «Ответить и углубиться» — повторяет последний вопрос бота
    if action == "self_inquiry:answer":
        update_user(uid, lambda u: u.__setitem__("state", STATE_AWAIT_ANSWER))
        question = user.get("last_bot_question", "Расскажи подробнее — что ты чувствуешь?")
        return {"text": question, "keyboard": build_continue_keyboard()}
    
    # v13.3: кнопка «Посмотреть под другим углом» — показывает меню линз
    if action == "self_inquiry:lenses":
        return {"text": "Выбери, через какую линзу посмотреть на этот опыт:", "keyboard": build_lenses_keyboard()}
    
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
    if action in ("unified", "user_answer"): send_long_message(chat_id, "…смотрю на это")
    r = execute_message(chat_id, action, text)
    if r: send_long_message(chat_id, r.get("text", ""), r.get("keyboard"))

def process_callback(chat_id: int, data: str) -> None:
    user = get_user(chat_id)
    if not data: return
    if is_rate_limited(user): send_long_message(chat_id, "⏳ Подожди секунду…"); return
    action = route_callback(data)
    if not action: return
    log(f"[CB] uid={chat_id} action={action}")
    if action in ("self_inquiry:deep", "self_inquiry:pni", "self_inquiry:answer", "self_inquiry:lenses") or action.startswith("lens:"):
        send_long_message(chat_id, "…смотрю глубже")
    r = execute_callback(chat_id, action)
    if r: send_long_message(chat_id, r.get("text", ""), r.get("keyboard"))

# ================= WEBHOOK =================
class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/13.3"
    def _send_json(self, code, payload):
        d = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(d)))
        self.end_headers()
        self.wfile.write(d)
    def do_GET(self):
        self._send_json(200, {"ok": True, "service": "shaman-bot", "version": "13.3", "users": len(users)}) if self.path in ("/", "/health") else self._send_json(404, {"error": "Not found"})
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
    log(f"ShamanBot v13.3 TWO-STEP-MENU on {HOST}:{PORT}")
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
```

---

v13.3 — TWO-STEP MENU

Что изменилось:

# Элемент Описание
1 build_entry_keyboard() Две кнопки: «✍️ Ответить и углубиться» и «🔍 Посмотреть под другим углом»
2 build_lenses_keyboard() Полное меню линз — показывается только при нажатии «Посмотреть под другим углом»
3 self_inquiry:answer Повторяет последний вопрос из unified и ждёт ответ пользователя
4 self_inquiry:lenses Показывает меню линз
5 build_continue_keyboard() Кнопка «Посмотреть через линзу» ведёт на self_inquiry:lenses

Поток v13.3:

```
Опыт → Unified (наука + нормализация + вопрос)
         ↓
[✍️ Ответить и углубиться]  [🔍 Посмотреть под другим углом]
         ↓                            ↓
Повтор вопроса из unified      Меню всех линз
Пользователь пишет ответ       [🧠 Нейро] [💭 КПТ] ...
         ↓
Бот продолжает диалог
```

Деплоим.
