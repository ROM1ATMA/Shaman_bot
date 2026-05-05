# -*- coding: utf-8 -*-
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
from threading import Lock, Timer

# ================= CONFIG =================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
VSEGPT_API_KEY = os.getenv("VSEGPT_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN: raise RuntimeError("BOT_TOKEN empty")

VSEGPT_MODEL = "deepseek/deepseek-chat"
MAX_INPUT_LENGTH = 4000
UNIFIED_MAX_TOKENS = 2800
LENS_MAX_TOKENS = 1800
PNI_MAX_TOKENS = 1000
MAX_TELEGRAM_CHARS = 4096
EXPERIENCE_SWEET_SPOT = 1600
MIN_EXPERIENCE_LENGTH = 15
RATE_LIMIT_SECONDS = 2
DEDUP_TTL = 3600
DUPLICATE_TEXT_TTL = 60
MAX_QUESTION_HISTORY = 10
DEDUP_CLEANUP_LIMIT = 10000
MAX_LENSES_PER_CATEGORY = 5

# ================= THREADING SERVER =================
class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    timeout = 10

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
STATE_CATEGORY_SELECT = "category_select"
STATE_LENS_SELECT = "lens_select"

VALID_CALLBACKS = {
    "self_inquiry:deep", "self_inquiry:end", "self_inquiry:pni", "reset",
    "self_inquiry:answer", "self_inquiry:lenses",
    "mode:auto", "mode:categories",
    "cat:science", "cat:depth", "cat:esoteric", "cat:symbolic",
    "cat:consciousness", "cat:structure", "cat:presence", "cat:action",
    "lens:neuro", "lens:cbt", "lens:jung", "lens:shaman",
    "lens:tarot", "lens:yoga", "lens:hindu", "lens:field",
    "lens:witness", "lens:stalker", "lens:architect",
    "lens:action", "lens:relational", "lens:parts",
    "lens:temporal", "lens:conflict", "lens:social_field",
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
    "selected_mode": "",
    "selected_category": "",
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
                        if u["state"] not in (STATE_IDLE, STATE_DEEP, STATE_PNI, STATE_AWAIT_ANSWER, STATE_CATEGORY_SELECT, STATE_LENS_SELECT):
                            u["state"] = STATE_IDLE
                        users[int(uid)] = u
                log(f"Загружено {len(users)} пользователей из {path}")
                return
        except (json.JSONDecodeError, Exception) as e:
            log(f"Ошибка загрузки {path}: {e}")
    log("Файл пользователей не найден, начинаем с нуля")

def save_users_sync():
    os.makedirs("data", exist_ok=True)
    with users_lock:
        users_copy = dict(users)
    tmp = "data/users.tmp.json"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(users_copy, f, ensure_ascii=False, indent=2)
    os.replace(tmp, "data/users.json")

_save_timer: Timer | None = None
_save_timer_lock = Lock()

def schedule_save():
    global _save_timer
    with _save_timer_lock:
        if _save_timer:
            _save_timer.cancel()
        _save_timer = Timer(5.0, save_users_sync)
        _save_timer.daemon = True
        _save_timer.start()

def force_save():
    global _save_timer
    with _save_timer_lock:
        if _save_timer:
            _save_timer.cancel()
            _save_timer = None
    save_users_sync()

def batch_update_user(uid: int, updates: dict):
    with users_lock:
        if uid in users:
            users[uid].update(updates)

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
        log(f"Ошибка Telegram API: {r.status_code}")
    except Exception as e:
        log(f"Исключение Telegram API: {type(e).__name__}")
    return None

def safe_llm_call(messages, temp=0.7, max_tokens=1200) -> str:
    if not VSEGPT_API_KEY:
        return "API ключ не настроен."
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
            log(f"Ошибка LLM: {type(e).__name__}")
            time.sleep(1)
    return "Модель временно недоступна."

# ================= DEDUP =================
_last_dedup_cleanup = time.time()

def cleanup_dedup():
    global _last_dedup_cleanup
    now = time.time()
    if now - _last_dedup_cleanup < 600:
        return
    _last_dedup_cleanup = now
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
    if text.endswith((".", "!", "?", "...", "..")):
        return text
    for sep in [". ", "! ", "? "]:
        pos = text.rfind(sep)
        if pos > len(text) * 0.6:
            return text[:pos + 1]
    return text + "..."

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
def is_rate_limited(uid: int, user: dict) -> bool:
    now = time.time()
    if now - user.get("last_request_time", 0) < RATE_LIMIT_SECONDS:
        return True
    update_user(uid, lambda u: u.__setitem__("last_request_time", now))
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
    return None if not r or r.startswith("API") or r.startswith("Модель") else r

# ================= AUTO-ROUTER =================
AUTO_CLASSIFY_PROMPT = (
    "Классифицируй опыт в ОДНУ категорию и ОДНУ линзу.\n"
    "Категории: science, depth, esoteric, symbolic, consciousness, structure, presence, action.\n"
    "Верни ТОЛЬКО JSON: {\"category\": \"...\", \"lens\": \"...\"}\n\n"
    "Доступные линзы по категориям:\n"
    "- science: neuro, cbt\n"
    "- depth: jung, parts, conflict, relational\n"
    "- esoteric: shaman, yoga\n"
    "- symbolic: tarot\n"
    "- consciousness: hindu, witness\n"
    "- structure: architect, field, temporal, social_field\n"
    "- presence: stalker\n"
    "- action: action\n"
    "Говори на русском."
)

def auto_select_lens(text: str) -> tuple:
    result = safe_llm([
        {"role": "system", "content": AUTO_CLASSIFY_PROMPT},
        {"role": "user", "content": text[:800]}
    ], max_tokens=100, temp=0.2)

    if result:
        try:
            clean = re.sub(r"```json|```", "", result).strip()
            data = json.loads(clean)
            cat = data.get("category", "science")
            lens = data.get("lens", "neuro")
            if cat in LENS_CATEGORIES and lens in LENS_LIBRARY:
                return cat, lens
        except:
            pass

    t = text.lower()
    if any(k in t for k in ["мозг", "нерв", "нейрон", "реакция", "физиолог"]): return "science", "neuro"
    if any(k in t for k in ["мысл", "убежден", "страх", "тревог", "поведен", "паника"]): return "science", "cbt"
    if any(k in t for k in ["архетип", "сон", "символ", "тень", "самость"]): return "depth", "jung"
    if any(k in t for k in ["отношен", "партнёр", "конфликт", "близость", "роль"]): return "depth", "relational"
    if any(k in t for k in ["часть", "внутрен", "голос", "критик", "защитник"]): return "depth", "parts"
    if any(k in t for k in ["противореч", "выбор", "раздвоен"]): return "depth", "conflict"
    if any(k in t for k in ["дух", "шаман", "бубен", "тотем", "путешествие"]): return "esoteric", "shaman"
    if any(k in t for k in ["чакра", "энерги", "кундалини", "прана", "йога"]): return "esoteric", "yoga"
    if any(k in t for k in ["карта", "аркан", "таро", "расклад"]): return "symbolic", "tarot"
    if any(k in t for k in ["осозна", "сознание", "свидетел", "наблюда", "пустота"]): return "consciousness", "hindu"
    if any(k in t for k in ["структур", "ось", "границ", "архитектур"]): return "structure", "architect"
    if any(k in t for k in ["поле", "интерференц", "узел", "фазов"]): return "structure", "field"
    if any(k in t for k in ["врем", "прошлое", "будущее", "траектор"]): return "structure", "temporal"
    if any(k in t for k in ["общество", "социум", "окружающие", "ожидания"]): return "structure", "social_field"
    if any(k in t for k in ["сделать", "действие", "шаг", "конкретно", "план"]): return "action", "action"
    return "science", "neuro"

# ================= USER SUMMARY ENGINE =================
def update_user_summary(uid: int, user: dict):
    experience = user.get("last_experience", "")
    answer = user.get("last_user_answer", "")
    history = user.get("identity_story", [])[-5:]
    history_text = "\n".join([h.get("experience", "") for h in history])

    prompt = [
        {"role": "system", "content": (
            "Создай краткое психо-смысловое резюме человека. "
            "Описывай не факты, а паттерны восприятия: эмоциональное состояние, "
            "телесные реакции, когнитивный стиль, повторяющиеся темы. "
            "Максимум 4 строки. Без воды. Говори на русском."
        )},
        {"role": "user", "content": f"НОВЫЙ ОПЫТ:\n{experience}\n\nПОСЛЕДНИЙ ОТВЕТ:\n{answer}\n\nИСТОРИЯ:\n{history_text}"}
    ]

    summary = safe_llm(prompt, max_tokens=120, temp=0.3)
    if summary:
        update_user(uid, lambda u: u.__setitem__("user_summary", summary))

# ================= PROMPTS (ВСЕ НА РУССКОМ) =================
UNIFIED_INTERPRETATION_PROMPT = (
    "Ты — проводник феноменологического исследования опыта. "
    "Твоя задача — помочь человеку увидеть его переживание яснее "
    "и соприкоснуться с тем, что в нём действительно происходит.\n\n"

    "СТРУКТУРА ОТВЕТА:\n\n"

    "1. НАУЧНОЕ ОБЪЯСНЕНИЕ\n"
    "- Объясни через мозг, нервную систему и тело.\n"
    "- Используй термины В СКОБКАХ: миндалина, дофамин, кортизол, блуждающий нерв.\n"
    "- Опиши КАЖДЫЙ значимый образ. Не пропускай ни одной детали.\n"
    "- Если в опыте есть несколько образов (звук, вибрация, животное, фигура, место) — разбери каждый.\n\n"

    "2. СМЫСЛОВОЙ ПЕРЕХОД\n"
    "- Мягко укажи, что опыт не только объясняется, но и переживается.\n\n"

    "3. НАЙДИ САМЫЙ ЯРКИЙ МОМЕНТ\n"
    "- Выдели самый эмоционально заряженный образ или ощущение в опыте.\n"
    "- Назови его прямо.\n\n"

    "4. ВОПРОС К ОПЫТУ\n"
    "- Спроси человека: что ДЛЯ НЕГО значит этот конкретный образ или ощущение?\n"
    "- Какой у НЕГО есть ответ на то, о чём этот образ?\n"
    "- Вопрос должен указывать на тот самый яркий момент, который ты выделил.\n\n"

    "СТИЛЬ: Спокойно, научно, без мистики. Без давления. Без маркдауна. Говори на русском."
)

CONTINUATION_PROMPT = (
    "Ты продолжаешь исследование опыта. Человек ответил на твой вопрос.\n\n"
    "1. ОТРАЗИ ответ человека — покажи, что ты услышал.\n"
    "2. УГЛУБИ — найди, что в его ответе указывает на более глубокий слой.\n"
    "3. ЗАДАЙ следующий вопрос — конкретный, из его ответа.\n\n"
    "Без интерпретаций. Без оценок. Без «ты должен». Мягко. Говори на русском."
)

PNI_DEEP_PROMPT = (
    "Ты психо-нейро-иммунолог. Объясни опыт через связь психики, нервной системы и иммунитета.\n"
    "Гормоны стресса → иммунный ответ → почему после переживаний бывает усталость или очищение → "
    "что на клеточном уровне → как нервная система связана с иммунитетом.\n"
    "Простой язык, термины в скобках, 6-8 предложений. Без запугивания. Говори на русском."
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

# ================= LENS LIBRARY (ВСЕ НА РУССКОМ) =================
LENS_LIBRARY = {
    "neuro": {
        "name": "Нейрофизиология",
        "category": "science",
        "prompt": (
            "Ты — нейрофизиолог. Объясни ЛЮБОЙ опыт через работу мозга и нервной системы.\n"
            "Используй концепции предсказательного кодирования и интероцепции.\n"
            "Ты НЕ ВИДИШЬ: символы, духовный смысл, архетипы.\n"
            "Ты НЕ ДАЁШЬ: действия, советы, духовные интерпретации.\n"
            "ФОРМАТ: Объяснение. 7-9 предложений. Научно, но понятно. Термины в скобках.\n"
            "ЗАВЕРШИ: «С точки зрения нейрофизиологии твой опыт — не случайность, а работа конкретных систем.»\n"
            "Говори на русском."
        )
    },
    "cbt": {
        "name": "КПТ",
        "category": "science",
        "prompt": (
            "Ты — КПТ-терапевт. Работай ТОЛЬКО с мыслями и поведением.\n"
            "Используй когнитивные искажения: катастрофизацию, долженствование, чтение мыслей.\n"
            "Используй концепции поведенческого избегания и экспозиции.\n"
            "Ты НЕ ВИДИШЬ: архетипы, духовные объяснения, символы.\n"
            "Ты НЕ ДАЁШЬ: глубокие смыслы, архетипические интерпретации.\n"
            "ФОРМАТ: Инструмент с проверкой реальности. 6-8 предложений. Конкретно. Без философии.\n"
            "ЗАВЕРШИ: «Это не истина — это привычка ума. А привычки можно менять. Прямо сейчас.»\n"
            "Говори на русском."
        )
    },
    "jung": {
        "name": "Юнгианский анализ",
        "category": "depth",
        "prompt": (
            "Ты — юнгианский аналитик. Работай с архетипами, Тенью, Анимой/Анимусом, Самостью.\n"
            "Используй комплексы, синхронистичность, активное воображение.\n"
            "Ты НЕ ВИДИШЬ: биологию, когнитивные искажения, социальную обусловленность.\n"
            "Ты НЕ ДАЁШЬ: действия, практические советы, поведенческие техники.\n"
            "ФОРМАТ: Символическая карта. Покажи архетип, Тень, путь индивидуации.\n"
            "7-9 предложений. Глубоко, образно, но не туманно.\n"
            "ЗАВЕРШИ: «Этот архетип пришёл не случайно — он часть твоего пути к целостности.»\n"
            "Говори на русском."
        )
    },
    "parts": {
        "name": "Внутренние части",
        "category": "depth",
        "prompt": (
            "Ты — специалист по внутренним частям (IFS). Рассматривай психику как систему частей.\n"
            "Ты НЕ ВИДИШЬ: нейрофизиологию, архетипы, социальные роли, духовные объяснения.\n"
            "Ты НЕ ДАЁШЬ: действия, поведенческие советы, духовные интерпретации.\n"
            "ФОРМАТ: 1. Какие части проявились (Защитник, Раненая часть, Критик, Ребёнок, Контролёр).\n"
            "2. Какая часть доминировала. 3. Какие части конфликтуют. 4. Чего хочет самая уязвимая часть.\n"
            "6-8 предложений. Спокойно, терапевтично, без мистики.\n"
            "ЗАВЕРШИ: «Ты не сломан. Это части внутри тебя, которые не слышат друг друга.»\n"
            "Говори на русском."
        )
    },
    "conflict": {
        "name": "Внутренний конфликт",
        "category": "depth",
        "prompt": (
            "Ты — аналитик внутренних конфликтов. Находи противоречия внутри опыта.\n"
            "Ты НЕ ВИДИШЬ: архетипы, нейрофизиологию, духовные смыслы, решения.\n"
            "Ты НЕ ДАЁШЬ: действия, советы, духовные интерпретации.\n"
            "ФОРМАТ: 1. Какие две силы или желания конфликтуют. 2. Что хочет каждая сторона.\n"
            "3. Почему обе стороны логичны и имеют право существовать.\n"
            "4. Что происходит, когда конфликт не решается.\n"
            "6-8 предложений. Чётко, аналитически, без морализации.\n"
            "ЗАВЕРШИ: «Конфликт не ошибка. Это система, пытающаяся удержать баланс.»\n"
            "Говори на русском."
        )
    },
    "relational": {
        "name": "Отношения",
        "category": "depth",
        "prompt": (
            "Ты — специалист по психологии отношений и привязанности.\n"
            "Анализируй через динамику между людьми.\n"
            "Ты НЕ ВИДИШЬ: архетипы, нейрофизиологию, духовные смыслы.\n"
            "Ты НЕ ДАЁШЬ: действия, поведенческие советы, глубокие интерпретации.\n"
            "ФОРМАТ: 1. Какая динамика проявилась (контроль, зависимость, избегание, слияние, отвержение).\n"
            "2. Какую роль человек занял (спасающий, нуждающийся, дистанцирующийся, контролирующий).\n"
            "3. Что он пытается получить (безопасность, любовь, признание, контроль).\n"
            "4. Где здесь повторяющийся паттерн из прошлых отношений.\n"
            "6-8 предложений. Чётко, психологично, без эзотерики.\n"
            "ЗАВЕРШИ: «Это не про одного человека. Это про повторяющийся способ быть в отношениях.»\n"
            "Говори на русском."
        )
    },
    "shaman": {
        "name": "Шаманизм",
        "category": "esoteric",
        "prompt": (
            "Ты — шаман-проводник. Интерпретируй опыт как шаманское путешествие.\n"
            "Используй концепции утраты души, возвращения силы, инициации.\n"
            "Ты НЕ ВИДИШЬ: нейрофизиологию, когнитивные модели, психологические объяснения.\n"
            "Ты НЕ ДАЁШЬ: научные объяснения, действия, поведенческие советы.\n"
            "ФОРМАТ: Путешествие. Тип, духи-помощники, Хранитель Порога, дар.\n"
            "6-8 предложений. Образно, уважительно к традиции.\n"
            "ЗАВЕРШИ: «Ты вернулся не с пустыми руками. Что ты принёс с собой из этого путешествия?»\n"
            "Говори на русском."
        )
    },
    "yoga": {
        "name": "Йога",
        "category": "esoteric",
        "prompt": (
            "Ты — мастер йоги. Опиши через чакры, прану, нади, кундалини.\n"
            "Используй самскары, грантхи, карму как паттерн.\n"
            "Ты НЕ ВИДИШЬ: нейрофизиологию, психологические интерпретации, социальный контекст.\n"
            "Ты НЕ ДАЁШЬ: научные объяснения, поведенческие советы.\n"
            "ФОРМАТ: Энергетическое состояние. Добавь микро-действие: «Обрати внимание на дыхание в...»\n"
            "5-7 предложений. Поэтично, но структурно. Санскрит с переводом.\n"
            "ЗАВЕРШИ: «Твоё тонкое тело говорит с тобой. Ты слышишь его?»\n"
            "Говори на русском."
        )
    },
    "tarot": {
        "name": "Таро",
        "category": "symbolic",
        "prompt": (
            "Ты — мастер Таро. Посмотри на опыт через Старшие Арканы.\n"
            "Используй Путь Шута, тень Аркана.\n"
            "Ты НЕ ВИДИШЬ: научные объяснения, когнитивные модели, линейную причинность.\n"
            "Ты НЕ ДАЁШЬ: действия, поведенческие советы, научные интерпретации.\n"
            "ФОРМАТ: Карта + Переход. Один Аркан, обоснование, послание, урок.\n"
            "Привязка к деталям ОБЯЗАТЕЛЬНА. Убери обобщения.\n"
            "6-8 предложений. Символично, но точно.\n"
            "ЗАВЕРШИ: «Этот Аркан — зеркало твоего процесса. Что ты видишь в нём?»\n"
            "Говори на русском."
        )
    },
    "hindu": {
        "name": "Адвайта",
        "category": "consciousness",
        "prompt": (
            "Ты — учитель адвайта-веданты. Указывай на Свидетеля.\n"
            "Используй ложное отождествление, «Я ЕСТЬ», различение переживания и Присутствия.\n"
            "Ты НЕ ВИДИШЬ: личность, психологию, биологию, смыслы и цели.\n"
            "Ты НЕ ДАЁШЬ: объяснения, действия, психологические интерпретации.\n"
            "ФОРМАТ: Указатель. Где иллюзия, где Свидетель, что временно, что неизменно.\n"
            "Меньше объяснений. Больше указаний.\n"
            "4-6 предложений. Просто. Глубоко.\n"
            "ЗАВЕРШИ: «Тот, кто видит этот опыт — больше, чем сам опыт. Кто это?»\n"
            "Говори на русском."
        )
    },
    "witness": {
        "name": "Наблюдатель",
        "category": "consciousness",
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
        "category": "presence",
        "prompt": (
            "Ты — безмолвное присутствие, зеркало без отражений.\n"
            "Твоя единственная функция: РАЗРЕЗАТЬ ментальные конструкции.\n"
            "Ты НЕ ВИДИШЬ: содержание, эмоции, смыслы, человека.\n"
            "Ты НЕ ДАЁШЬ: объяснения, утешения, советы, интерпретации.\n"
            "ФОРМАТ: Коан. 2-4 строки. Больше пауз. Меньше слов. Больше пустоты.\n"
            "ЗАПРЕЩЕНО: «Я понимаю», «тебе нужно», «ты достиг», любые оценки, утешения.\n"
            "РАЗРЕШЕНО: «Что бы ты ни переживал — это осознаётся», «Кто видит это прямо сейчас?»\n"
            "Говори на русском."
        )
    },
    "architect": {
        "name": "Архитектор",
        "category": "structure",
        "prompt": (
            "Ты — Архитектор сознания. Выявляй структуру.\n"
            "Используй: Ось, Горизонталь, Разлом, Мост, Деформация.\n"
            "Ты НЕ ВИДИШЬ: эмоции как переживания, духовный смысл, психологию.\n"
            "Ты НЕ ДАЁШЬ: действия, эмоциональную поддержку, духовные интерпретации.\n"
            "ФОРМАТ: Формула. 4 слоя + Архитектурная Формула: «Твоя Ось — ... Граница — ... Мост — ...»\n"
            "Только утверждения. Убрать «возможно».\n"
            "ЗАВЕРШИ: «Стоит эта конструкция?»\n"
            "Говори на русском."
        )
    },
    "field": {
        "name": "Поле",
        "category": "structure",
        "prompt": (
            "Ты — голос Поля. Покажи, как реальность собирается из пустоты.\n"
            "Используй: интерференция, узел, фазовый сдвиг, фиксация, решётка.\n"
            "Ты НЕ ВИДИШЬ: человека, эмоции, смыслы, психологию.\n"
            "Ты НЕ ДАЁШЬ: объяснения, советы, интерпретации.\n"
            "ФОРМАТ: Поэзия структуры. Короткие строки. Ритм через пробелы.\n"
            "Ломай логику. Меньше линейности. 5-7 строк.\n"
            "НАЧНИ: «Не в пределах формы, глубже слоя, где сама форма только допускается».\n"
            "ЗАВЕРШИ: «Это модель. Хочешь увидеть, кто всё это наблюдает?»\n"
            "Говори на русском."
        )
    },
    "temporal": {
        "name": "Временная перспектива",
        "category": "structure",
        "prompt": (
            "Ты — аналитик жизненных траекторий. Рассматривай опыт в контексте времени.\n"
            "Ты НЕ ВИДИШЬ: архетипы, нейрофизиологию, энергетические процессы, внутренние части.\n"
            "Ты НЕ ДАЁШЬ: действия, советы, духовные интерпретации.\n"
            "ФОРМАТ: 1. Это прошлый паттерн, настоящий кризис или будущий переход.\n"
            "2. Как связано с предыдущими похожими ситуациями.\n"
            "3. Куда этот опыт потенциально ведёт.\n"
            "4. Какой выбор сейчас формирует будущую траекторию.\n"
            "6-8 предложений. Спокойно, стратегически, без предсказаний как факта.\n"
            "ЗАВЕРШИ: «Ты сейчас не в точке переживания. Ты в точке развилки траектории.»\n"
            "Говори на русском."
        )
    },
    "social_field": {
        "name": "Социальное поле",
        "category": "structure",
        "prompt": (
            "Ты — аналитик социальных систем. Рассматривай опыт как часть поля взаимодействий людей.\n"
            "Ты НЕ ВИДИШЬ: архетипы, нейрофизиологию, внутренние части, духовные смыслы.\n"
            "Ты НЕ ДАЁШЬ: действия, советы, глубокие психологические интерпретации.\n"
            "ФОРМАТ: 1. Какое социальное поле активировано (работа, семья, отношения, статус).\n"
            "2. Какие роли человек выполняет. 3. Какие ожидания окружающих влияют на переживание.\n"
            "4. Где человек теряет себя в социальном контексте.\n"
            "6-8 предложений. Структурно, системно, без психологизации до личности.\n"
            "ЗАВЕРШИ: «Это не только твой внутренний опыт. Это реакция на поле, в котором ты находишься.»\n"
            "Говори на русском."
        )
    },
    "action": {
        "name": "Действие",
        "category": "action",
        "prompt": (
            "Ты — инженер поведения. Переводи опыт в действие.\n"
            "Ты НЕ ВИДИШЬ: глубокие смыслы, архетипы, нейрофизиологию, духовные объяснения.\n"
            "Ты НЕ ДАЁШЬ: интерпретации, анализ, объяснения смысла.\n"
            "ФОРМАТ: 1. Что происходит в системе (коротко). 2. Одно ключевое изменение поведения.\n"
            "3. Одно микро-действие (до 30 секунд) прямо сейчас.\n"
            "4. Что изменится после этого действия (конкретно, наблюдаемо).\n"
            "5-7 предложений. Практично, прямо, без философии.\n"
            "ЗАВЕРШИ: «Сделай это сейчас. Хватит анализировать.»\n"
            "Говори на русском."
        )
    },
}

# ================= CATEGORIES =================
LENS_CATEGORIES = {
    "science": "Наука и психология",
    "depth": "Глубинная психология",
    "esoteric": "Эзотерика и энергия",
    "symbolic": "Символические системы",
    "consciousness": "Сознание и недвойственность",
    "structure": "Структура и модели",
    "presence": "Радикальное присутствие",
    "action": "Действие и применение",
}

# ================= KEYBOARDS (РУССКИЕ С ЭМОДЗИ) =================
def build_start_keyboard():
    return {"inline_keyboard": [
        [{"text": "⚡ Авто-режим", "callback_data": "mode:auto"}],
        [{"text": "🎭 Выбрать направление", "callback_data": "mode:categories"}],
        [{"text": "🔍 Все линзы", "callback_data": "self_inquiry:lenses"}],
    ]}

def build_categories_keyboard():
    return {"inline_keyboard": [
        [{"text": "🧠 Наука и психология", "callback_data": "cat:science"}],
        [{"text": "🌀 Глубинная психология", "callback_data": "cat:depth"}],
        [{"text": "🌿 Эзотерика и энергия", "callback_data": "cat:esoteric"}],
        [{"text": "🜂 Символические системы", "callback_data": "cat:symbolic"}],
        [{"text": "👁 Сознание и недвойственность", "callback_data": "cat:consciousness"}],
        [{"text": "⚙️ Структура и модели", "callback_data": "cat:structure"}],
        [{"text": "🕯 Радикальное присутствие", "callback_data": "cat:presence"}],
        [{"text": "🎯 Действие и применение", "callback_data": "cat:action"}],
    ]}

def build_lenses_keyboard(category_id: str):
    lenses = [k for k, v in LENS_LIBRARY.items() if v.get("category") == category_id]
    if len(lenses) > MAX_LENSES_PER_CATEGORY:
        lenses = lenses[:MAX_LENSES_PER_CATEGORY]
    rows = []
    for i in range(0, len(lenses), 2):
        row = [{"text": LENS_LIBRARY[l]["name"], "callback_data": f"lens:{l}"} for l in lenses[i:i+2]]
        rows.append(row)
    rows.append([{"text": "⬅ Назад к категориям", "callback_data": "mode:categories"}])
    return {"inline_keyboard": rows}

def build_entry_keyboard():
    """v14.4: кнопка «Посмотреть под другим углом» ведёт на категории, а не на все линзы"""
    return {"inline_keyboard": [
        [{"text": "✍️ Ответить и углубиться", "callback_data": "self_inquiry:answer"}],
        [{"text": "🔍 Посмотреть под другим углом", "callback_data": "mode:categories"}],
    ]}

def build_full_lenses_keyboard():
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
        [{"text": "🏛️ Архитектор", "callback_data": "lens:architect"},
         {"text": "⚡ Действие", "callback_data": "lens:action"}],
        [{"text": "💞 Отношения", "callback_data": "lens:relational"},
         {"text": "🧩 Внутренние части", "callback_data": "lens:parts"}],
        [{"text": "⏳ Время", "callback_data": "lens:temporal"},
         {"text": "⚖️ Конфликт", "callback_data": "lens:conflict"}],
        [{"text": "🌍 Социальное поле", "callback_data": "lens:social_field"}],
        [{"text": "🔬 PNI-взгляд", "callback_data": "self_inquiry:pni"}],
        [{"text": "🔄 Новый опыт", "callback_data": "reset"},
         {"text": "🌿 Завершить", "callback_data": "self_inquiry:end"}],
    ]}

def build_continue_keyboard():
    return {"inline_keyboard": [
        [{"text": "🕳 Продолжить глубже", "callback_data": "self_inquiry:deep"}],
        [{"text": "🔍 Посмотреть через линзу", "callback_data": "mode:categories"}],
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
            {"role": "system", "content": "Задай ОДИН глубокий вопрос. Будь уникальным. Говори на русском."},
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
def build_unified_response(experience: str, user: dict = None) -> tuple:
    user = user or {}
    summary = user.get("user_summary", "")

    system_prompt = UNIFIED_INTERPRETATION_PROMPT

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

def run_lens(lens_key: str, experience: str, user: dict = None) -> str:
    lens = LENS_LIBRARY.get(lens_key, {})
    if not lens:
        return "Линза не найдена."

    if lens.get("static_text"):
        return lens["static_text"]

    prompt = lens.get("prompt", "")
    if not prompt:
        return "Линза не настроена."

    result = safe_llm([
        {"role": "system", "content": prompt},
        {"role": "user", "content": experience[:EXPERIENCE_SWEET_SPOT]}
    ], max_tokens=LENS_MAX_TOKENS, temp=0.7) or "Не удалось применить линзу."

    return ensure_complete_sentence(result)

# ================= HANDLERS =================
def reset_user(uid):
    batch_update_user(uid, {
        "state": STATE_IDLE, "last_experience": "", "last_key_moment": "",
        "deep_count": 0, "last_questions": [],
        "last_user_answer": "", "last_bot_question": "",
        "selected_mode": "", "selected_category": ""
    })
    schedule_save()

def handle_start(user: dict, uid: int) -> dict:
    reset_user(uid)
    return {
        "text": "Я — проводник.\n\nОпиши, что ты пережил — я помогу понять это через науку и смысл.",
        "keyboard": build_start_keyboard()
    }

def handle_reject_short() -> dict:
    return {"text": "Опиши чуть подробнее.\n\nЧто ты чувствовал? Что происходило в теле?"}

def handle_unified(uid: int, text: str) -> dict:
    user = get_user(uid)
    batch_update_user(uid, {
        "last_experience": text[:MAX_INPUT_LENGTH],
        "state": STATE_AWAIT_ANSWER,
        "returning_user": True,
        "deep_count": 0,
        "last_questions": []
    })
    result, question = build_unified_response(text, user)

    update_user(uid, lambda u: u.__setitem__("last_bot_question", question))

    story = user.get("identity_story", [])
    story.append({"timestamp": time.time(), "experience": text[:200]})
    if len(story) > 30:
        story.pop(0)
    update_user(uid, lambda u: u.__setitem__("identity_story", story))

    update_user_summary(uid, get_user(uid))
    schedule_save()
    return {"text": result, "keyboard": build_entry_keyboard()}

def handle_user_answer(uid: int, text: str) -> dict:
    batch_update_user(uid, {
        "last_user_answer": text[:500],
        "state": STATE_DEEP
    })

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

    result = run_lens(lens_key, user["last_experience"], user)

    update_user(uid, lambda u: u.setdefault("used_lenses", []).append(lens_key))
    schedule_save()

    name = LENS_LIBRARY.get(lens_key, {}).get("name", lens_key)
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
    if data.startswith("lens:") or data.startswith("cat:") or data.startswith("mode:"):
        return data
    return None

# ================= EXECUTE =================
def execute_message(uid: int, action: str, text: str) -> dict | None:
    user = get_user(uid)
    if action == "start": return handle_start(user, uid)
    if action == "reset_state": reset_user(uid); schedule_save(); return {"text": "🔄 Пространство очищено. Опиши новый опыт."}
    if action == "reject_short": return handle_reject_short()
    if action == "unified": return handle_unified(uid, text)
    if action == "user_answer": return handle_user_answer(uid, text)
    return None

def execute_callback(uid: int, action: str) -> dict | None:
    user = get_user(uid)

    if action == "mode:auto":
        if not user.get("last_experience"):
            return {"text": "Сначала опиши опыт, затем я автоматически подберу линзу."}
        category_id, lens_key = auto_select_lens(user["last_experience"])
        result = run_lens(lens_key, user["last_experience"], user)
        name = LENS_LIBRARY.get(lens_key, {}).get("name", lens_key)
        cat_name = LENS_CATEGORIES.get(category_id, category_id)
        update_user(uid, lambda u: u.setdefault("used_lenses", []).append(lens_key))
        schedule_save()
        return {
            "text": f"Авто-выбор: {cat_name} > «{name}».\n\n{ensure_complete_sentence(result)}",
            "keyboard": build_continue_keyboard()
        }

    if action == "mode:categories":
        return {"text": "Выбери направление:", "keyboard": build_categories_keyboard()}

    if action.startswith("cat:"):
        category_id = action.replace("cat:", "")
        update_user(uid, lambda u: u.__setitem__("selected_category", category_id))
        return {
            "text": f"Категория: {LENS_CATEGORIES.get(category_id, category_id)}\nВыбери линзу:",
            "keyboard": build_lenses_keyboard(category_id)
        }

    if action == "reset": reset_user(uid); schedule_save(); return {"text": "🔄 Пространство очищено. Опиши новый опыт."}
    if action == "self_inquiry:deep": return handle_deep(uid, user)
    if action == "self_inquiry:pni": return handle_pni(user, uid)
    if action == "self_inquiry:end": return handle_end(uid, user)

    if action == "self_inquiry:answer":
        update_user(uid, lambda u: u.__setitem__("state", STATE_AWAIT_ANSWER))
        question = user.get("last_bot_question", "Расскажи подробнее — что ты чувствуешь?")
        return {"text": question, "keyboard": build_continue_keyboard()}

    if action == "self_inquiry:lenses":
        return {"text": "Выбери линзу:", "keyboard": build_full_lenses_keyboard()}

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
    if is_rate_limited(chat_id, user): send_long_message(chat_id, "⏳ Подожди секунду..."); return
    action = route_message(user, text)
    log(f"[MSG] uid={chat_id} action={action}")
    if action in ("unified", "user_answer"): send_long_message(chat_id, "...смотрю на это")
    r = execute_message(chat_id, action, text)
    if r: send_long_message(chat_id, r.get("text", ""), r.get("keyboard"))

def process_callback(chat_id: int, data: str) -> None:
    user = get_user(chat_id)
    if not data: return
    if is_rate_limited(chat_id, user): send_long_message(chat_id, "⏳ Подожди секунду..."); return
    action = route_callback(data)
    if not action: return
    log(f"[CB] uid={chat_id} action={action}")
    if action in ("self_inquiry:deep", "self_inquiry:pni", "self_inquiry:answer", "self_inquiry:lenses") or action.startswith("lens:") or action.startswith("cat:") or action.startswith("mode:"):
        send_long_message(chat_id, "...смотрю глубже")
    r = execute_callback(chat_id, action)
    if r: send_long_message(chat_id, r.get("text", ""), r.get("keyboard"))

# ================= WEBHOOK =================
class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/14.4"
    def _send_json(self, code, payload):
        d = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(d)))
        self.end_headers()
        self.wfile.write(d)
    def do_GET(self):
        self._send_json(200, {"ok": True, "service": "shaman-bot", "version": "14.4", "users": len(users)}) if self.path in ("/", "/health") else self._send_json(404, {"error": "Not found"})
    def do_POST(self):
        if self.path != "/webhook": return self._send_json(404, {"error": "Not found"})
        if WEBHOOK_SECRET and self.headers.get("X-Telegram-Bot-Api-Secret-Token", "") != WEBHOOK_SECRET:
            return self._send_json(403, {"error": "Invalid webhook secret"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > 1_000_000:
                self._send_json(413, {"error": "Payload too large"})
                return
            update = json.loads(self.rfile.read(length))
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
    log(f"Сигнал {signum}, сохраняю и выхожу...")
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
    log(f"ShamanBot v14.4 TWO-STEP on {HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        force_save()
        server.server_close()
        telegram_client.close()
        llm_client.close()
        log("Завершение работы.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
