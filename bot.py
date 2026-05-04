import json
import os
import sys
import time
import re
import random
import html
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
ADMIN_ID = 781629557
USER_TTL = 3600
MAX_INPUT_LENGTH = 4000

UNIFIED_MAX_TOKENS = 1000
MAX_TELEGRAM_CHARS = 4096
EXPERIENCE_SWEET_SPOT = 1200
MIN_EXPERIENCE_LENGTH = 15

RATE_LIMIT_SECONDS = 2
DEDUP_TTL = 3600
DUPLICATE_TEXT_TTL = 60         # 🔥 60 секунд — разрешаем повторный анализ
MAX_QUESTION_HISTORY = 10

# ================= THREADING SERVER =================
class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

save_lock = Lock()
# 🔥 http_lock УБРАН — httpx.Client потокобезопасен сам
# 🔥 client_lock только для закрытия при shutdown
client_close_lock = Lock()

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
    "self_inquiry:deep",
    "self_inquiry:end",
    "self_inquiry:pni",
    "reset",
}

users = {}
processed_updates: dict[int, float] = {}
dedup_lock = Lock()

# ================= USERS =================
def load_users():
    global users
    loaded = False
    for path in ["data/users.json", "data/users.tmp.json"]:
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    loaded_data = json.load(f)
                    for uid, data in loaded_data.items():
                        users[int(uid)] = data
                log(f"Loaded {len(users)} users from {path}")
                loaded = True
                break
        except (json.JSONDecodeError, Exception) as e:
            log(f"Failed to load {path}: {e}")
    
    if not loaded:
        log("No users file found, starting fresh")

def save_users_sync():
    """Атомарное сохранение с копией словаря — защита от гонок итерации"""
    with save_lock:
        os.makedirs("data", exist_ok=True)
        # 🔥 Копируем словарь под локом — никакой поток не изменит его во время итерации
        users_copy = dict(users)
        tmp = "data/users.tmp.json"
        final = "data/users.json"
        
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(users_copy, f, ensure_ascii=False, indent=2)
        
        os.replace(tmp, final)

_save_pending = False
_save_flag_lock = Lock()
_save_debounce = 5.0

def schedule_save():
    global _save_pending
    with _save_flag_lock:
        if _save_pending:
            return
        _save_pending = True
    
    def do_save():
        global _save_pending
        time.sleep(_save_debounce)
        save_users_sync()
        with _save_flag_lock:
            _save_pending = False
    
    t = __import__('threading').Thread(target=do_save, daemon=True)
    t.start()

def force_save():
    """Немедленное сохранение — вызывается при shutdown"""
    global _save_pending
    save_users_sync()
    with _save_flag_lock:
        _save_pending = False

def get_user(uid: int) -> dict:
    uid = int(uid)
    if uid not in users:
        users[uid] = {
            "state": STATE_IDLE,
            "last_experience": "",
            "last_active": time.time(),
            "last_key_moment": "",
            "returning_user": False,
            "identity_story": [],
            "deep_count": 0,
            "last_request_time": 0,
            "last_update_hash": "",
            "last_update_time": 0,        # 🔥 TTL для duplicate check
            "last_questions": [],
        }
        schedule_save()
    users[uid]["last_active"] = time.time()
    return users[uid]

# ================= HTTP CLIENTS (thread-safe без lock) =================
telegram_client = httpx.Client(
    timeout=30,
    limits=httpx.Limits(max_connections=200, max_keepalive_connections=50)
)
llm_client = httpx.Client(
    timeout=65,
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
)

def safe_telegram_api(method: str, payload: dict) -> dict | None:
    """Потокобезопасно — httpx.Client сам управляет connection pool"""
    if not BOT_TOKEN:
        return None
    try:
        r = telegram_client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
            json=payload
        )
        if r.status_code == 200:
            return r.json()
        log(f"Telegram API error: {r.status_code} {r.text[:200]}")
    except Exception as e:
        log(f"Telegram API exception: {e}")
    return None

def safe_llm_call(messages, temp=0.7, max_tokens=1200, user=None) -> str:
    """Потокобезопасно — httpx.Client сам управляет connection pool"""
    if not VSEGPT_API_KEY:
        return "⚠️ API ключ не настроен."
    
    for attempt in range(2):
        try:
            r = llm_client.post(
                "https://api.vsegpt.ru:6070/v1/chat/completions",
                json={
                    "model": VSEGPT_MODEL,
                    "messages": messages,
                    "temperature": temp,
                    "max_tokens": max_tokens
                },
                headers={"Authorization": f"Bearer {VSEGPT_API_KEY}"},
                timeout=65
            )
            if r.status_code == 200:
                data = r.json()
                if data.get("choices"):
                    return data["choices"][0]["message"]["content"]
            else:
                log(f"LLM HTTP {r.status_code}: {r.text[:200]}")
        except httpx.TimeoutException:
            log(f"LLM timeout (attempt {attempt+1})")
        except Exception as e:
            log(f"LLM error: {type(e).__name__}: {e}")
        
        if attempt < 1:
            time.sleep(1)
    
    return "⚠️ Модель временно недоступна."

# ================= DEDUP =================
def is_duplicate_update(update_id: int) -> bool:
    """Дедупликация webhook'ов по update_id"""
    now = time.time()
    with dedup_lock:
        expired = [uid for uid, ts in processed_updates.items() if now - ts > DEDUP_TTL]
        for uid in expired:
            processed_updates.pop(uid, None)
        
        if update_id in processed_updates:
            return True
        processed_updates[update_id] = now
        return False

# ================= TEXT UTILITIES =================
def ensure_complete_sentence(text: str) -> str:
    if not text:
        return text
    text = text.strip()
    
    if text.endswith((".", "!", "?", "…", "»", "”", ")", "]", "}")):
        return text
    
    for sep in [". ", "! ", "? ", ".\n", "!\n", "?\n"]:
        pos = text.rfind(sep)
        if pos > len(text) * 0.6:
            return text[:pos + 1]
    
    return text + "…"

# ================= TELEGRAM API =================
def send_long_message(chat_id: int, text: str, keyboard: dict = None) -> bool:
    """Отправка длинных сообщений с разбиением"""
    if not text:
        return False
    
    text = ensure_complete_sentence(text)
    escaped = html.escape(text)
    
    chunks = []
    remaining = escaped
    
    while remaining:
        if len(remaining) <= MAX_TELEGRAM_CHARS:
            chunks.append(remaining)
            break
        
        part = remaining[:MAX_TELEGRAM_CHARS]
        cut = part.rfind(" ")
        if cut < MAX_TELEGRAM_CHARS * 0.5:
            cut = MAX_TELEGRAM_CHARS
        
        chunks.append(remaining[:cut])
        remaining = remaining[cut:]
    
    for chunk in chunks[:-1]:
        success = False
        for _ in range(2):
            result = safe_telegram_api("sendMessage", {"chat_id": chat_id, "text": chunk})
            if result and result.get("ok"):
                success = True
                break
            time.sleep(0.5)
        if not success:
            return False
        time.sleep(0.25)
    
    payload = {"chat_id": chat_id, "text": chunks[-1]}
    if keyboard:
        payload["reply_markup"] = keyboard
    
    for _ in range(2):
        result = safe_telegram_api("sendMessage", payload)
        if result and result.get("ok"):
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
    user["last_request_time"] = now
    return False

def is_duplicate(uid: int, user: dict, text: str) -> bool:
    """Проверка дубликата текста с TTL — разрешает повторный анализ через 60 секунд"""
    now = time.time()
    h = hashlib.sha256(f"{uid}:{text.strip()}".encode()).hexdigest()
    
    last_hash = user.get("last_update_hash", "")
    last_time = user.get("last_update_time", 0)
    
    if last_hash == h and now - last_time < DUPLICATE_TEXT_TTL:
        return True
    
    user["last_update_hash"] = h
    user["last_update_time"] = now
    return False

# ================= LLM =================
def safe_llm(messages, user=None, **kwargs) -> str | None:
    try:
        result = safe_llm_call(messages, user=user, **kwargs)
        if not result:
            return None
        if result.startswith("⚠️"):
            return None
        return result
    except Exception as e:
        log(f"LLM CRASH: {type(e).__name__}: {e}")
        return None

# ================= PROMPTS =================
UNIFIED_INTERPRETATION_PROMPT = (
    "Ты объясняешь человеку его опыт так, чтобы он почувствовал: "
    "«это про меня, и это имеет смысл».\n\n"

    "Сначала мягко объясни, что происходило через тело и мозг. "
    "Добавляй научные термины В СКОБКАХ "
    "(например: миндалина, дофамин, нервная система, кортизол, цитокины, иммунный ответ), "
    "но пиши простым языком.\n\n"

    "Если в опыте есть признаки стресса, усталости, телесных реакций или восстановления — "
    "коснись связи между переживанием и иммунной системой "
    "(психо-нейро-иммунология): как эмоции влияют на защитные силы организма, "
    "почему после сильных процессов может быть физическая усталость или очищение, "
    "как гормоны стресса (кортизол) взаимодействуют с иммунными клетками (цитокины).\n\n"

    "Затем естественно перейди к тому, что за этим может стоять на уровне психики. "
    "Используй формулировки: «похоже на», «может указывать», «часто в таких состояниях». "
    "Можно касаться образов, символов, внутренних сюжетов.\n\n"

    "Важно: не противопоставляй науку и смысл. "
    "Покажи, что это один процесс, просто с разных сторон.\n\n"

    "В конце отрази человеку его состояние через «ты», "
    "как будто ты видишь, что он проживает внутри этого опыта.\n\n"

    "Заверши одним мягким вопросом, который ведёт внутрь. "
    "Без давления. Без «почему».\n\n"

    "Стиль:\n"
    "- живой, человеческий\n"
    "- без пафоса\n"
    "- без морали\n"
    "- без советов\n"
    "- без структуры и заголовков\n"
    "- без маркдауна\n"
    "- без нумерации"
)

PNI_DEEP_PROMPT = (
    "Ты психо-нейро-иммунолог. Объясни этот опыт через связь психики, "
    "нервной системы и иммунитета.\n\n"
    
    "Покажи:\n"
    "1. Какие гормоны стресса (кортизол, адреналин, норадреналин) могли включиться\n"
    "2. Как это повлияло на иммунный ответ (цитокины, воспалительные процессы)\n"
    "3. Почему после таких переживаний тело может чувствовать усталость или очищение\n"
    "4. Что происходит на клеточном уровне когда психика проходит трансформацию\n"
    "5. Как нервная система (симпатическая/парасимпатическая) связана с иммунитетом\n\n"
    
    "Пиши простым языком. Термины в скобках. 6-8 предложений. "
    "Без запугивания — это естественный процесс. "
    "Свяжи с конкретными ощущениями из опыта человека."
)

SCIENCE_HOOKS = [
    "Интересно, что в этом опыте есть вполне конкретное объяснение. ",
    "То, что ты описываешь, имеет довольно точную физиологическую основу. ",
    "Это выглядит как спонтанный процесс, но у него есть понятная механика. ",
    "С точки зрения нейрофизиологии, здесь происходит кое-что очень конкретное. ",
]

PNI_HOOKS = [
    "С точки зрения психо-нейро-иммунологии, в этом опыте есть интересная связь: ",
    "То, что ты переживаешь — это процесс на уровне тела и иммунитета: ",
    "Наука о связи психики и иммунитета объясняет это так: ",
]

IDENTITY_SOFT_HOOKS = [
    "\n\nИ в этом месте ты как будто выбираешь, как с этим быть дальше.",
    "\n\nИ здесь уже появляется не только понимание, но и выбор — как ты с этим обходишься.",
    "\n\nИ теперь, когда это становится понятнее — ты можешь решить, что с этим делать.",
]

DEEP_PATTERNS = [
    "Что в этом опыте всё ещё остаётся незавершённым?",
    "Где это ощущается в тебе прямо сейчас?",
    "Как бы выглядело завершение этого для тебя?",
    "Что в этом ты не позволил себе до конца?",
    "Если бы это можно было выразить одним словом — что это было бы?",
    "Что изменилось бы, если бы этот процесс завершился так, как ты хотел?",
    "Какую часть себя ты узнаёшь в этом опыте?",
]

# ================= KEYBOARDS =================
def build_entry_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🔍 Хочу понять глубже", "callback_data": "self_inquiry:deep"}],
    ]}

def build_deep_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🕳 Ещё глубже", "callback_data": "self_inquiry:deep"}],
        [{"text": "🔬 Взгляд психо-нейро-иммунолога", "callback_data": "self_inquiry:pni"}],
        [{"text": "🔄 Новый опыт", "callback_data": "reset"}],
        [{"text": "🌿 Завершить", "callback_data": "self_inquiry:end"}],
    ]}

# ================= QUESTION ANTI-REPEAT ENGINE =================
STOP_WORDS = [
    "сейчас", "прямо", "для тебя", "в этом", "это", "ты", "тебя",
    "тебе", "твой", "твоё", "твоя", "твои", "как", "что", "где",
    "когда", "почему", "зачем", "ли", "бы", "же", "то", "вот"
]

def normalize_question(text: str) -> str:
    if not text:
        return ""
    
    t = text.lower()
    t = re.sub(r"[^\w\s]", "", t)
    
    words = [w for w in t.split() if w not in STOP_WORDS]
    return " ".join(words[:6])

def is_repeated_question(user: dict, question: str) -> bool:
    norm = normalize_question(question)
    if not norm or len(norm) < 2:
        return True
    
    for q in user.get("last_questions", []):
        if norm == q:
            return True
    return False

def remember_question(user: dict, question: str):
    norm = normalize_question(question)
    if not norm:
        return
    
    questions = user.setdefault("last_questions", [])
    questions.append(norm)
    
    if len(questions) > MAX_QUESTION_HISTORY:
        questions.pop(0)

def generate_unique_question(user: dict, pool: list) -> str:
    """
    Три уровня: пул → LLM (2 попытки) → fallback.
    """
    # 1. Пул (shuffled)
    shuffled_pool = pool.copy()
    random.shuffle(shuffled_pool)
    
    for q in shuffled_pool:
        if not is_repeated_question(user, q):
            remember_question(user, q)
            return q
    
    # 2. LLM (2 попытки, повышенная температура)
    for attempt in range(2):
        result = safe_llm([
            {"role": "system", "content": (
                "Задай ОДИН глубокий вопрос для самоисследования. "
                "Не повторяй формулировки вроде: "
                "«что ты чувствуешь», «что осталось незавершённым», «где это ощущается». "
                "Будь конкретным, уникальным и неожиданным. "
                "Используй другие слова, другой ракурс."
            )},
            {"role": "user", "content": user["last_experience"][:500]}
        ], max_tokens=100, temp=0.9 if attempt == 0 else 1.0, user=user)
        
        if result and not is_repeated_question(user, result):
            remember_question(user, result)
            return result
    
    # 3. Fallback — случайный из пула (лучше повториться, чем молчать)
    fallback = random.choice(pool)
    # 🔥 Не вызываем remember_question — вопрос уже в истории
    return fallback

# ================= UNIFIED ENGINE =================
def extract_key_moment(text: str, user: dict = None) -> str:
    result = safe_llm([
        {"role": "system", "content": "Найди в опыте момент внутреннего напряжения или незавершённости. Верни ОДНУ фразу (до 12 слов). Только фраза."},
        {"role": "user", "content": text[:800]}
    ], max_tokens=40, temp=0.3, user=user)
    
    if result and len(result) > 10:
        moment = result.strip().lower()
        moment = moment.replace('"', '').replace("«", "").replace("»", "")
        moment = moment.strip(" .,-\n")
        if len(moment.split()) > 14:
            moment = " ".join(moment.split()[:14])
        return moment
    
    t = text.replace("\n", " ")
    sentences = re.split(r"[.!?…]", t)
    tension_words = ["не произошло", "не случилось", "не дошло", "не хватило", "хотел", "ждал"]
    
    for s in sentences:
        if any(w in s.lower() for w in tension_words):
            return s.strip()[:120]
    
    return text[:120]

def has_pni_markers(text: str) -> bool:
    negations = ["не устал", "без стресса", "нет напряжения", "не болел", "не было стресса"]
    if any(neg in text.lower() for neg in negations):
        return False
    
    pni_keywords = [
        "устал", "усталость", "болел", "болезнь", "выздоровел",
        "стресс", "напряжён", "истощён", "очищен", "восстановлен",
        "слабость", "подъём", "иммун", "гормон", "температур",
        "физическ", "телесн", "давлен", "спазм", "воспален",
        "простыл", "вирус", "кашель", "насморк"
    ]
    return any(w in text.lower() for w in pni_keywords)

def build_unified_response(experience: str, user: dict) -> str:
    moment = extract_key_moment(experience, user)
    user["last_key_moment"] = moment
    
    result = safe_llm([
        {"role": "system", "content": UNIFIED_INTERPRETATION_PROMPT},
        {"role": "user", "content": experience[:EXPERIENCE_SWEET_SPOT]}
    ], max_tokens=UNIFIED_MAX_TOKENS, temp=0.6, user=user)

    if not result:
        result = (
            "Похоже, нервная система (регуляция напряжения) не получила полного цикла разрядки — "
            "и это создало ощущение незавершённости.\n\n"
            "Но в этом есть не только физиология. "
            "Похоже на внутренний сюжет, где что-то важное искало выход — "
            "и, возможно, продолжает искать.\n\n"
            "Ты проживаешь это по-своему, и только ты знаешь, что откликается.\n\n"
            "Что в этом для тебя всё ещё остаётся незавершённым?"
        )
    
    result = ensure_complete_sentence(result)
    
    if has_pni_markers(experience) and random.random() < 0.5:
        prefix = random.choice(PNI_HOOKS)
    elif "(" not in result:
        prefix = random.choice(SCIENCE_HOOKS)
    elif random.random() < 0.4:
        prefix = random.choice(SCIENCE_HOOKS)
    else:
        prefix = None
    
    if prefix:
        result = prefix + result
    
    if random.random() < 0.6:
        hook = random.choice(IDENTITY_SOFT_HOOKS)
        if result.rstrip().endswith("?"):
            result = result.rstrip() + hook
        else:
            result = result.rstrip().rstrip(".") + "." + hook
    
    return result

# ================= PNI HANDLER =================
def handle_pni(user: dict) -> dict:
    if not user.get("last_experience"):
        return {"text": "Сначала опиши опыт, чтобы я мог разобрать его."}
    
    user["state"] = STATE_PNI
    
    result = safe_llm([
        {"role": "system", "content": PNI_DEEP_PROMPT},
        {"role": "user", "content": user["last_experience"][:1000]}
    ], max_tokens=600, temp=0.5, user=user)

    if not result:
        result = (
            "Когда ты переживаешь сильный процесс, твоя нервная система (симпатическая) "
            "активирует выброс кортизола и адреналина. "
            "Это влияет на иммунные клетки (цитокины) — может временно снижаться защита, "
            "но после разрешения наступает фаза восстановления.\n\n"
            "То, что ты чувствуешь усталость или очищение — это не «сбой», "
            "а естественный цикл: стресс → адаптация → обновление."
        )
    
    result = ensure_complete_sentence(result)

    return {
        "text": (
            f"🔬 ВЗГЛЯД ПСИХО-НЕЙРО-ИММУНОЛОГА\n\n"
            f"{result}\n\n"
            f"────────────────────\n\n"
            f"Это взгляд через призму связи тела, мозга и иммунитета."
        ),
        "keyboard": build_deep_keyboard()
    }

# ================= ROUTING =================
def route_message(user: dict, text: str) -> str:
    state = user["state"]

    if text in ("/start", "/new"):
        return "start"
    if text in ("/reset", "reset"):
        return "reset_state"

    if state == STATE_IDLE and len(text.strip()) < MIN_EXPERIENCE_LENGTH:
        return "reject_short"

    return "unified"

def route_callback(data: str) -> str | None:
    if data in VALID_CALLBACKS:
        return data
    log(f"Invalid callback data: {data[:50]}")
    return None

# ================= HANDLERS =================
def handle_start(user: dict) -> dict:
    user.update({
        "state": STATE_IDLE, "last_experience": "", "last_key_moment": "",
        "deep_count": 0, "last_questions": []
    })
    schedule_save()
    
    if user.get("returning_user"):
        return {"text": "🌿 С возвращением.\n\nОпиши новый опыт."}
    
    return {
        "text": (
            "🌿 Я — проводник осознания.\n\n"
            "Опиши, что ты пережил.\n"
            "Я помогу тебе понять это — через тело, мозг, иммунитет и deeper смысл."
        )
    }

def handle_reject_short() -> dict:
    return {
        "text": "Опиши чуть подробнее, чтобы можно было увидеть структуру опыта.\n\nЧто ты чувствовал? Что происходило в теле?"
    }

def handle_unified(user: dict, text: str) -> dict:
    with save_lock:
        user["last_experience"] = text[:MAX_INPUT_LENGTH]
        user["state"] = STATE_DEEP
        user["returning_user"] = True
        user["deep_count"] = 0
        user["last_questions"] = []

    result = build_unified_response(text, user)

    user["identity_story"].append({
        "timestamp": time.time(),
        "experience": text[:200],
        "moment": user.get("last_key_moment", "")
    })
    if len(user["identity_story"]) > 30:
        user["identity_story"].pop(0)

    schedule_save()

    return {
        "text": result,
        "keyboard": build_entry_keyboard()
    }

def handle_deep(user: dict) -> dict:
    with save_lock:
        user["state"] = STATE_DEEP
        user["deep_count"] = user.get("deep_count", 0) + 1
    
    depth = user["deep_count"]
    
    if depth <= 2:
        pool = DEEP_PATTERNS[:3]
    elif depth <= 4:
        pool = DEEP_PATTERNS[2:5]
    else:
        pool = DEEP_PATTERNS[3:]
    
    result = generate_unique_question(user, pool)

    return {
        "text": result,
        "keyboard": build_deep_keyboard()
    }

def handle_end(user: dict) -> dict:
    identity_count = len(user.get("identity_story", []))
    deep_count = user.get("deep_count", 0)
    
    with save_lock:
        user["state"] = STATE_IDLE
        user["deep_count"] = 0
        user["last_questions"] = []
    
    schedule_save()
    
    return {
        "text": (
            f"🌿 Цикл завершён.\n\n"
            f"Ты прошёл путь от опыта — к пониманию — к глубине.\n"
            f"Ты углублялся {deep_count} раз(а) в этот опыт.\n"
            f"За всё время ты исследовал {identity_count} переживаний.\n\n"
            f"Ты можешь начать с новым опытом или побыть с тем, что сейчас."
        )
    }

# ================= EXECUTE =================
def execute_message(uid: int, action: str, text: str) -> dict | None:
    user = get_user(uid)
    log(f"[MSG] uid={uid} action={action} state={user['state']}")
    
    if action == "start": return handle_start(user)
    if action == "reset_state":
        user.update({
            "state": STATE_IDLE, "last_experience": "", "last_key_moment": "",
            "deep_count": 0, "last_questions": []
        })
        schedule_save()
        return {"text": "🔄 Пространство очищено. Опиши новый опыт."}
    
    if action == "reject_short": return handle_reject_short()
    if action == "unified": return handle_unified(user, text)
    
    return None

def execute_callback(uid: int, action: str) -> dict | None:
    user = get_user(uid)
    log(f"[CB] uid={uid} action={action} state={user['state']}")
    
    if action == "reset":
        user.update({
            "state": STATE_IDLE, "last_experience": "", "last_key_moment": "",
            "deep_count": 0, "last_questions": []
        })
        schedule_save()
        return {"text": "🔄 Пространство очищено. Опиши новый опыт."}
    
    if action == "self_inquiry:deep":
        return handle_deep(user)
    
    if action == "self_inquiry:pni":
        return handle_pni(user)
    
    if action == "self_inquiry:end":
        return handle_end(user)
    
    return None

# ================= PROCESS =================
def process_message(chat_id: int, text: str) -> None:
    user = get_user(chat_id)

    if not text:
        return

    text = text.strip()

    if is_duplicate(chat_id, user, text):
        log(f"Duplicate text skipped: {chat_id}")
        return

    if is_rate_limited(user):
        send_long_message(chat_id, "⏳ Подожди секунду…")
        log(f"Rate limited: {chat_id}")
        return

    action = route_message(user, text)

    log(f"[MSG] uid={chat_id} state={user['state']} action={action}")

    response = execute_message(chat_id, action, text)

    if response:
        send_long_message(chat_id, response.get("text", ""), response.get("keyboard"))

def process_callback(chat_id: int, data: str) -> None:
    user = get_user(chat_id)

    if not data:
        return

    if is_rate_limited(user):
        send_long_message(chat_id, "⏳ Подожди секунду…")
        log(f"Rate limited callback: {chat_id}")
        return

    action = route_callback(data)
    if action is None:
        return

    log(f"[CB] uid={chat_id} action={action}")

    response = execute_callback(chat_id, action)

    if response:
        send_long_message(chat_id, response.get("text", ""), response.get("keyboard"))

# ================= WEBHOOK =================
class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/12.0"
    
    def _send_json(self, code: int, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    
    def do_GET(self) -> None:
        if self.path in ("/", "/health"):
            self._send_json(200, {
                "ok": True,
                "service": "shaman-bot",
                "version": "12.0",
                "users": len(users),
                "dedup_entries": len(processed_updates)
            })
            return
        self._send_json(404, {"ok": False, "error": "Not found"})
    
    def do_POST(self) -> None:
        if self.path != "/webhook":
            self._send_json(404, {"ok": False, "error": "Not found"})
            return
        
        if WEBHOOK_SECRET:
            incoming = self.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if incoming != WEBHOOK_SECRET:
                self._send_json(403, {"ok": False, "error": "Invalid webhook secret"})
                return
        
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        body = raw.decode("utf-8", errors="replace")
        
        try:
            update = json.loads(body) if body else {}
        except json.JSONDecodeError:
            self._send_json(400, {"ok": False, "error": "Invalid JSON"})
            return
        
        update_id = update.get("update_id")
        if update_id and is_duplicate_update(update_id):
            self._send_json(200, {"ok": True})
            return
        
        try:
            callback = update.get("callback_query")
            if callback:
                cid = callback.get("id", "")
                msg = callback.get("message", {})
                chat_id = msg.get("chat", {}).get("id")
                data = callback.get("data", "")
                
                answer_callback(cid)
                if chat_id and data:
                    process_callback(chat_id, data)
                
                self._send_json(200, {"ok": True})
                return
            
            message = (
                update.get("message")
                or update.get("edited_message")
                or update.get("channel_post")
                or update.get("edited_channel_post")
                or {}
            )
            chat_id = message.get("chat", {}).get("id")
            text = message.get("text") or message.get("caption") or ""
            
            if chat_id and text:
                process_message(chat_id, text)
            
        except Exception as e:
            log(f"FATAL: {traceback.format_exc()}")
        
        self._send_json(200, {"ok": True})
    
    def log_message(self, format: str, *args) -> None:
        log(f"{self.client_address[0]} - {format % args}")

# ================= GRACEFUL SHUTDOWN =================
def signal_handler(signum, frame):
    """🔥 Гарантированное сохранение при SIGTERM"""
    log(f"Received signal {signum}, force saving and exiting...")
    force_save()
    # Закрываем клиенты
    with client_close_lock:
        if not telegram_client.is_closed:
            telegram_client.close()
        if not llm_client.is_closed:
            llm_client.close()
    log("Shutdown complete.")
    os._exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ================= MAIN =================
def set_webhook(public_url: str) -> int:
    payload = {"url": f"{public_url.rstrip('/')}/webhook"}
    if WEBHOOK_SECRET:
        payload["secret_token"] = WEBHOOK_SECRET
    
    result = safe_telegram_api("setWebhook", payload)
    if result and result.get("ok"):
        log(f"Webhook set: {result.get('result', {}).get('url', 'unknown')}")
        return 0
    
    log(f"setWebhook failed: {result}")
    return 1

def main() -> int:
    if len(sys.argv) >= 3 and sys.argv[1] == "--set-webhook":
        return set_webhook(sys.argv[2].strip())
    
    load_users()
    
    if not BOT_TOKEN:
        log("WARNING: BOT_TOKEN empty")
    
    server = ThreadingHTTPServer((HOST, PORT), WebhookHandler)
    log(f"ShamanBot v12.0 PRODUCTION on {HOST}:{PORT}")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("Shutting down...")
    finally:
        force_save()
        server.server_close()
        
        with client_close_lock:
            if not telegram_client.is_closed:
                telegram_client.close()
            if not llm_client.is_closed:
                llm_client.close()
        
        log("Shutdown complete.")
    
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
