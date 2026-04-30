import os
import json
import time
import asyncio
import re
import random
import traceback
import httpx
from urllib.parse import quote
from functools import lru_cache
from collections import deque, defaultdict
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.staticfiles import StaticFiles

# ================= LIFESPAN =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_users()
    log_event("startup", port=int(os.getenv("PORT", "8080")))
    
    # Background maintenance tasks
    cleanup_task = spawn(cleanup_users())
    worker_cleanup_task = spawn(cleanup_workers())
    monitor_task = spawn(monitor_background_tasks())
    user_ids_cleanup_task = spawn(cleanup_user_ids())
    rate_cleanup_task = spawn(cleanup_rate_limits())
    dedup_cleanup_task = spawn(cleanup_dedup())
    
    yield
    
    await force_save()
    log_event("shutdown_start")
    
    # Cancel all maintenance tasks
    for task in [cleanup_task, worker_cleanup_task, monitor_task, 
                 user_ids_cleanup_task, rate_cleanup_task, dedup_cleanup_task]:
        task.cancel()
    
    # Graceful worker shutdown with hard timeout
    if workers:
        for w in workers.values():
            if not w.done():
                w.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(*workers.values(), return_exceptions=True),
                timeout=10
            )
        except asyncio.TimeoutError:
            log_event("shutdown_worker_timeout")
    
    # Background tasks cleanup
    if background_tasks:
        try:
            await asyncio.wait_for(
                asyncio.gather(*background_tasks, return_exceptions=True),
                timeout=5
            )
        except asyncio.TimeoutError:
            log_event("shutdown_tasks_timeout")
    
    # Close HTTP clients
    await telegram_http.aclose()
    await llm_http.aclose()
    await media_http.aclose()
    log_event("shutdown_complete")

app = FastAPI(title="ShamanBot v9.4.0 (production-ready)", lifespan=lifespan)

if os.path.exists("landing"):
    app.mount("/landing", StaticFiles(directory="landing", html=True), name="landing")

# ================= CONFIG =================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
VSEGPT_API_KEY = os.getenv("VSEGPT_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

if not BOT_TOKEN: raise RuntimeError("❌ BOT_TOKEN empty")
if not VSEGPT_API_KEY: raise RuntimeError("❌ VSEGPT_API_KEY empty")
if not WEBHOOK_SECRET: print("⚠️ WEBHOOK_SECRET not set")

VSEGPT_MODEL = "deepseek/deepseek-chat"
ADMIN_ID = 781629557
USER_TTL = 3600
MAX_QUEUE_SIZE = 100
MAX_INPUT_LENGTH = 4000
MAX_WORKERS = 50  # Reduced from 100 to prevent resource exhaustion
MAX_CALLBACK_DATA = 200
BROADCAST_SEMAPHORE = 10
LLM_CONCURRENCY = int(os.getenv("LLM_CONCURRENCY", "20"))
TELEGRAM_CONCURRENCY = int(os.getenv("TELEGRAM_CONCURRENCY", "30"))
RATE_LIMIT_WINDOW = 5
RATE_LIMIT_MAX = 10

# --- Global semaphores ---
llm_semaphore = asyncio.Semaphore(LLM_CONCURRENCY)
telegram_semaphore = asyncio.Semaphore(TELEGRAM_CONCURRENCY)

# --- HTTP Clients with keep-alive (no double retry) ---
limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)

telegram_http = httpx.AsyncClient(
    timeout=httpx.Timeout(30.0, connect=10.0),
    limits=limits,
    transport=httpx.AsyncHTTPTransport(retries=0)  # No transport-level retry
)
llm_http = httpx.AsyncClient(
    timeout=httpx.Timeout(65.0, connect=10.0),
    limits=limits,
    transport=httpx.AsyncHTTPTransport(retries=0)
)
media_http = httpx.AsyncClient(
    timeout=httpx.Timeout(40.0, connect=10.0),
    limits=limits,
    transport=httpx.AsyncHTTPTransport(retries=0)
)

# ================= PERSISTENCE =================
save_lock = asyncio.Lock()
_save_pending = False
_save_flag_lock = asyncio.Lock()
_save_debounce = 5.0

def load_users():
    global users
    try:
        if os.path.exists("data/users.json"):
            with open("data/users.json", "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for uid, data in loaded.items():
                    users[int(uid)] = data
            log_event("load_users", count=len(users))
    except json.JSONDecodeError as e:
        log_event("load_users_corrupted", error=str(e))
        if os.path.exists("data/users.json"):
            backup_name = f"data/users.json.bak.{int(time.time())}"
            os.rename("data/users.json", backup_name)
            log_event("users_backup_created", backup=backup_name)
    except Exception as e:
        log_event("load_users_error", error=str(e))
        traceback.print_exc()

def _save_users_sync():
    os.makedirs("data", exist_ok=True)
    with open("data/users.json", "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)

async def save_users():
    await asyncio.to_thread(_save_users_sync)

async def safe_save():
    """Thread-safe debounced save"""
    global _save_pending
    async with _save_flag_lock:
        if _save_pending:
            return
        _save_pending = True

    try:
        await asyncio.sleep(_save_debounce)
        async with save_lock:
            await save_users()
    finally:
        async with _save_flag_lock:
            _save_pending = False

async def force_save():
    """Immediate save (shutdown)"""
    async with save_lock:
        await save_users()

# ================= STATE =================
STATE_IDLE = "idle"
STATE_REFLECTION = "reflection"
STATE_FOCUS = "focus"
STATE_EMOTION = "emotion"
STATE_PATTERN = "pattern"
STATE_SELF_INQUIRY = "self_inquiry"
STATE_DEEP = "deep"
STATE_LENS = "lens"
STATE_IMAGE = "image"
STATE_ARCHITECT_ANALYSIS = "architect_analysis"
STATE_ARCHITECT_FORMULA = "architect_formula"
STATE_ARCHITECT_STRATEGY = "architect_strategy"

TRANSITIONS = {
    STATE_IDLE: {
        "start": "start",
        "input_experience": "input_experience",
        "short_input": "short_input",
        "show_menu": "show_menu",
        "show_patterns": "show_patterns",
        "art": "art",
        "reset_state": "reset_state",
    },
    STATE_REFLECTION: {"*": "focus"},
    STATE_FOCUS: {"*": "emotion"},
    STATE_EMOTION: {"*": "pattern"},
    STATE_PATTERN: {"*": "mirror_entry"},
    STATE_SELF_INQUIRY: {
        "self_inquiry_response": "self_inquiry_response",
        "self_inquiry_action": "self_inquiry_action",
        "end": "end",
    },
    STATE_DEEP: {"*": "end"},
    STATE_LENS: {"*": "end"},
    STATE_IMAGE: {"*": "image"},
    STATE_ARCHITECT_ANALYSIS: {"*": "architect_analysis_response"},
    STATE_ARCHITECT_FORMULA: {"*": "architect_formula_response"},
    STATE_ARCHITECT_STRATEGY: {"*": "architect_strategy_response"},
}

users = {}
queues = {}
workers = {}
locks = {}
users_lock = asyncio.Lock()
workers_lock = asyncio.Lock()

# Background tasks with error logging
background_tasks = set()

def spawn(coro):
    """Spawn background task with error logging"""
    task = asyncio.create_task(coro)
    
    def _done(t: asyncio.Task):
        background_tasks.discard(t)
        try:
            t.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log_event("background_task_error", error=str(e)[:300])
    
    background_tasks.add(task)
    task.add_done_callback(_done)
    return task

async def monitor_background_tasks():
    while True:
        await asyncio.sleep(300)
        if len(background_tasks) > 50:
            log_event("background_tasks_warning", count=len(background_tasks))

# ================= RATE LIMITING (with cleanup) =================
user_rate_limit = {}

def check_rate_limit(uid: int) -> bool:
    now = time.time()
    bucket = user_rate_limit.setdefault(uid, [])
    bucket.append(now)
    while bucket and now - bucket[0] > RATE_LIMIT_WINDOW:
        bucket.pop(0)
    return len(bucket) <= RATE_LIMIT_MAX

async def cleanup_rate_limits():
    """Prevent memory leak from abandoned rate limit buckets"""
    while True:
        await asyncio.sleep(600)
        now = time.time()
        to_delete = [
            uid for uid, bucket in user_rate_limit.items()
            if not bucket or now - bucket[-1] > RATE_LIMIT_WINDOW * 2
        ]
        for uid in to_delete:
            user_rate_limit.pop(uid, None)

# ================= BROADCAST =================
broadcast_media_data = {}
broadcast_lock = asyncio.Lock()
user_ids_set = set()
user_last_seen = {}
USER_IDS_TTL = 86400

def save_user_to_memory(chat_id: int) -> None:
    now = time.time()
    user_ids_set.add(chat_id)
    user_last_seen[chat_id] = now

async def cleanup_user_ids():
    while True:
        await asyncio.sleep(3600)
        now = time.time()
        to_remove = [uid for uid, ts in user_last_seen.items() if now - ts > USER_IDS_TTL]
        for uid in to_remove:
            user_ids_set.discard(uid)
            user_last_seen.pop(uid, None)

async def save_broadcast_media(media_type: str, file_id: str, caption: str = "") -> None:
    async with broadcast_lock:
        global broadcast_media_data
        broadcast_media_data = {"type": media_type, "file_id": file_id, "caption": caption}

async def load_broadcast_media() -> dict:
    async with broadcast_lock:
        return broadcast_media_data.copy()

async def _send_broadcast_one(uid: int, method: str, key: str, fid: str, cap: str) -> bool:
    for attempt in range(3):
        try:
            async with telegram_semaphore:
                await asyncio.wait_for(
                    telegram_http.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
                        data={"chat_id": uid, key: fid, "caption": cap}
                    ),
                    timeout=10
                )
            return True
        except Exception as e:
            if attempt == 2:
                log_event("broadcast_error", uid=uid, error=str(e)[:100])
            else:
                await asyncio.sleep(0.5 * (attempt + 1))
    return False

async def broadcast_to_all(media: dict) -> int:
    if not user_ids_set:
        return 0
    mtype = media.get("type", "photo")
    fid = media.get("file_id", "")
    cap = media.get("caption", "")
    method = "sendVoice" if mtype == "voice" else "sendPhoto"
    key = "voice" if mtype == "voice" else "photo"
    sem = asyncio.Semaphore(BROADCAST_SEMAPHORE)

    async def send_one(uid):
        async with sem:
            return await _send_broadcast_one(uid, method, key, fid, cap)

    results = await asyncio.gather(*[send_one(uid) for uid in list(user_ids_set)])
    return sum(1 for r in results if r)

# ================= INLINE KEYBOARDS =================
def build_menu_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🧠 Нейро", "callback_data": "neuro"}, {"text": "💭 КПТ", "callback_data": "cbt"}],
        [{"text": "🏺 Юнг", "callback_data": "jung"}, {"text": "🦅 Шаман", "callback_data": "shaman"}],
        [{"text": "🃏 Таро", "callback_data": "tarot"}, {"text": "🧘 Йога", "callback_data": "yoga"}],
        [{"text": "🕉️ Адвайта", "callback_data": "hindu"}, {"text": "🌐 Поле", "callback_data": "field"}],
        [{"text": "👁️ Наблюдатель", "callback_data": "witness"}, {"text": "🎯 Сталкер", "callback_data": "stalker"}],
        [{"text": "🏛️ Архитектор", "callback_data": "architect"}],
    ]}

def build_emotion_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "😨 Страх", "callback_data": "emotion:страх"}],
        [{"text": "😤 Напряжение", "callback_data": "emotion:напряжение"}],
        [{"text": "🤔 Интерес", "callback_data": "emotion:интерес"}],
        [{"text": "😞 Разочарование", "callback_data": "emotion:разочарование"}],
        [{"text": "😌 Принятие", "callback_data": "emotion:принятие"}],
        [{"text": "✍️ Другое", "callback_data": "emotion:другое"}],
    ]}

def build_control_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🎮 Контроль", "callback_data": "control:контроль"}],
        [{"text": "🌊 Потеря контроля", "callback_data": "control:потеря"}],
        [{"text": "🤷 Не знаю", "callback_data": "control:не знаю"}],
    ]}

def build_self_inquiry_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🕳 Углубиться", "callback_data": "self_inquiry:deep"}],
        [{"text": "🔍 Другая линза", "callback_data": "self_inquiry:lens"}],
        [{"text": "🌿 Завершить", "callback_data": "self_inquiry:end"}],
    ]}

async def send_response(uid: int, response: dict) -> None:
    if not response:
        return
    text = response.get("text")
    keyboard = response.get("keyboard")
    if text:
        if keyboard:
            for attempt in range(3):
                try:
                    async with telegram_semaphore:
                        await telegram_http.post(
                            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                            json={
                                "chat_id": uid,
                                "text": text,
                                "reply_markup": keyboard,
                                "parse_mode": "HTML"
                            }
                        )
                    break
                except Exception as e:
                    if attempt == 2:
                        log_event("send_response_error", uid=uid, error=str(e)[:100])
                    else:
                        await asyncio.sleep(0.5 * (attempt + 1))
        else:
            await send(uid, text)

async def answer_callback(callback_id: str) -> None:
    try:
        await telegram_http.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": callback_id}
        )
    except:
        pass

# ================= DEDUP WITH TTL (with cleanup) =================
processed_updates: dict[int, float] = {}
dedup_lock = asyncio.Lock()
DEDUP_TTL = 3600

async def dedup(update_id: int) -> bool:
    """Check and record update with TTL-based deduplication"""
    now = time.time()
    async with dedup_lock:
        if update_id in processed_updates:
            return True
        processed_updates[update_id] = now
        return False

async def cleanup_dedup():
    """Periodic cleanup to prevent memory leak"""
    while True:
        await asyncio.sleep(600)
        now = time.time()
        async with dedup_lock:
            expired = [k for k, v in processed_updates.items() if now - v > DEDUP_TTL]
            for k in expired:
                processed_updates.pop(k, None)

# ================= LOGGING / METRICS =================
trace_log = deque(maxlen=2000)

def log_event(event: str, uid: int = 0, **kwargs):
    print(json.dumps({"ts": time.time(), "event": event, "uid": uid, **kwargs}, ensure_ascii=False))

def trace(uid: int, action: str, stage: str, meta: dict = None):
    log_event("trace", uid=uid, action=action, stage=stage, meta=meta or {})

action_metrics = defaultdict(int)
error_metrics = defaultdict(int)
metrics = {
    "requests": 0, "llm_calls": 0, "broadcasts": 0,
    "guide_sessions": 0, "interpretations": 0,
    "self_inquiries": 0, "vector_updates": 0
}

def record_action(action: str):
    action_metrics[action] += 1

def record_error(error_type: str):
    error_metrics[error_type] += 1

# ================= CIRCUIT BREAKER (fixed) =================
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30):
        self.failure_count = 0
        self.last_failure = 0
        self.threshold = failure_threshold
        self.recovery = recovery_timeout
        self.state = "closed"
        self._lock = asyncio.Lock()
    
    async def call(self, coro):
        async with self._lock:
            if self.state == "open":
                if time.time() - self.last_failure > self.recovery:
                    self.state = "half-open"
                    log_event("circuit_breaker_half_open")
                else:
                    raise CircuitBreakerOpen("Circuit breaker is open")
        
        try:
            result = await coro
            async with self._lock:
                self.failure_count = 0  # ✅ Reset on success
                if self.state == "half-open":
                    self.state = "closed"
                    log_event("circuit_breaker_closed")
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure = time.time()
                if self.failure_count >= self.threshold:
                    self.state = "open"
                    log_event("circuit_breaker_open", failures=self.failure_count)
            raise

class CircuitBreakerOpen(Exception):
    pass

llm_circuit = CircuitBreaker(failure_threshold=5, recovery_timeout=30)

# ================= USER MANAGEMENT (fixed lock order) =================
async def get_user(uid: int):
    uid = int(uid)
    async with users_lock:
        if uid not in users:
            users[uid] = {
                "state": STATE_IDLE, "last_experience": "", "last_active": time.time(),
                "used_lenses": [], "integration_count": 0,
                "_last_response": None, "_last_action": None,
                "_last_user_reflection": None,
                "user_vector": {"depth": 0, "clarity": 0, "resistance": 0, "stability": 0},
                "guide_focus": "", "guide_emotion": "", "guide_control": "",
                "collected_patterns": [],
                "user_world": {"patterns": [], "events": [], "interpretation_nodes": []}
            }
            spawn(safe_save())
        users[uid]["last_active"] = time.time()
        return users[uid]

async def cleanup_users():
    """Clean inactive users - unified lock order: workers_lock → users_lock"""
    while True:
        await asyncio.sleep(600)
        async with workers_lock:
            async with users_lock:
                for uid in list(users.keys()):
                    now = time.time()
                    if now - users[uid]["last_active"] <= USER_TTL:
                        continue
                    q = queues.get(uid)
                    w = workers.get(uid)
                    is_active = (q and not q.empty()) or (w and not w.done())
                    if is_active:
                        continue
                    users.pop(uid, None)
                    queues.pop(uid, None)
                    locks.pop(uid, None)
                    if w:
                        workers.pop(uid, None)
        spawn(safe_save())

async def cleanup_workers():
    while True:
        await asyncio.sleep(300)
        async with workers_lock:
            done_uids = [uid for uid, w in workers.items() if w.done()]
            for uid in done_uids:
                workers.pop(uid, None)

# ================= TELEGRAM HELPERS =================
async def send(chat_id: int, text: str):
    for i in range(0, len(text), 4000):
        chunk = text[i:i+4000]
        for attempt in range(3):
            try:
                async with telegram_semaphore:
                    await asyncio.wait_for(
                        telegram_http.post(
                            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                            json={
                                "chat_id": chat_id,
                                "text": chunk,
                                "parse_mode": "HTML"
                            }
                        ),
                        timeout=10
                    )
                break
            except Exception as e:
                if attempt == 2:
                    log_event("send_error", uid=chat_id, error=str(e)[:100])
                else:
                    await asyncio.sleep(0.5 * (attempt + 1))

async def send_photo(chat_id: int, img: bytes, caption: str = ""):
    for attempt in range(3):
        try:
            async with telegram_semaphore:
                await telegram_http.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
                    data={"chat_id": str(chat_id), "caption": caption},
                    files={"photo": ("img.jpg", img, "image/jpeg")}
                )
            break
        except Exception as e:
            if attempt == 2:
                log_event("send_photo_error", uid=chat_id, error=str(e)[:100])
            else:
                await asyncio.sleep(0.5 * (attempt + 1))

# ================= LLM =================
def get_system_style(user: dict) -> str:
    v = user.get("user_vector", {})
    if v.get("resistance", 0) >= 7: return "soft"
    if v.get("clarity", 0) >= 8: return "precise"
    if v.get("depth", 0) >= 7: return "symbolic"
    if v.get("stability", 0) <= 3: return "grounding"
    return "balanced"

STYLE_PROMPTS = {
    "soft": "Говори мягко, не дави.",
    "precise": "Будь точным и структурным.",
    "symbolic": "Допускай символы и архетипы.",
    "grounding": "Фокус на теле и реальности.",
    "balanced": "Нейтральный аналитический стиль."
}

async def call_llm(messages, temp=0.7, max_tokens=2000, user=None):
    """LLM call with concurrency control and circuit breaker"""
    async with llm_semaphore:
        metrics["llm_calls"] += 1
        
        if user:
            style = get_system_style(user)
            if style != "balanced":
                messages = [{"role": "system", "content": STYLE_PROMPTS[style]}] + messages
        
        async def _make_request():
            for attempt in range(2):
                try:
                    r = await asyncio.wait_for(
                        llm_http.post(
                            "https://api.vsegpt.ru:6070/v1/chat/completions",
                            json={
                                "model": VSEGPT_MODEL,
                                "messages": messages,
                                "temperature": temp,
                                "max_tokens": max_tokens
                            },
                            headers={"Authorization": f"Bearer {VSEGPT_API_KEY}"}
                        ),
                        timeout=65
                    )
                    if r.status_code == 200:
                        data = r.json()
                        if data.get("choices"):
                            return data["choices"][0]["message"]["content"]
                    if attempt < 1:
                        await asyncio.sleep(1)
                except asyncio.TimeoutError:
                    log_event("llm_timeout", attempt=attempt+1)
                    if attempt < 1:
                        await asyncio.sleep(1)
                except Exception as e:
                    log_event("llm_error", error=str(e)[:200])
                    if attempt < 1:
                        await asyncio.sleep(1)
            return "⚠️ Модель временно недоступна."
        
        try:
            return await llm_circuit.call(_make_request())
        except CircuitBreakerOpen:
            return "⚠️ Сервис временно перегружен. Пожалуйста, повтори попытку позже."

# ================= PROMPTS =================
GUIDE_REFLECTION_PROMPT = (
    "Ты — внимательный проводник.\n\n"
    "1. Коротко (2-3 предложения) отрази, что человек пережил — конкретно, без мистики.\n"
    "2. Выдели 2-3 ключевых момента опыта (телесное, эмоции, образы).\n"
    "3. Задай один простой вопрос: «Где в этом опыте было самое сильное место?»\n\n"
    "Без поэзии. Без абстракций. Ясно и по делу. Чистый русский, без маркдауна."
)

GUIDE_PATTERN_PROMPT = "Сформулируй один простой паттерн (1-2 предложения). Чистый русский, коротко."

MIRROR_PROMPT = (
    "Ты даёшь интерпретацию опыта в 2 слоях:\n"
    "1. Нейрофизиология — что в теле и мозге, нормализуй.\n"
    "2. Юнгианский слой — возможные архетипы, как гипотеза.\n"
    "В конце: «Это только зеркало. Важно — что ты сам узнаёшь».\n"
    "Чистый русский."
)

SELF_INQUIRY_PROMPT = (
    "Верни пользователя к его субъективному опыту.\n"
    "Задай вопросы: Что ты сам чувствуешь? Что здесь твоё? С чем ты согласен?\n"
    "2-3 коротких вопроса. Чистый русский."
)

SELF_INQUIRY_MODES = {
    "deepen": "Задай один вопрос глубже в чувство.",
    "clarify": "Помоги сформулировать точнее. 1 отражение + 1 уточняющий вопрос.",
    "reflect": "1 отражение + 1 мягкий вопрос.",
    "soften": "Человек сопротивляется. Мягкий, безопасный вопрос.",
    "silence": "Ничего не спрашивай. 1 фраза фиксации.",
}

GUIDE_DEEP_PROMPT = "Задай один глубокий вопрос: «Если убрать ожидание — что ты на самом деле хотел почувствовать?»"

# ================= LENS LIBRARY =================
LENS_LIBRARY = {
    "neuro": {"name": "Нейрофизиология", "prompt": "Ты — нейрофизиолог."},
    "cbt": {"name": "КПТ", "prompt": "Ты — КПТ-терапевт."},
    "jung": {"name": "Юнг", "prompt": "Ты — юнгианский аналитик."},
    "shaman": {"name": "Шаманизм", "prompt": "Ты — шаман."},
    "tarot": {"name": "Таро", "prompt": "Ты — мастер Таро."},
    "yoga": {"name": "Йога", "prompt": "Ты — мастер йоги."},
    "hindu": {"name": "Адвайта", "prompt": "Ты — учитель адвайты."},
    "field": {"name": "Поле", "prompt": "Ты — голос Поля."},
    "architect": {"name": "Архитектор", "prompt": "Ты — Архитектор сознания."},
    "witness": {"name": "Наблюдатель", "prompt": None, "static_text": "Что бы ты ни переживал — это осознаётся."},
    "stalker": {"name": "Сталкер", "prompt": "Ты — безмолвное присутствие."},
}

async def apply_lens(lens_key: str, experience_text: str, user: dict) -> tuple:
    lens = LENS_LIBRARY[lens_key]
    style = get_system_style(user)
    prefix = {
        "soft": "Мягко.", "precise": "Структурно.",
        "symbolic": "Символично.", "grounding": "Телесно.", "balanced": ""
    }[style]
    prompt = f"{prefix}\n\n{lens['prompt']}" if lens.get("prompt") else None
    if lens.get("static_text"):
        return lens_key, lens["name"], lens["static_text"]
    result = await call_llm([
        {"role": "system", "content": prompt},
        {"role": "user", "content": experience_text}
    ], user=user)
    return lens_key, lens["name"], result

async def generate_image(prompt: str) -> bytes:
    r = await asyncio.wait_for(
        media_http.get(f"https://image.pollinations.ai/prompt/{quote(prompt)}"), timeout=40
    )
    if r.status_code != 200 or len(r.content) < 1000:
        raise Exception("bad image")
    return r.content

# ================= SELF-INQUIRY ENGINE =================
async def build_mirror(user: dict, pattern: str) -> str:
    result = await call_llm([
        {"role": "system", "content": MIRROR_PROMPT},
        {"role": "user", "content": f"Паттерн: {pattern}\n\nОпыт: {user.get('last_experience', '')[:500]}"}
    ], max_tokens=600, user=user)
    metrics["interpretations"] = metrics.get("interpretations", 0) + 1
    return result

async def build_self_inquiry(user: dict, pattern: str) -> str:
    result = await call_llm([
        {"role": "system", "content": SELF_INQUIRY_PROMPT},
        {"role": "user", "content": f"Паттерн: {pattern}\n\nОпыт: {user.get('last_experience', '')[:500]}"}
    ], max_tokens=200, user=user)
    metrics["self_inquiries"] = metrics.get("self_inquiries", 0) + 1
    return result

async def update_user_vector(user: dict, text: str) -> None:
    prompt = (
        "Оцени состояние по сообщению. "
        "Верни ТОЛЬКО JSON: {\"depth\":0-10,\"clarity\":0-10,\"resistance\":0-10,\"stability\":0-10}"
    )
    raw = await call_llm([
        {"role": "system", "content": prompt},
        {"role": "user", "content": text}
    ], max_tokens=150, user=user)
    metrics["vector_updates"] = metrics.get("vector_updates", 0) + 1
    try:
        match = re.search(r"\{.*\}", raw, re.S)
        if match:
            data = json.loads(match.group())
            vec = user.setdefault("user_vector", {"depth": 0, "clarity": 0, "resistance": 0, "stability": 0})
            for k in vec:
                vec[k] = max(0, min(10, int(0.7 * vec[k] + 0.3 * int(data.get(k, 0)))))
    except:
        pass

def choose_inquiry_mode(user: dict) -> str:
    v = user.get("user_vector", {})
    if v.get("resistance", 0) >= 6: return "soften"
    if v.get("depth", 0) < 3: return "deepen"
    if v.get("clarity", 0) < 4: return "clarify"
    if v.get("depth", 0) >= 7 and v.get("stability", 0) >= 5: return "silence"
    return "reflect"

# ================= ROUTE =================
def route(user: dict, text: str) -> str:
    state = user["state"]

    if text.startswith("self_inquiry:"): return "self_inquiry_action"
    if text.startswith("emotion:"): return "emotion"
    if text.startswith("control:"): return "pattern"

    if text == "/start" or text == "/new": return "start"
    if text == "/menu": return "show_menu"
    if text == "/art": return "art"
    if text == "/reset": return "reset_state"
    if text == "/patterns": return "show_patterns"

    lens_cmd = text[1:] if text.startswith("/") else text
    if lens_cmd in LENS_LIBRARY: return f"lens_{lens_cmd}"

    if state == STATE_SELF_INQUIRY and text.lower() in ["всё", "хватит", "понял", "ясно"]:
        return "self_inquiry_action"

    transitions = TRANSITIONS.get(state, {})
    if "*" in transitions: return transitions["*"]
    if state == STATE_IDLE:
        return "input_experience" if len(text.split()) >= 5 else "short_input"
    if state == STATE_SELF_INQUIRY:
        return "self_inquiry_response"

    v = user.get("user_vector", {})
    if v.get("resistance", 0) >= 8 and state == STATE_EMOTION: return "focus"
    if v.get("depth", 0) >= 8 and state == STATE_PATTERN: return "mirror_entry"

    return "start"

# ================= DOMAIN LOGIC =================

async def handle_start(user: dict) -> dict:
    user.update({"state": STATE_IDLE, "last_experience": "", "used_lenses": [], "integration_count": 0})
    spawn(safe_save())
    return {"text": "🌿 Я — проводник осознания.\n\nОпиши, что ты пережил — и я помогу тебе пройти путь от фактов к пониманию, шаг за шагом.", "keyboard": build_menu_keyboard()}

async def handle_experience_input(user: dict, text: str) -> dict:
    metrics["guide_sessions"] = metrics.get("guide_sessions", 0) + 1
    user["last_experience"] = text[:MAX_INPUT_LENGTH]
    result = await call_llm([
        {"role": "system", "content": GUIDE_REFLECTION_PROMPT},
        {"role": "user", "content": text[:MAX_INPUT_LENGTH]}
    ], max_tokens=300, user=user)
    user["state"] = STATE_REFLECTION
    spawn(safe_save())
    return {"text": result}

async def handle_focus(user: dict, text: str) -> dict:
    user["guide_focus"] = text
    user["state"] = STATE_FOCUS
    spawn(safe_save())
    return {"text": "🪨 Ты выбрал это место.\n\nКакое чувство здесь было сильнее всего?", "keyboard": build_emotion_keyboard()}

async def handle_emotion(user: dict, text: str) -> dict:
    clean = text.replace("emotion:", "") if text.startswith("emotion:") else text
    user["guide_emotion"] = clean
    user["state"] = STATE_EMOTION
    spawn(safe_save())
    return {"text": "🌬️ Это больше про контроль или про отпускание?", "keyboard": build_control_keyboard()}

async def handle_pattern(user: dict, text: str) -> dict:
    clean = text.replace("control:", "") if text.startswith("control:") else text
    user["guide_control"] = clean
    result = await call_llm([
        {"role": "system", "content": GUIDE_PATTERN_PROMPT},
        {"role": "user", "content": user["last_experience"][:500]}
    ], max_tokens=300, user=user)
    pattern_line = result.split("\n")[0] if "\n" in result else result
    patterns = user.setdefault("collected_patterns", [])
    patterns.append(pattern_line)
    if len(patterns) > 50:
        patterns.pop(0)
    world = user.setdefault("user_world", {})
    world.setdefault("patterns", []).append(pattern_line)
    user["state"] = STATE_SELF_INQUIRY
    mirror_text = await build_mirror(user, pattern_line)
    question = await build_self_inquiry(user, pattern_line)
    spawn(safe_save())
    return {
        "text": (
            f"🧭 Я зафиксировал узор:\n\n{pattern_line}\n\n"
            f"────────────────────\n🧠 Интерпретация:\n{mirror_text}\n\n"
            f"────────────────────\n❓ {question}"
        ),
        "keyboard": build_self_inquiry_keyboard()
    }

async def handle_self_inquiry_response(user: dict, uid: int, text: str) -> dict:
    user_input = text[:500]
    user["_last_user_reflection"] = user_input
    
    # ✅ Optimize: only update vector 30% of the time
    if random.random() < 0.3:
        await update_user_vector(user, user_input)
    
    mode = choose_inquiry_mode(user)
    trace(uid, "self_inquiry_mode", mode, user.get("user_vector", {}))
    prompt = SELF_INQUIRY_MODES[mode]
    result = await call_llm([
        {"role": "system", "content": prompt},
        {"role": "user", "content": user_input}
    ], max_tokens=200, user=user)
    if mode == "silence":
        return {"text": result}
    return {"text": f"{result}", "keyboard": build_self_inquiry_keyboard()}

async def handle_self_inquiry_action(user: dict, text: str) -> dict | None:
    sub = text.split(":", 1)[1] if ":" in text else text
    if sub in ["всё", "хватит", "понял", "ясно"]:
        sub = "end"
    if sub == "deep":
        user["state"] = STATE_DEEP
        result = await call_llm([
            {"role": "system", "content": GUIDE_DEEP_PROMPT},
            {"role": "user", "content": user["last_experience"][:500]}
        ], max_tokens=150, user=user)
        spawn(safe_save())
        return {"text": result}
    elif sub == "lens":
        user["state"] = STATE_LENS
        return {"text": "Выбери, через что посмотреть:", "keyboard": build_menu_keyboard()}
    elif sub == "end":
        user["state"] = STATE_IDLE
        spawn(safe_save())
        return {"text": "🌿 Принято. Ты можешь понаблюдать это в жизни или принести новый опыт."}
    return None

async def handle_lens(user: dict, lens_key: str) -> dict:
    if not user.get("last_experience"):
        return {"text": "Сначала расскажи опыт."}
    lens_key, lens_name, lresult = await apply_lens(lens_key, user["last_experience"], user)
    record_action(f"lens_{lens_key}")
    user.setdefault("used_lenses", []).append(lens_key)
    user["state"] = STATE_IDLE
    spawn(safe_save())
    return {"text": f"Смотрю через «{lens_name}».\n\n{lresult}\n\nЧто ты возьмёшь из этого в следующий раз?"}

# ================= EXECUTE =================
async def execute(uid: int, action: str, text: str) -> dict | None:
    user = await get_user(uid)
    trace(uid, action, "exec_start")
    if user.get("_last_action") == action and action in ("short_input", "end"):
        return None
    user["_last_action"] = action

    if action == "start": return await handle_start(user)
    if action == "reset_state":
        user.update({"used_lenses": [], "integration_count": 0, "state": STATE_IDLE})
        spawn(safe_save())
        return {"text": "🔄 Пространство очищено."}
    if action == "show_menu": return {"text": "🎯 Выбери линзу:", "keyboard": build_menu_keyboard()}
    if action == "show_patterns":
        patterns = user.get("collected_patterns", [])
        if not patterns: return {"text": "Узоры ещё не проявились."}
        return {"text": "🕸️ Твои узоры:\n\n" + "\n\n".join(f"{i+1}. {p}" for i, p in enumerate(patterns[-10:]))}
    if action == "art": user["state"] = STATE_IMAGE; return {"text": "🎨 Опиши образ."}
    if action == "image":
        try:
            img = await generate_image(text[:MAX_INPUT_LENGTH])
            await send_photo(uid, img, f"✨ {text}")
            user["state"] = STATE_IDLE
            return {"text": "✨ Образ проявился."}
        except:
            user["state"] = STATE_IDLE
            return {"text": "🌫️ Образ не смог проявиться."}

    if action == "input_experience": return await handle_experience_input(user, text)
    if action == "short_input":
        user["last_experience"] = text; user["state"] = STATE_REFLECTION
        result = await call_llm([
            {"role": "system", "content": GUIDE_REFLECTION_PROMPT},
            {"role": "user", "content": text}
        ], max_tokens=300, user=user)
        return {"text": result}
    if action == "focus": return await handle_focus(user, text)
    if action == "emotion": return await handle_emotion(user, text)
    if action == "pattern": return await handle_pattern(user, text)
    if action == "mirror_entry":
        user["state"] = STATE_SELF_INQUIRY
        world = user.get("user_world", {})
        patterns = world.get("patterns", [])
        last_pattern = patterns[-1] if patterns else ""
        mirror_text = await build_mirror(user, last_pattern)
        question = await build_self_inquiry(user, last_pattern)
        spawn(safe_save())
        return {
            "text": f"🧠 Интерпретация:\n{mirror_text}\n\n────────────────────\n❓ {question}",
            "keyboard": build_self_inquiry_keyboard()
        }

    if action == "self_inquiry_response": return await handle_self_inquiry_response(user, uid, text)
    if action == "self_inquiry_action": return await handle_self_inquiry_action(user, text)
    if action == "deep":
        user["state"] = STATE_DEEP
        result = await call_llm([
            {"role": "system", "content": GUIDE_DEEP_PROMPT},
            {"role": "user", "content": user["last_experience"][:500]}
        ], max_tokens=150, user=user)
        spawn(safe_save())
        return {"text": result}
    if action == "lens_choice": user["state"] = STATE_LENS; return {"text": "Выбери, через что посмотреть:", "keyboard": build_menu_keyboard()}
    if action == "end":
        user["state"] = STATE_IDLE; spawn(safe_save())
        return {"text": "Ты можешь понаблюдать это в жизни или принести новый опыт. Я здесь."}

    if action.startswith("lens_"): return await handle_lens(user, action.replace("lens_", ""))

    if action == "architect_analysis_response":
        user["_architect_raw"] = user["last_experience"] + "\n\n" + text
        result = await call_llm([
            {"role": "system", "content": LENS_LIBRARY["architect"]["prompt"]},
            {"role": "user", "content": user["_architect_raw"]}
        ], max_tokens=2500, user=user)
        user["_architect_analysis"] = result; user["state"] = STATE_ARCHITECT_FORMULA
        return {"text": result + "\n\nСтоит эта конструкция?"}
    if action == "architect_formula_response":
        if any(w in text.lower() for w in ["да", "стоит", "принимаю"]):
            user["state"] = STATE_ARCHITECT_STRATEGY
            return {"text": await call_llm([
                {"role": "system", "content": "Выдай алгоритм из 3 шагов."},
                {"role": "user", "content": user.get("_architect_analysis", "")}
            ], max_tokens=2000, user=user)}
        user["state"] = STATE_ARCHITECT_ANALYSIS; return {"text": "Что именно не держится?"}
    if action == "architect_strategy_response":
        user["state"] = STATE_IDLE; user["_architect_raw"] = None; user["_architect_analysis"] = None
        return {"text": "Цикл Архитектора завершён. /menu для линз."}

    log_event("empty_response", uid=uid, action=action)
    return None

# ================= WORKER =================
async def worker(uid: int):
    q = queues[uid]
    try:
        while True:
            msg = await q.get()
            if msg is None: break
            async with locks[uid]:
                user = await get_user(uid)
                metrics["requests"] += 1
                action = route(user, msg)
                record_action(action)
                response = await execute(uid, action, msg)
                if not response:
                    log_event("empty_response_worker", uid=uid, action=action)
                    continue
                last = user.get("_last_response")
                if last and response.get("text") == last.get("text"): continue
                user["_last_response"] = response
                await send_response(uid, response)
    except Exception as e:
        log_event("worker_crash", uid=uid, error=str(e)[:500])
    finally:
        log_event("worker_stopped", uid=uid)

async def enqueue(uid: int, text: str):
    # Rate limit check
    if not check_rate_limit(uid):
        log_event("rate_limited", uid=uid)
        return
    
    async with workers_lock:
        if uid not in workers and len(workers) >= MAX_WORKERS:
            return
        
        if uid not in queues:
            queues[uid] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
            locks[uid] = asyncio.Lock()
        
        q = queues[uid]
        
        # Backpressure: don't drop, wait briefly
        if q.full():
            try:
                await asyncio.wait_for(q.put(text), timeout=0.2)
                return
            except asyncio.TimeoutError:
                record_error("queue_overflow")
                log_event("queue_overflow", uid=uid)
                return
        
        try:
            q.put_nowait(text)
        except asyncio.QueueFull:
            record_error("queue_full")
            log_event("queue_full", uid=uid)
            return
        
        if uid not in workers or workers[uid].done():
            workers[uid] = spawn(worker(uid))

# ================= WEBHOOK =================
@app.post("/webhook")
async def webhook(req: Request, x_telegram_bot_api_secret_token: str = Header(None)):
    if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        raise HTTPException(403)
    try:
        data = await req.json()
    except:
        return {"ok": True}
    
    callback = data.get("callback_query")
    if callback:
        cid = callback.get("id")
        chat_id = callback["message"]["chat"]["id"]
        dtext = callback.get("data", "")
        
        # ✅ Protect against huge callback_data
        if len(dtext) > MAX_CALLBACK_DATA:
            return {"ok": True}
        
        await answer_callback(cid)
        
        if dtext.startswith("self_inquiry:"): await enqueue(chat_id, dtext)
        elif dtext.startswith("emotion:"): await enqueue(chat_id, dtext)
        elif dtext.startswith("control:"): await enqueue(chat_id, dtext)
        else: await enqueue(chat_id, dtext)
        return {"ok": True}
    
    msg = data.get("message") or data.get("edited_message") or {}
    if not msg: return {"ok": True}
    
    update_id = data.get("update_id")
    if update_id and await dedup(update_id): return {"ok": True}
    
    chat_id = msg["chat"]["id"]
    save_user_to_memory(chat_id)
    
    # Admin broadcast logic
    if chat_id == ADMIN_ID:
        photo = msg.get("photo")
        voice = msg.get("voice")
        caption = msg.get("caption", "")
        
        if photo:
            await save_broadcast_media("photo", photo[-1]["file_id"], caption)
            spawn(send(chat_id, "✅ Фото сохранено."))
            return {"ok": True}
        if voice:
            await save_broadcast_media("voice", voice["file_id"], caption)
            spawn(send(chat_id, "✅ Голосовое сохранено."))
            return {"ok": True}
    
    text = msg.get("text", "")
    
    if chat_id == ADMIN_ID and text == "/send_all":
        media = await load_broadcast_media()
        if not media:
            spawn(send(chat_id, "❌ Нет медиа."))
        else:
            metrics["broadcasts"] += 1
            count = await broadcast_to_all(media)
            spawn(send(chat_id, f"✅ Отправлено: {count}"))
        return {"ok": True}
    
    if text:
        await enqueue(chat_id, text.strip())
    
    return {"ok": True}

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "users": len(users),
        "workers": len(workers),
        "active_workers": sum(1 for w in workers.values() if not w.done()),
        "background_tasks": len(background_tasks),
        "user_ids_set": len(user_ids_set),
        "rate_limit_buckets": len(user_rate_limit),
        "dedup_entries": len(processed_updates),
        "circuit_breaker": llm_circuit.state,
        "metrics": metrics
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")), workers=1, log_level="info")
