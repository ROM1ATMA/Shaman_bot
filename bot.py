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

try:
    from vosk import Model, KaldiRecognizer
    import wave
    VOSK_AVAILABLE = True
except ImportError:
    VOSK_AVAILABLE = False
    print("⚠️ Vosk не установлен.")

VOSK_MODEL_PATH = "vosk-model-small-ru-0.22"
SAMPLE_RATE = 16000

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

app = FastAPI(title="ShamanBot v8.3.2 (visual menu)", lifespan=lifespan)

if os.path.exists("landing"):
    app.mount("/landing", StaticFiles(directory="landing", html=True), name="landing")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
VSEGPT_API_KEY = os.getenv("VSEGPT_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

if not BOT_TOKEN: raise RuntimeError("❌ BOT_TOKEN is empty")
if not VSEGPT_API_KEY: raise RuntimeError("❌ VSEGPT_API_KEY is empty")
if not WEBHOOK_SECRET: print("⚠️ WEBHOOK_SECRET not set")

VSEGPT_MODEL = "deepseek/deepseek-chat"
ADMIN_ID = 781629557
USER_TTL = 3600
MAX_QUEUE_SIZE = 100
MAX_INPUT_LENGTH = 4000
MAX_WORKERS = 100

http_limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
telegram_http = httpx.AsyncClient(timeout=30, limits=http_limits)
llm_http = httpx.AsyncClient(timeout=60, limits=http_limits)
media_http = httpx.AsyncClient(timeout=60, limits=http_limits)

STATE_IDLE = "idle"
STATE_SELF_INQUIRY = "self_inquiry"
STATE_EXPERIENCE_RECEIVED = "experience_received"
STATE_IMAGE = "image"
STATE_ANCHOR_AWAIT = "anchor_await"
STATE_ARCHITECT_ANALYSIS = "architect_analysis"
STATE_ARCHITECT_FORMULA = "architect_formula"
STATE_ARCHITECT_STRATEGY = "architect_strategy"

users = {}
queues = {}
workers = {}
locks = {}

BROADCAST_FILE = "broadcast_media.json"
USER_IDS_FILE = "user_ids.json"

def save_user_to_file(chat_id: int) -> None:
    try:
        if os.path.exists(USER_IDS_FILE):
            with open(USER_IDS_FILE, "r") as f: u = json.load(f)
        else: u = []
        if chat_id not in u:
            u.append(chat_id)
            with open(USER_IDS_FILE, "w") as f: json.dump(u, f)
    except: pass

def save_broadcast_media(media_type: str, file_id: str, caption: str = "") -> None:
    with open(BROADCAST_FILE, "w") as f: json.dump({"type": media_type, "file_id": file_id, "caption": caption}, f)

def load_broadcast_media() -> dict:
    if not os.path.exists(BROADCAST_FILE): return {}
    with open(BROADCAST_FILE, "r") as f: return json.load(f)

async def broadcast_to_all(media: dict) -> int:
    if not os.path.exists(USER_IDS_FILE): return 0
    with open(USER_IDS_FILE, "r") as f: u = json.load(f)
    media_type = media.get("type", "photo")
    file_id = media.get("file_id", "")
    caption = media.get("caption", "")
    method = "sendVoice" if media_type == "voice" else "sendPhoto"
    key = "voice" if media_type == "voice" else "photo"
    count = 0
    for uid in u:
        try:
            payload = {"chat_id": uid, key: file_id, "caption": caption}
            await asyncio.wait_for(telegram_http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", data=payload, timeout=10), timeout=10)
            count += 1
            await asyncio.sleep(0.05)
        except: pass
    return count

# ================= INLINE KEYBOARD =================

def build_menu_keyboard() -> dict:
    buttons = [
        [{"text": "🧠 Нейро", "callback_data": "neuro"}],
        [{"text": "💭 КПТ", "callback_data": "cbt"}],
        [{"text": "🏺 Юнг", "callback_data": "jung"}],
        [{"text": "🦅 Шаман", "callback_data": "shaman"}],
        [{"text": "🃏 Таро", "callback_data": "tarot"}],
        [{"text": "🧘 Йога", "callback_data": "yoga"}],
        [{"text": "🕉️ Адвайта", "callback_data": "hindu"}],
        [{"text": "🌐 Поле", "callback_data": "field"}],
        [{"text": "👁️ Наблюдатель", "callback_data": "witness"}],
        [{"text": "🎯 Сталкер", "callback_data": "stalker"}],
        [{"text": "🏛️ Архитектор", "callback_data": "architect"}],
        [
            {"text": "✨ Собрать в целое", "callback_data": "/integrate"},
            {"text": "🔄 Новый опыт", "callback_data": "/new"},
        ],
    ]
    return {"inline_keyboard": buttons}

async def send_menu_with_buttons(chat_id: int) -> None:
    await telegram_http.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": "🎯 *Выбери линзу:*",
            "parse_mode": "Markdown",
            "reply_markup": build_menu_keyboard()
        }
    )

async def answer_callback(callback_id: str, text: str = "Принято") -> None:
    await telegram_http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id": callback_id, "text": text})

# ================= DEDUP / LOGGING / METRICS =================

processed_updates = set()
processed_order = deque(maxlen=5000)
def dedup(update_id: int) -> bool:
    if update_id in processed_updates: return True
    processed_updates.add(update_id)
    processed_order.append(update_id)
    if len(processed_updates) > 5000: processed_updates.discard(processed_order.popleft())
    return False

trace_log = deque(maxlen=2000)
def log_event(event: str, uid: int = 0, **kwargs):
    entry = {"ts": time.time(), "event": event, "uid": uid, **kwargs}
    print(json.dumps(entry, ensure_ascii=False))
    trace_log.append(entry)
def trace(uid: int, action: str, stage: str, meta: dict = None):
    log_event("trace", uid=uid, action=action, stage=stage, meta=meta or {})

action_metrics = defaultdict(int)
action_latency = defaultdict(lambda: [0.0, 0])
error_metrics = defaultdict(int)
transition_metrics = defaultdict(int)
metrics = {"requests": 0, "llm_calls": 0, "image_calls": 0, "voice_calls": 0, "broadcasts": 0, "integrations": 0, "anchor_questions": 0, "architect_sessions": 0}
def record_action(action: str): action_metrics[action] += 1
def record_latency(action: str, latency: float): action_latency[action][0] += latency; action_latency[action][1] += 1
def record_error(error_type: str): error_metrics[error_type] += 1
def record_transition(from_action: str, to_action: str): transition_metrics[(from_action, to_action)] += 1
def get_latency_stats():
    return {action: {"count": count, "avg_seconds": round(total / count, 3) if count > 0 else 0} for action, (total, count) in action_latency.items()}
def get_transition_stats():
    return [{"from": f, "to": t, "count": c} for (f, t), c in transition_metrics.items()]

# ================= PROMPTS / LENSES (сокращены для компактности) =================

SELF_INQUIRY_PROMPT = "Ты — коуч. Найди пробел в описании опыта (тело, чувства, смысл) и задай ОДИН мягкий вопрос. Если всё глубоко — ответь ПОЛНО."
LENS_LIBRARY = {
    "neuro": {"name": "Нейрофизиология", "prompt": "Ты — нейрофизиолог. Объясни опыт через мозг."},
    "cbt": {"name": "КПТ", "prompt": "Ты — КПТ-терапевт. Раздели факты и мысли."},
    "jung": {"name": "Юнг", "prompt": "Ты — юнгианский аналитик. Раскрой архетипы."},
    "shaman": {"name": "Шаманизм", "prompt": "Ты — шаман. Интерпретируй опыт."},
    "tarot": {"name": "Таро", "prompt": "Ты — мастер Таро. Посмотри через Арканы."},
    "yoga": {"name": "Йога", "prompt": "Ты — мастер йоги. Опиши энергетические процессы."},
    "hindu": {"name": "Адвайта", "prompt": "Ты — учитель адвайты. Где иллюзия, а где Свидетель?"},
    "field": {"name": "Поле", "prompt": "Ты — голос Поля. Покажи, как реальность собирается из пустоты."},
    "architect": {"name": "Архитектор", "prompt": "Ты — Архитектор сознания. Выяви структуру."},
    "witness": {"name": "Наблюдатель", "prompt": None, "static_text": "Что бы ты ни переживал — это осознаётся..."},
    "stalker": {"name": "Сталкер", "prompt": "Ты — безмолвное присутствие. Указывай на Осознавание."},
}
ANCHOR_QUESTIONS = {
    "neuro": ["Что в теле изменилось после этого?"],
    "cbt": ["Какая мысль крутится сейчас?"],
    "jung": ["Что ты чувствуешь к этому архетипу сейчас?"],
    "shaman": ["Что ты принёс из путешествия в теле?"],
    "tarot": ["Что в этом аркане — про тебя?"],
    "yoga": ["Где в теле движется энергия сейчас?"],
    "hindu": ["Кто наблюдает эту мысль?"],
    "field": ["Что будет, если перестать фиксировать этот узел?"],
    "witness": ["Кто читает эти слова?"],
    "stalker": ["Что останется без слов?"],
}

def get_user(uid: int):
    if uid not in users:
        users[uid] = {"state": STATE_IDLE, "last_experience": "", "last_action": None, "last_active": time.time(), "used_lenses": [], "micro_states": [], "integration_count": 0, "pending_anchor_lens": None, "last_integration_action": None, "_architect_raw": None, "_architect_analysis": None}
    users[uid]["last_active"] = time.time()
    return users[uid]

async def cleanup_users():
    while True:
        now = time.time()
        for uid in list(users.keys()):
            if now - users[uid]["last_active"] > USER_TTL:
                users.pop(uid, None); queues.pop(uid, None); locks.pop(uid, None)
                worker = workers.pop(uid, None)
                if worker and not worker.done(): worker.cancel()
        await asyncio.sleep(600)

async def send(chat_id: int, text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for i in range(0, len(text), 4000):
        try: await asyncio.wait_for(telegram_http.post(url, json={"chat_id": chat_id, "text": text[i:i+4000]}), timeout=10)
        except Exception as e: log_event("send_timeout", uid=chat_id, error=str(e)[:100])

async def send_photo(chat_id: int, img: bytes, caption: str = ""):
    try: await asyncio.wait_for(telegram_http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto", data={"chat_id": str(chat_id), "caption": caption}, files={"photo": ("img.jpg", img, "image/jpeg")}), timeout=15)
    except Exception as e: log_event("send_photo_timeout", uid=chat_id, error=str(e)[:100])

async def call_llm(messages, temp=0.7, max_tokens=2000):
    metrics["llm_calls"] += 1
    for attempt in range(2):
        try:
            r = await llm_http.post("https://api.vsegpt.ru:6070/v1/chat/completions", json={"model": VSEGPT_MODEL, "messages": messages, "temperature": temp, "max_tokens": max_tokens}, headers={"Authorization": f"Bearer {VSEGPT_API_KEY}"})
            if r.status_code == 200:
                data = r.json()
                if "choices" in data and data["choices"]: return data["choices"][0]["message"]["content"]
                record_error("llm_parse_fail")
            else: record_error("llm_fail")
            if attempt < 1: await asyncio.sleep(1)
        except: record_error("llm_exception"); await asyncio.sleep(1)
    return "⚠️ Модель временно недоступна."

async def ask_self_inquiry(text: str): return await call_llm([{"role": "system", "content": SELF_INQUIRY_PROMPT}, {"role": "user", "content": text}], temp=0.5, max_tokens=150)
async def apply_lens(lens_key: str, experience_text: str) -> tuple:
    lens = LENS_LIBRARY[lens_key]
    if lens.get("static_text"): return lens_key, lens["name"], lens["static_text"]
    result = await call_llm([{"role": "system", "content": lens["prompt"]}, {"role": "user", "content": f"Опыт: {experience_text}"}], temp=0.7)
    return lens_key, lens["name"], result

async def generate_image(prompt: str) -> bytes:
    metrics["image_calls"] += 1
    r = await asyncio.wait_for(media_http.get(f"https://image.pollinations.ai/prompt/{httpx.quote(prompt)}"), timeout=40)
    if r.status_code != 200: raise Exception("bad status")
    if "image" not in r.headers.get("Content-Type", ""): raise Exception("not image")
    if len(r.content) < 1000: raise Exception("too small")
    return r.content

def classify_anchor_response(answer: str) -> dict:
    a = answer.lower()
    depth = "insight" if any(w in a for w in ["осознал", "озарение", "дошло"]) else ("emotional" if any(w in a for w in ["чувствую", "страх", "радость"]) else ("resistance" if any(w in a for w in ["не верю", "сомневаюсь"]) else "surface"))
    relation = "integration" if any(w in a for w in ["принимаю", "резонирует"]) else ("overwhelm" if any(w in a for w in ["слишком много", "перегруз"]) else ("rejection" if any(w in a for w in ["не моё", "чужое"]) else "exploration"))
    dominant = "body" if any(w in a for w in ["тело", "грудь", "живот"]) else ("symbol" if any(w in a for w in ["образ", "вижу"]) else ("meaning" if any(w in a for w in ["смысл", "понял"]) else "emotion"))
    return {"depth": depth, "relation": relation, "dominant": dominant, "raw": answer, "timestamp": time.time()}

def should_trigger_integrator(user: dict) -> tuple:
    used = user.get("used_lenses", []); micro = user.get("micro_states", [])
    if len(used) < 2: return False, "not_enough_lenses"
    if not micro: return (True, "lens_count_force") if len(used) >= 3 else (False, "no_micro_states")
    last = micro[-1]
    if last["depth"] == "insight" and last["relation"] == "integration": return True, "insight_integration"
    if last["relation"] == "overwhelm": return True, "overwhelm"
    if sum(1 for m in micro if m["depth"] == "insight") >= 2: return True, "double_insight"
    if len(used) >= 3: return True, "lens_count_force"
    if last["relation"] == "rejection": return False, "resistance"
    return False, "not_ready"

async def run_integrator(user: dict) -> str:
    metrics["integrations"] = metrics.get("integrations", 0) + 1
    user["last_integration_action"] = user.get("last_action")
    dominant = user["micro_states"][-1].get("dominant", "emotion") if user.get("micro_states") else "emotion"
    return await call_llm([{"role": "system", "content": "Собери опыт в единую картину."}, {"role": "user", "content": user["last_experience"]}], temp=0.7)

def assemble_lens_response(lens_key: str, lens_name: str, result: str, user: dict) -> str:
    parts = [f"Смотрю через «{lens_name}».\n\n{result}"]
    anchor = random.choice(ANCHOR_QUESTIONS[lens_key]) if lens_key in ANCHOR_QUESTIONS else None
    if anchor:
        parts.append(f"\n\n{anchor}")
        user["pending_anchor_lens"] = lens_key
        user["state"] = STATE_ANCHOR_AWAIT
        metrics["anchor_questions"] = metrics.get("anchor_questions", 0) + 1
    return "\n".join(parts)

def route(user: dict, text: str) -> str:
    state = user["state"]
    if text == "/start" or text == "/new": return "new_experience"
    if text == "/menu": return "show_menu"
    if text == "/art": return "art"
    if text == "/integrate": return "manual_integrate"
    if text == "/reset": return "reset_state"
    lens_cmd = text.lstrip("/")
    if lens_cmd in LENS_LIBRARY: return f"lens_{lens_cmd}"
    if state == STATE_IMAGE: return "image"
    if state == STATE_ANCHOR_AWAIT: return "anchor_response"
    if state == STATE_SELF_INQUIRY: return "self_inquiry_response"
    if state == STATE_ARCHITECT_ANALYSIS: return "architect_analysis_response"
    if state == STATE_ARCHITECT_FORMULA: return "architect_formula_response"
    if state == STATE_ARCHITECT_STRATEGY: return "architect_strategy_response"
    if state == STATE_EXPERIENCE_RECEIVED: return "new_experience_silent" if len(text.split()) >= 5 else "short_input_with_state"
    return "experience_with_inquiry" if len(text.split()) >= 5 else "short_input"

async def execute(uid: int, action: str, text: str) -> str:
    user = get_user(uid)
    trace(uid, action, "exec_start")
    last_action = user.get("last_action")
    if last_action and last_action != action: record_transition(last_action, action)
    user["last_action"] = action

    if action == "new_experience":
        user.update({"state": STATE_IDLE, "last_experience": "", "used_lenses": [], "micro_states": [], "integration_count": 0, "pending_anchor_lens": None, "last_integration_action": None, "_architect_raw": None, "_architect_analysis": None})
        asyncio.create_task(send_menu_with_buttons(uid))
        return "🌿 Я — многомерный проводник и коуч.\n\nРасскажи свой опыт, и я помогу тебе исследовать его глубже.\n\n👆 Выбери линзу на кнопках выше или расскажи, что ты пережил."

    if action == "reset_state":
        user.update({"used_lenses": [], "micro_states": [], "integration_count": 0, "pending_anchor_lens": None, "last_integration_action": None, "_architect_raw": None, "_architect_analysis": None, "state": STATE_IDLE})
        return "🔄 Состояние сброшено."

    if action == "show_menu":
        await send_menu_with_buttons(uid)
        return None

    if action == "art":
        user["state"] = STATE_IMAGE
        return "🎨 Опиши образ."

    if action == "image":
        user["state"] = STATE_EXPERIENCE_RECEIVED
        try:
            img = await generate_image(text[:MAX_INPUT_LENGTH])
            await send_photo(uid, img, f"✨ {text}")
            return "✨ Образ создан."
        except: return "🌫️ Не удалось создать образ."

    if action.startswith("lens_"):
        lens_key = action.replace("lens_", "")
        if not user.get("last_experience"): return "Сначала расскажи опыт."
        if lens_key == "architect":
            metrics["architect_sessions"] = metrics.get("architect_sessions", 0) + 1
            user["state"] = STATE_ARCHITECT_ANALYSIS
            return "🔧 Активирован Архитектор. Ответь на три вопроса:\n1. Что твоя Ось?\n2. Как устроена вторая сторона?\n3. Где линия разлома?"
        lens_key, lens_name, result = await apply_lens(lens_key, user["last_experience"])
        record_action(f"lens_{lens_key}")
        user.setdefault("used_lenses", []).append(lens_key)
        return assemble_lens_response(lens_key, lens_name, result, user)

    if action == "experience_with_inquiry":
        user["last_experience"] = text[:MAX_INPUT_LENGTH]
        inquiry = await ask_self_inquiry(text)
        if "ПОЛНО" in inquiry.upper():
            user["state"] = STATE_EXPERIENCE_RECEIVED
            return await _auto_lens_or_ask(uid, text, "🌿 Сохранил твой опыт.")
        user["state"] = STATE_SELF_INQUIRY
        return inquiry

    if action == "self_inquiry_response":
        user["last_experience"] = user["last_experience"] + "\n\n" + text
        user["state"] = STATE_EXPERIENCE_RECEIVED
        return await _auto_lens_or_ask(uid, user["last_experience"], "🌿 Спасибо.")

    if action == "architect_analysis_response":
        user["_architect_raw"] = user["last_experience"] + "\n\n" + text
        result = await call_llm([{"role": "system", "content": LENS_LIBRARY["architect"]["prompt"]}, {"role": "user", "content": user["_architect_raw"]}], max_tokens=2500)
        user["_architect_analysis"] = result
        user["state"] = STATE_ARCHITECT_FORMULA
        return result + "\n\nСтоит эта конструкция?"

    if action == "architect_formula_response":
        if any(w in text.lower() for w in ["да", "стоит", "принимаю"]):
            user["state"] = STATE_ARCHITECT_STRATEGY
            return await call_llm([{"role": "system", "content": "Выдай алгоритм коммуникации из 3 шагов."}, {"role": "user", "content": user.get("_architect_analysis", "")}], max_tokens=2000)
        user["state"] = STATE_ARCHITECT_ANALYSIS
        return "Что именно не держится?"

    if action == "architect_strategy_response":
        user["state"] = STATE_EXPERIENCE_RECEIVED
        user["_architect_raw"] = None; user["_architect_analysis"] = None
        return "Цикл Архитектора завершён. /menu для линз."

    if action == "anchor_response":
        micro = classify_anchor_response(text)
        micro["lens"] = user.get("pending_anchor_lens", "unknown")
        user["pending_anchor_lens"] = None
        user.setdefault("micro_states", []).append(micro)
        trigger, reason = should_trigger_integrator(user)
        if trigger:
            user["state"] = STATE_EXPERIENCE_RECEIVED
            integration_text = await run_integrator(user)
            user["integration_count"] = user.get("integration_count", 0) + 1
            return f"{integration_text}\n\n/menu для линз."
        user["state"] = STATE_EXPERIENCE_RECEIVED
        return "Я запомнил это. /menu для линз."

    if action == "manual_integrate":
        if not user.get("last_experience"): return "Сначала расскажи опыт."
        if len(user.get("used_lenses", [])) < 1: return "Нужна хотя бы одна линза."
        integration_text = await run_integrator(user)
        user["integration_count"] = user.get("integration_count", 0) + 1
        user["state"] = STATE_EXPERIENCE_RECEIVED
        return f"{integration_text}\n\n/menu для линз."

    return "🌫️ Неизвестное действие. /menu"

async def _auto_lens_or_ask(uid: int, text: str, prefix: str) -> str:
    return f"{prefix}\n\n/menu для выбора линзы."

async def worker(uid: int):
    q = queues[uid]
    try:
        while True:
            msg = await q.get()
            if msg is None: break
            async with locks[uid]:
                user = get_user(uid)
                metrics["requests"] += 1
                action = route(user, msg)
                record_action(action)
                response = await execute(uid, action, msg)
                if response: await send(uid, response)
    except Exception as e: log_event("worker_crash", uid=uid, error=str(e)[:500])
    finally: log_event("worker_stopped", uid=uid)

def enqueue(uid: int, text: str):
    if len(workers) > MAX_WORKERS and uid not in workers: return
    if uid not in queues: queues[uid] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE); locks[uid] = asyncio.Lock()
    q = queues[uid]
    if q.full():
        try: q.get_nowait(); record_error("queue_dropped")
        except: pass
    try: q.put_nowait(text)
    except: record_error("queue_full"); return
    async def ensure_worker():
        async with locks[uid]:
            try: asyncio.get_running_loop()
            except: return
            if uid not in workers or workers[uid].done(): workers[uid] = asyncio.create_task(worker(uid))
    asyncio.create_task(ensure_worker())

@app.post("/webhook")
async def webhook(req: Request, x_telegram_bot_api_secret_token: str = Header(None)):
    if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET: raise HTTPException(403)
    try: data = await req.json()
    except: return {"ok": True}
    callback = data.get("callback_query")
    if callback:
        callback_id = callback.get("id")
        chat_id = callback["message"]["chat"]["id"]
        data_text = callback.get("data", "")
        await answer_callback(callback_id)
        if data_text == "/integrate": enqueue(chat_id, "/integrate")
        elif data_text == "/new": enqueue(chat_id, "/new")
        elif data_text in LENS_LIBRARY: enqueue(chat_id, data_text)
        return {"ok": True}
    msg = data.get("message") or data.get("edited_message") or {}
    if not msg: return {"ok": True}
    update_id = data.get("update_id")
    if update_id and dedup(update_id): return {"ok": True}
    chat_id = msg["chat"]["id"]
    save_user_to_file(chat_id)
    if chat_id == ADMIN_ID:
        photo = msg.get("photo"); voice = msg.get("voice"); caption = msg.get("caption", "")
        if photo: save_broadcast_media("photo", photo[-1]["file_id"], caption); asyncio.create_task(send(chat_id, "✅ Фото сохранено.")); return {"ok": True}
        if voice: save_broadcast_media("voice", voice["file_id"], caption); asyncio.create_task(send(chat_id, "✅ Голосовое сохранено.")); return {"ok": True}
    text = msg.get("text", "")
    if chat_id == ADMIN_ID and text == "/send_all":
        media = load_broadcast_media()
        if not media: asyncio.create_task(send(chat_id, "❌ Нет медиа."))
        else:
            metrics["broadcasts"] += 1
            count = await broadcast_to_all(media)
            asyncio.create_task(send(chat_id, f"✅ Отправлено: {count}"))
        return {"ok": True}
    if text: enqueue(chat_id, text.strip())
    return {"ok": True}

@app.get("/health")
async def health():
    return {"status": "ok", "users": len(users), "workers": len(workers), "metrics": metrics}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")), workers=1, log_level="info")
