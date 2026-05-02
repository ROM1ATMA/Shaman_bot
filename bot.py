import json
import os
import sys
import time
import re
import random
import html
import traceback
import httpx
from urllib.parse import quote
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

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

MIRROR_MAX_TOKENS = 600
MAX_TELEGRAM_CHARS = 3900
EXPERIENCE_SWEET_SPOT = 800
MIN_EXPERIENCE_LENGTH = 15

# 🔒 NEW: Launch-safe config
RATE_LIMIT_SECONDS = 2
DUPLICATE_TTL = 5
THINKING_MESSAGE = "…смотрю на это"

# ================= LOGGING =================
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def log(message: str) -> None:
    print(f"[{utc_now()}] {message}", flush=True)

# ================= STATE MACHINE =================
STATE_IDLE = "idle"
STATE_CONFLICT = "conflict"
STATE_REWRITE = "rewrite"
STATE_DEEP = "deep"

users = {}

# ================= USERS =================
def load_users():
    global users
    try:
        if os.path.exists("data/users.json"):
            with open("data/users.json", "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for uid, data in loaded.items():
                    users[int(uid)] = data
            log(f"Loaded {len(users)} users")
    except Exception as e:
        log(f"Load users error: {e}")

def save_users_sync():
    os.makedirs("data", exist_ok=True)
    with open("data/users.json", "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)

def get_user(uid: int) -> dict:
    uid = int(uid)
    if uid not in users:
        users[uid] = {
            "state": STATE_IDLE,
            "last_experience": "",
            "last_active": time.time(),
            "last_key_moment": "",
            "used_lenses": [],
            "returning_user": False,
            "chosen_view": "",
            "identity_story": [],
            "rewrite_history": [],
            "_conflict_views": {},
            "_conflict_moment": "",
            # 🔒 NEW: rate limit + dedup
            "last_request_time": 0,
            "last_update_hash": "",
            "user_vector": {"depth": 5, "clarity": 5, "resistance": 5, "stability": 5},
        }
        save_users_sync()
    users[uid]["last_active"] = time.time()
    return users[uid]

# ================= HTTP CLIENTS =================
telegram_client = httpx.Client(timeout=30)
llm_client = httpx.Client(timeout=65)

# ================= TELEGRAM API =================
def telegram_api(method: str, payload: dict) -> dict | None:
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

def send_message(chat_id: int, text: str, keyboard: dict = None) -> bool:
    text = safe_text(text)
    payload = {"chat_id": chat_id, "text": text}
    if keyboard:
        payload["reply_markup"] = keyboard
    result = telegram_api("sendMessage", payload)
    return result is not None and result.get("ok", False)

def answer_callback(callback_id: str) -> None:
    telegram_api("answerCallbackQuery", {"callback_query_id": callback_id})

# ================= UTILS =================
def safe_text(text: str) -> str:
    """Экранируем HTML и режем под Telegram"""
    text = html.escape(text or "")
    if len(text) > MAX_TELEGRAM_CHARS:
        return text[:MAX_TELEGRAM_CHARS]
    return text

# 🔒 NEW: Защита от спама и дубликатов
def is_rate_limited(user: dict) -> bool:
    now = time.time()
    if now - user.get("last_request_time", 0) < RATE_LIMIT_SECONDS:
        return True
    user["last_request_time"] = now
    return False

def is_duplicate(user: dict, text: str) -> bool:
    h = str(hash(text.strip()))
    if user.get("last_update_hash") == h:
        return True
    user["last_update_hash"] = h
    return False

# ================= LLM =================
def call_llm_sync(messages, temp=0.7, max_tokens=1200, user=None) -> str:
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
        except Exception as e:
            log(f"LLM error: {e}")
        
        if attempt < 1:
            time.sleep(1)
    
    return "⚠️ Модель временно недоступна."

def safe_llm(messages, user=None, **kwargs) -> str | None:
    """Единая обёртка с fallback"""
    try:
        result = call_llm_sync(messages, user=user, **kwargs)
        if not result or "⚠️" in result:
            return None
        return result
    except Exception as e:
        log(f"LLM CRASH: {e}")
        return None

def send_thinking(chat_id: int) -> None:
    """UX: показываем что бот думает"""
    send_message(chat_id, THINKING_MESSAGE)

# ================= PROMPTS =================
KEY_MOMENT_PROMPT = (
    "Найди в опыте момент внутреннего напряжения или незавершённости.\n\n"
    "Верни ОДНУ фразу (до 12 слов) — максимально конкретно.\n"
    "Только фраза."
)

MIRROR_PROMPT_V3 = (
    "Ты возвращаешь человека ВНУТРЬ его опыта.\n"
    "1. УЗНАВАНИЕ (2–3 предл.) — опиши что происходило, используй 'ты'.\n"
    "2. НАПРЯЖЕНИЕ (2–4 предл.) — покажи момент где что-то НЕ ДОШЛО.\n"
    "3. УГЛУБЛЕНИЕ (2–3 предл.) — мягко предположи что под этим.\n"
    "Стиль: живой русский, без терминов, без советов, без вопросов."
)

NEURO_PROMPT = (
    "Ты нейрофизиолог. Объясни этот опыт через тело и мозг: процессы, волны, нейромедиаторы. "
    "Коротко, ясно, без терминов."
)

JUNG_PROMPT = (
    "Ты юнгианский аналитик. Интерпретируй образы как архетипы. "
    "Покажи связь между ними. Используй: 'похоже на', 'может указывать'."
)

REWRITE_SYSTEM_A = (
    "Ты объясняешь опыт через тело, нервную систему и реакции. "
    "Сформируй вывод: если смотреть через эту версию, кто человек в этом опыте? "
    "Начни с: «Если смотреть отсюда, ты — тот, кто...»"
)

REWRITE_SYSTEM_B = (
    "Ты объясняешь опыт через смысл, архетипы и внутренние сюжеты. "
    "Сформируй вывод: если смотреть через эту версию, кто человек в этом опыте? "
    "Начни с: «Если смотреть отсюда, ты — тот, кто...»"
)

# ================= KEYBOARDS =================
def build_conflict_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🔬 А — это про тело", "callback_data": "choice_A"}],
        [{"text": "🏺 B — это про смысл", "callback_data": "choice_B"}],
    ]}

def build_rewrite_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🕳 Углубиться", "callback_data": "self_inquiry:deep"}],
        [{"text": "🔄 Новый опыт", "callback_data": "reset"}],
        [{"text": "🌿 Завершить", "callback_data": "self_inquiry:end"}],
    ]}

# ================= HOOK ENGINE =================
def extract_key_moment(text: str, user: dict = None) -> str:
    result = safe_llm([
        {"role": "system", "content": KEY_MOMENT_PROMPT},
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

def identity_hook(moment: str) -> str:
    hooks = [
        f"Ты уже не просто наблюдаешь это — ты выбираешь, кем становишься внутри {moment}.",
        f"После этого выбора ты не сможешь воспринимать опыт так же.",
        f"Обе версии тебя уже существуют — вопрос в том, какую ты усилишь.",
        f"Ты не смотришь на опыт. Ты собираешь себя из него.",
        f"Это не просто интерпретация. Это точка, где ты решаешь, кто ты.",
    ]
    return random.choice(hooks)

# ================= IDENTITY REWRITE ENGINE =================
def build_choice_block(conflict: dict, moment: str) -> str:
    return (
        "⚖️ ДВЕ ВЕРСИИ ОДНОГО ОПЫТА\n\n"

        "🔬 А — ТЕЛЕСНАЯ / НЕЙРОФИЗИОЛОГИЧЕСКАЯ:\n"
        f"{conflict['neuro'][:400]}\n\n"

        "🏺 B — СМЫСЛОВАЯ / АРХЕТИПИЧЕСКАЯ:\n"
        f"{conflict['jung'][:400]}\n\n"

        "────────────────────\n\n"

        "Выбери, что ближе к тебе прямо сейчас:\n\n"

        "А — это про тело, реакцию, нервную систему\n"
        "B — это про символ, смысл, внутренний сюжет\n\n"

        "Ты не выбираешь правильное.\n"
        "Ты выбираешь, кем ты становишься в этом опыте."
    )

def generate_conflict_views(experience: str, user: dict) -> dict:
    neuro_view = safe_llm([
        {"role": "system", "content": NEURO_PROMPT},
        {"role": "user", "content": experience[:600]}
    ], max_tokens=400, temp=0.5, user=user) or "Тело здесь реагирует раньше, чем ты это осознаёшь. Напряжение — это сигнал, который не получил разрядки."

    jung_view = safe_llm([
        {"role": "system", "content": JUNG_PROMPT},
        {"role": "user", "content": experience[:600]}
    ], max_tokens=400, temp=0.5, user=user) or "Это похоже на внутренний сюжет, который повторяется. Образы — не случайны, они часть твоего пути."

    return {"neuro": neuro_view, "jung": jung_view}

def build_identity_rewrite(selected: str, experience: str, moment: str, user: dict) -> str:
    views = user.get("_conflict_views", {})

    if selected == "A":
        base = views.get("neuro", "Тело здесь — главный проводник опыта.")
        system = REWRITE_SYSTEM_A
    else:
        base = views.get("jung", "Смысл здесь — главный проводник опыта.")
        system = REWRITE_SYSTEM_B

    result = safe_llm([
        {"role": "system", "content": system},
        {"role": "user", "content": experience[:800]}
    ], max_tokens=300, temp=0.5, user=user)

    if not result:
        result = "Если смотреть через эту версию, ты действуешь из глубинной реакции, которая уже сложилась внутри тебя."

    user["rewrite_history"].append({
        "timestamp": time.time(),
        "choice": selected,
        "moment": moment,
        "identity": result[:200]
    })
    if len(user["rewrite_history"]) > 20:
        user["rewrite_history"].pop(0)

    return (
        "🧬 СБОРКА СМЫСЛА\n\n"
        f"{base}\n\n"
        "──────────────\n\n"
        f"{result}\n\n"
        "Это не факт о тебе.\n"
        "Это версия, в которую ты входишь, если смотришь отсюда.\n\n"
        "Это не истина о тебе.\n"
        "Это один из способов увидеть себя через этот опыт."
    )

# ================= CONFLICT MIRROR =================
def build_conflict_mirror(experience: str, user: dict) -> dict:
    moment = extract_key_moment(experience, user)
    user["_conflict_moment"] = moment

    base = safe_llm([
        {"role": "system", "content": MIRROR_PROMPT_V3},
        {"role": "user", "content": experience[:EXPERIENCE_SWEET_SPOT]}
    ], max_tokens=MIRROR_MAX_TOKENS, temp=0.6, user=user) or "Ты описываешь опыт, в котором есть напряжение, но оно не завершилось. Ты ждал разрядки — но что-то её не пустило."

    views = generate_conflict_views(experience, user)
    user["_conflict_views"] = views

    hook = identity_hook(moment)

    friction = (
        "Остановись на секунду.\n"
        "Что из этого ближе не логически, а по ощущению?\n\n"
    )

    result = (
        f"{base}\n\n"
        f"──────────────\n\n"
        f"{hook}\n\n"
        f"{friction}"
        f"{build_choice_block(views, moment)}"
    )

    return {
        "text": safe_text(result),
        "keyboard": build_conflict_keyboard()
    }

# ================= ROUTING =================
def route(user: dict, text: str) -> str:
    state = user["state"]
    log(f"Route: state={state} text={text[:50]}")

    if text in ("/start", "/new"):
        return "start"
    if text in ("/reset", "reset"):
        return "reset_state"

    if text in ("choice_A", "choice_B"):
        return text

    if text.startswith("self_inquiry:"):
        return text

    if state == STATE_IDLE and len(text.strip()) < MIN_EXPERIENCE_LENGTH:
        return "reject_short"

    if state == STATE_IDLE:
        return "mirror_conflict"

    if state == STATE_CONFLICT:
        return "mirror_conflict"

    if state == STATE_REWRITE:
        return "after_rewrite"

    return "mirror_conflict"

# ================= HANDLERS =================
def handle_start(user: dict) -> dict:
    user.update({
        "state": STATE_IDLE, "last_experience": "", "used_lenses": [],
        "chosen_view": "", "_conflict_views": {}, "_conflict_moment": ""
    })
    save_users_sync()
    
    if user.get("returning_user"):
        return {"text": "🌿 С возвращением.\n\nОпиши новый опыт."}
    
    return {
        "text": (
            "🌿 Я — проводник осознания.\n\n"
            "Опиши, что ты пережил.\n"
            "Я покажу тебе это с разных сторон — и ты сам решишь, кто ты в этом."
        )
    }

def handle_reject_short() -> dict:
    return {
        "text": "Опиши чуть подробнее, чтобы можно было увидеть структуру опыта.\n\nЧто ты чувствовал? Что происходило в теле?"
    }

def handle_mirror_conflict(user: dict, text: str) -> dict:
    user["last_experience"] = text[:MAX_INPUT_LENGTH]
    user["state"] = STATE_CONFLICT
    user["returning_user"] = True
    
    result = build_conflict_mirror(text, user)
    
    save_users_sync()
    
    return result

def handle_choice(user: dict, choice: str) -> dict:
    view = "A" if choice == "choice_A" else "B"
    user["chosen_view"] = view

    user["identity_story"].append({
        "timestamp": time.time(),
        "choice": view,
        "moment": user.get("_conflict_moment", "")
    })
    if len(user["identity_story"]) > 30:
        user["identity_story"].pop(0)

    save_users_sync()

    return handle_do_rewrite(user)

def handle_do_rewrite(user: dict) -> dict:
    user["state"] = STATE_REWRITE

    text = build_identity_rewrite(
        user.get("chosen_view", "A"),
        user.get("last_experience", ""),
        user.get("_conflict_moment", ""),
        user
    )

    save_users_sync()

    return {
        "text": safe_text(text),
        "keyboard": build_rewrite_keyboard()
    }

def handle_after_rewrite(user: dict) -> dict:
    identity_count = len(user.get("identity_story", []))
    
    user["state"] = STATE_IDLE
    save_users_sync()
    
    return {
        "text": (
            f"🌿 Цикл завершён.\n\n"
            f"Ты прошёл путь от опыта — к выбору — к новой сборке себя.\n\n"
            f"За всё время ты сделал {identity_count} выборов того, кто ты.\n\n"
            f"Ты можешь начать с новым опытом или побыть с тем, что сейчас."
        ),
        "keyboard": build_rewrite_keyboard()
    }

def handle_deep(user: dict) -> dict:
    user["state"] = STATE_DEEP
    
    result = safe_llm([
        {"role": "system", "content": "Задай один глубокий вопрос о том, кем человек становится через этот опыт."},
        {"role": "user", "content": user["last_experience"][:500]}
    ], max_tokens=150, temp=0.6, user=user) or "Что в этом опыте всё ещё остаётся незавершённым?"
    
    return {"text": result}

def handle_end(user: dict) -> dict:
    user["state"] = STATE_IDLE
    user["chosen_view"] = ""
    save_users_sync()
    return {"text": "🌿 Принято. Ты можешь вернуться к этому опыту в любое время."}

# ================= EXECUTE =================
def execute(uid: int, action: str, text: str) -> dict | None:
    user = get_user(uid)
    log(f"Execute: uid={uid} action={action} state={user['state']}")
    
    if action == "start": return handle_start(user)
    if action == "reset_state":
        user.update({
            "state": STATE_IDLE, "last_experience": "", "used_lenses": [],
            "chosen_view": "", "_conflict_views": {}, "_conflict_moment": ""
        })
        save_users_sync()
        return {"text": "🔄 Пространство очищено. Опиши новый опыт."}
    
    if action == "reject_short": return handle_reject_short()
    if action == "mirror_conflict": return handle_mirror_conflict(user, text)
    
    if action == "choice_A": return handle_choice(user, "choice_A")
    if action == "choice_B": return handle_choice(user, "choice_B")
    
    if action == "after_rewrite": return handle_after_rewrite(user)
    
    if action.startswith("self_inquiry:"):
        sub = action.split(":", 1)[1]
        if sub == "deep":
            return handle_deep(user)
        elif sub == "end":
            return handle_end(user)
    
    if action == "end": return handle_end(user)
    
    return None

# ================= PROCESS =================
def process_message(chat_id: int, text: str) -> None:
    user = get_user(chat_id)

    if not text:
        return

    text = text.strip()

    # 🔒 Защита от дубликатов
    if is_duplicate(user, text):
        log(f"Duplicate skipped: {chat_id}")
        return

    # 🔒 Rate limit
    if is_rate_limited(user):
        log(f"Rate limited: {chat_id}")
        return

    action = route(user, text)

    log(f"[FLOW] uid={chat_id} state={user['state']} action={action}")

    # 💭 UX: thinking indicator перед LLM-операциями
    if action in ("mirror_conflict", "choice_A", "choice_B", "do_rewrite"):
        send_thinking(chat_id)

    response = execute(chat_id, action, text)

    if response:
        send_message(chat_id, response.get("text", ""), response.get("keyboard"))

def process_callback(chat_id: int, data: str) -> None:
    user = get_user(chat_id)

    if not data:
        return

    # 🔒 Rate limit для callback'ов тоже
    if is_rate_limited(user):
        return

    action = route(user, data)

    log(f"[CALLBACK] uid={chat_id} action={action}")

    send_thinking(chat_id)

    response = execute(chat_id, action, data)

    if response:
        send_message(chat_id, response.get("text", ""), response.get("keyboard"))

# ================= WEBHOOK =================
class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/11.4.2"
    
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
                "version": "11.4.2",
                "users": len(users)
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
        
        # 🔒 Защита от падений в webhook
        try:
            update = json.loads(body) if body else {}
        except json.JSONDecodeError:
            self._send_json(400, {"ok": False, "error": "Invalid JSON"})
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
            
            message = update.get("message") or update.get("edited_message") or {}
            chat_id = message.get("chat", {}).get("id")
            text = message.get("text") or message.get("caption") or ""
            
            if chat_id and text:
                process_message(chat_id, text)
            
        except Exception as e:
            log(f"FATAL: {traceback.format_exc()}")
        
        self._send_json(200, {"ok": True})
    
    def log_message(self, format: str, *args) -> None:
        log(f"{self.client_address[0]} - {format % args}")

# ================= MAIN =================
def set_webhook(public_url: str) -> int:
    payload = {"url": f"{public_url.rstrip('/')}/webhook"}
    if WEBHOOK_SECRET:
        payload["secret_token"] = WEBHOOK_SECRET
    
    result = telegram_api("setWebhook", payload)
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
    
    server = HTTPServer((HOST, PORT), WebhookHandler)
    log(f"ShamanBot v11.4.2 on {HOST}:{PORT}")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("Shutting down...")
    finally:
        save_users_sync()
        server.server_close()
    
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
