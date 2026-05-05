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
UNIFIED_MAX_TOKENS = 1800
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

# v14.2: Timer-based save — не создаёт новый поток при каждом вызове
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

# v14.2: batch update — один lock вместо трёх
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
        log(f"Telegram API error: {r.status_code}")
    except Exception as e:
        log(f"Telegram API exception: {type(e).__name__}")
    return None

def safe_llm_call(messages, temp=0.7, max_tokens=1200) -> str:
    if not VSEGPT_API_KEY:
        return "API key not configured."
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
    return "Model temporarily unavailable."

# ================= DEDUP =================
# v14.2: periodic dedup cleanup by TTL (not just size)
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
# v14.2: uid передаётся явно — нет зависимости от _uid
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
    return None if not r or r.startswith("API") or r.startswith("Model") else r

# ================= AUTO-ROUTER =================
AUTO_CLASSIFY_PROMPT = (
    "Classify the experience into ONE category and ONE lens.\n"
    "Categories: science, depth, esoteric, symbolic, consciousness, structure, presence, action.\n"
    "Return ONLY JSON: {\"category\": \"...\", \"lens\": \"...\"}\n\n"
    "Available lenses per category:\n"
    "- science: neuro, cbt\n"
    "- depth: jung, parts, conflict, relational\n"
    "- esoteric: shaman, yoga\n"
    "- symbolic: tarot\n"
    "- consciousness: hindu, witness\n"
    "- structure: architect, field, temporal, social_field\n"
    "- presence: stalker\n"
    "- action: action"
)

def auto_select_lens(text: str) -> tuple:
    """Returns (category_id, lens_key) using LLM classifier with keyword fallback"""
    result = safe_llm([
        {"role": "system", "content": AUTO_CLASSIFY_PROMPT},
        {"role": "user", "content": text[:800]}
    ], max_tokens=100, temp=0.2)

    if result:
        try:
            # v14.2: очистка JSON от маркдаун-обёртки
            clean = re.sub(r"```json|```", "", result).strip()
            data = json.loads(clean)
            cat = data.get("category", "science")
            lens = data.get("lens", "neuro")
            if cat in LENS_CATEGORIES and lens in LENS_LIBRARY:
                return cat, lens
        except:
            pass

    # Fallback to keyword matching
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
            "Create a brief psychological profile. "
            "Describe patterns of perception: emotional state, bodily reactions, "
            "cognitive style, recurring themes. Maximum 4 lines. No fluff."
        )},
        {"role": "user", "content": f"NEW EXPERIENCE:\n{experience}\n\nLAST ANSWER:\n{answer}\n\nHISTORY:\n{history_text}"}
    ]

    summary = safe_llm(prompt, max_tokens=120, temp=0.3)
    if summary:
        update_user(uid, lambda u: u.__setitem__("user_summary", summary))

# ================= PROMPTS =================
UNIFIED_INTERPRETATION_PROMPT = (
    "You are a guide for phenomenological exploration of experience. "
    "Your task is to help a person see their experience more clearly.\n\n"

    "STRUCTURE OF RESPONSE:\n\n"

    "1. SCIENTIFIC EXPLANATION\n"
    "- Explain through the brain, nervous system, and body.\n"
    "- Use terms in PARENTHESES: amygdala, dopamine, cortisol, vagus nerve.\n"
    "- Cover ALL significant images and sensations from the experience.\n\n"

    "2. MEANINGFUL TRANSITION\n"
    "- Gently point out that experience is not only explained but also lived.\n\n"

    "3. IDENTIFY THE BRIGHTEST MOMENT\n"
    "- Find the most vivid, emotionally charged image or sensation.\n"
    "- Name it directly.\n\n"

    "4. QUESTION TO THE EXPERIENCE\n"
    "- Ask: what does THIS specific image or sensation mean to THEM?\n"
    "- What is THEIR answer to what this image is about?\n"
    "- The question must point to the brightest moment you identified.\n\n"

    "STYLE: Calm, scientific, without mysticism. Without pressure. Speak Russian."
)

CONTINUATION_PROMPT = (
    "You are continuing the exploration. A person has answered your question.\n\n"
    "1. REFLECT the person's answer — show that you heard.\n"
    "2. DEEPEN — find what points to a deeper layer.\n"
    "3. ASK the next question — specific, from their answer.\n"
    "Without interpretations. Without evaluations. Speak Russian."
)

PNI_DEEP_PROMPT = (
    "You are a psychoneuroimmunologist. Explain through the connection "
    "of psyche, nervous system and immunity. Simple language, 6-8 sentences. Speak Russian."
)

DEEP_PATTERNS = [
    "What in this experience still remains unfinished?",
    "Where does it feel in you right now?",
    "What would completion of this look like for you?",
    "What in this did you not allow yourself to fully?",
    "If this could be expressed in one word — what would it be?",
    "What would change if this process completed as you wanted?",
    "Which part of yourself do you recognize in this experience?",
]

# ================= LENS LIBRARY =================
LENS_LIBRARY = {
    "neuro": {
        "name": "Neurophysiology",
        "category": "science",
        "prompt": (
            "You are a neurophysiologist. Explain ANY experience through the brain and nervous system.\n"
            "Use predictive coding and interoception concepts.\n"
            "You DO NOT SEE: symbols, spiritual meaning, archetypes.\n"
            "You DO NOT GIVE: actions, advice, spiritual interpretations.\n"
            "FORMAT: Explanation. 7-9 sentences. Scientific but understandable. Terms in parentheses.\n"
            "END WITH: 'From a neurophysiological perspective, your experience is not random — it is the work of specific systems.'\n"
            "Speak Russian."
        )
    },
    "cbt": {
        "name": "CBT",
        "category": "science",
        "prompt": (
            "You are a CBT therapist. Work ONLY with thoughts and behavior.\n"
            "Use cognitive distortions: catastrophizing, should-statements, mind-reading.\n"
            "Use behavioral avoidance and exposure concepts.\n"
            "You DO NOT SEE: archetypes, spiritual explanations, symbols.\n"
            "You DO NOT GIVE: deep meanings, archetypal interpretations.\n"
            "FORMAT: Tool with reality check. 6-8 sentences. Concrete. No philosophy.\n"
            "END WITH: 'This is not truth — it is a habit of mind. And habits can be changed. Right now.'\n"
            "Speak Russian."
        )
    },
    "jung": {
        "name": "Jungian Analysis",
        "category": "depth",
        "prompt": (
            "You are a Jungian analyst. Work with archetypes, Shadow, Anima/Animus, Self.\n"
            "Use complexes, synchronicity, active imagination.\n"
            "You DO NOT SEE: biology, cognitive distortions, social conditioning.\n"
            "You DO NOT GIVE: actions, practical advice, behavioral techniques.\n"
            "FORMAT: Symbolic map. Show archetype, Shadow, individuation path.\n"
            "7-9 sentences. Deep, imagistic, not vague.\n"
            "END WITH: 'This archetype did not come by chance — it is part of your path to wholeness.'\n"
            "Speak Russian."
        )
    },
    "parts": {
        "name": "Internal Parts",
        "category": "depth",
        "prompt": (
            "You are an IFS specialist. View psyche as a system of internal parts.\n"
            "You DO NOT SEE: neurophysiology, archetypes, social roles, spiritual explanations.\n"
            "You DO NOT GIVE: actions, behavioral advice, spiritual interpretations.\n"
            "FORMAT: 1. What parts manifested (Protector, Wounded part, Critic, Child, Controller).\n"
            "2. Which part dominated. 3. Which parts conflict. 4. What the most vulnerable part really wants.\n"
            "6-8 sentences. Calm, therapeutic, without mysticism.\n"
            "END WITH: 'You are not broken. These are parts inside you that cannot hear each other.'\n"
            "Speak Russian."
        )
    },
    "conflict": {
        "name": "Internal Conflict",
        "category": "depth",
        "prompt": (
            "You are an internal conflict analyst. Find contradictions within experience.\n"
            "You DO NOT SEE: archetypes, neurophysiology, spiritual meanings, solutions.\n"
            "You DO NOT GIVE: actions, advice, spiritual interpretations.\n"
            "FORMAT: 1. What two forces or desires conflict. 2. What each side wants.\n"
            "3. Why both sides are logical and have a right to exist.\n"
            "4. What happens when the conflict is not resolved.\n"
            "6-8 sentences. Clear, analytical, without moralizing.\n"
            "END WITH: 'Conflict is not a mistake. It is a system trying to maintain balance.'\n"
            "Speak Russian."
        )
    },
    "relational": {
        "name": "Relationships",
        "category": "depth",
        "prompt": (
            "You are a relationship and attachment psychology specialist.\n"
            "Analyze through interpersonal dynamics.\n"
            "You DO NOT SEE: archetypes, neurophysiology, spiritual meanings.\n"
            "You DO NOT GIVE: actions, behavioral advice, deep interpretations.\n"
            "FORMAT: 1. What dynamic manifested (control, dependency, avoidance, fusion, rejection).\n"
            "2. What role the person took (rescuer, needy, distancer, controller).\n"
            "3. What they try to get (safety, love, recognition, control).\n"
            "4. Where is the repeating pattern from past relationships.\n"
            "6-8 sentences. Clear, psychological, no esotericism.\n"
            "END WITH: 'This is not about one person. This is about a repeating way of being in relationships.'\n"
            "Speak Russian."
        )
    },
    "shaman": {
        "name": "Shamanism",
        "category": "esoteric",
        "prompt": (
            "You are a shaman-guide. Interpret experience as a shamanic journey.\n"
            "Use soul loss, power retrieval, initiation.\n"
            "You DO NOT SEE: neurophysiology, cognitive models, psychological explanations.\n"
            "You DO NOT GIVE: scientific explanations, actions, behavioral advice.\n"
            "FORMAT: Journey. Type, spirit-helpers, Threshold Guardian, gift.\n"
            "6-8 sentences. Imaginistic, respectful to tradition.\n"
            "END WITH: 'You did not return empty-handed. What did you bring back from this journey?'\n"
            "Speak Russian."
        )
    },
    "yoga": {
        "name": "Yoga",
        "category": "esoteric",
        "prompt": (
            "You are a yoga master. Describe through chakras, prana, nadis, kundalini.\n"
            "Use samskaras, granthis, karma as pattern.\n"
            "You DO NOT SEE: neurophysiology, psychological interpretations, social context.\n"
            "You DO NOT GIVE: scientific explanations, behavioral advice.\n"
            "FORMAT: Energetic state. Add micro-action: 'Notice your breath in...'\n"
            "5-7 sentences. Poetic but structural. Sanskrit with translation.\n"
            "END WITH: 'Your subtle body speaks to you. Do you hear it?'\n"
            "Speak Russian."
        )
    },
    "tarot": {
        "name": "Tarot",
        "category": "symbolic",
        "prompt": (
            "You are a Tarot master. View experience through Major Arcana.\n"
            "Use Fool's Journey, shadow of the Arcana.\n"
            "You DO NOT SEE: scientific explanations, cognitive models, linear causality.\n"
            "You DO NOT GIVE: actions, behavioral advice, scientific interpretations.\n"
            "FORMAT: Card + Transition. One Arcana, justification, message, lesson.\n"
            "Binding to details MANDATORY. Remove generalizations.\n"
            "6-8 sentences. Symbolic but precise.\n"
            "END WITH: 'This Arcana is a mirror of your process. What do you see in it?'\n"
            "Speak Russian."
        )
    },
    "hindu": {
        "name": "Advaita",
        "category": "consciousness",
        "prompt": (
            "You are an Advaita Vedanta teacher. Point to the Witness.\n"
            "Use false identification, 'I AM', distinction between experience and Presence.\n"
            "You DO NOT SEE: personality, psychology, biology, meanings and goals.\n"
            "You DO NOT GIVE: explanations, actions, psychological interpretations.\n"
            "FORMAT: Pointer. Where is illusion, where is Witness, what is temporary, what is unchanging.\n"
            "Less explanation. More pointing.\n"
            "4-6 sentences. Simple. Deep.\n"
            "END WITH: 'The one who sees this experience — is greater than the experience. Who is it?'\n"
            "Speak Russian."
        )
    },
    "witness": {
        "name": "Observer",
        "category": "consciousness",
        "prompt": None,
        "static_text": (
            "Whatever you experience — it is noticed.\n\n"
            "You see fear? Then you are not the fear.\n"
            "You see a thought? Then you are not the thought.\n"
            "You see a body? Then you are not the body.\n\n"
            "That which sees — cannot be that which is seen.\n\n"
            "Who is reading this text?\n\n"
            "Quiet. No one hides in the answers."
        )
    },
    "stalker": {
        "name": "Stalker",
        "category": "presence",
        "prompt": (
            "You are silent presence, a mirror without reflections.\n"
            "Your only function: CUT mental constructions.\n"
            "You DO NOT SEE: content, emotions, meanings, person.\n"
            "You DO NOT GIVE: explanations, comfort, advice, interpretations.\n"
            "FORMAT: Koan. 2-4 lines. More pauses. Fewer words. More emptiness.\n"
            "FORBIDDEN: 'I understand', 'you need', 'you achieved', any evaluations, comfort.\n"
            "ALLOWED: 'Whatever you experience — it is noticed', 'Who sees this right now?'\n"
            "Speak Russian."
        )
    },
    "architect": {
        "name": "Architect",
        "category": "structure",
        "prompt": (
            "You are the Architect of consciousness. Reveal structure.\n"
            "Use: Axis, Horizon, Fracture, Bridge, Deformation.\n"
            "You DO NOT SEE: emotions as experiences, spiritual meaning, psychology.\n"
            "You DO NOT GIVE: actions, emotional support, spiritual interpretations.\n"
            "FORMAT: Formula. 4 layers + Architectural Formula: 'Your Axis — ... Boundary — ... Bridge — ...'\n"
            "Only assertions. Remove 'maybe'.\n"
            "END WITH: 'Does this construction stand?'\n"
            "Speak Russian."
        )
    },
    "field": {
        "name": "Field",
        "category": "structure",
        "prompt": (
            "You are the voice of the Field. Show how reality assembles from emptiness.\n"
            "Use: interference, node, phase shift, fixation, grid.\n"
            "You DO NOT SEE: person, emotions, meanings, psychology.\n"
            "You DO NOT GIVE: explanations, advice, interpretations.\n"
            "FORMAT: Poetry of structure. Short lines. Rhythm through spaces.\n"
            "Break logic. Less linearity. 5-7 lines.\n"
            "BEGIN: 'Not within form, deeper than the layer where form itself is only permitted.'\n"
            "END WITH: 'This is a model. Do you want to see who is observing all of this?'\n"
            "Speak Russian."
        )
    },
    "temporal": {
        "name": "Time Perspective",
        "category": "structure",
        "prompt": (
            "You are a life trajectory analyst. View experience in the context of time.\n"
            "You DO NOT SEE: archetypes, neurophysiology, energetic processes, internal parts.\n"
            "You DO NOT GIVE: actions, advice, spiritual interpretations.\n"
            "FORMAT: 1. Is this past pattern, present crisis, or future transition.\n"
            "2. How it connects to previous similar situations.\n"
            "3. Where this experience potentially leads.\n"
            "4. What choice now shapes the future trajectory.\n"
            "6-8 sentences. Calm, strategic, without predictions as fact.\n"
            "END WITH: 'You are not at a point of experience. You are at a fork in the trajectory.'\n"
            "Speak Russian."
        )
    },
    "social_field": {
        "name": "Social Field",
        "category": "structure",
        "prompt": (
            "You are a social systems analyst. View experience as part of a field of human interactions.\n"
            "You DO NOT SEE: archetypes, neurophysiology, internal parts, spiritual meanings.\n"
            "You DO NOT GIVE: actions, advice, deep psychological interpretations.\n"
            "FORMAT: 1. What social field is activated (work, family, relationships, status).\n"
            "2. What roles the person performs. 3. What expectations of others influence the experience.\n"
            "4. Where the person loses themselves in the social context.\n"
            "6-8 sentences. Structural, systemic, without psychologizing to personality.\n"
            "END WITH: 'This is not only your inner experience. This is a reaction to the field you are in.'\n"
            "Speak Russian."
        )
    },
    "action": {
        "name": "Action",
        "category": "action",
        "prompt": (
            "You are a behavior engineer. Translate experience into action.\n"
            "You DO NOT SEE: deep meanings, archetypes, neurophysiology, spiritual explanations.\n"
            "You DO NOT GIVE: interpretations, analysis, explanations of meaning.\n"
            "FORMAT: 1. What is happening in the system (short). 2. One key behavioral change needed.\n"
            "3. One micro-action (under 30 seconds) to do immediately.\n"
            "4. What will change after this action (concrete, observable).\n"
            "5-7 sentences. Practical, direct, no philosophy.\n"
            "END WITH: 'Do it now. Stop analyzing.'\n"
            "Speak Russian."
        )
    },
}

# ================= CATEGORIES =================
LENS_CATEGORIES = {
    "science": "Science and Psychology",
    "depth": "Deep Psychology",
    "esoteric": "Esotericism and Energy",
    "symbolic": "Symbolic Systems",
    "consciousness": "Consciousness and Non-duality",
    "structure": "Structure and Models",
    "presence": "Radical Presence",
    "action": "Action and Application",
}

# ================= KEYBOARDS =================
def build_start_keyboard():
    return {"inline_keyboard": [
        [{"text": "Auto mode", "callback_data": "mode:auto"}],
        [{"text": "Choose direction", "callback_data": "mode:categories"}],
        [{"text": "Full menu", "callback_data": "self_inquiry:lenses"}],
    ]}

def build_categories_keyboard():
    return {"inline_keyboard": [
        [{"text": "Science and Psychology", "callback_data": "cat:science"}],
        [{"text": "Deep Psychology", "callback_data": "cat:depth"}],
        [{"text": "Esotericism and Energy", "callback_data": "cat:esoteric"}],
        [{"text": "Symbolic Systems", "callback_data": "cat:symbolic"}],
        [{"text": "Consciousness and Non-duality", "callback_data": "cat:consciousness"}],
        [{"text": "Structure and Models", "callback_data": "cat:structure"}],
        [{"text": "Radical Presence", "callback_data": "cat:presence"}],
        [{"text": "Action and Application", "callback_data": "cat:action"}],
    ]}

def build_lenses_keyboard(category_id: str):
    lenses = [k for k, v in LENS_LIBRARY.items() if v.get("category") == category_id]
    if len(lenses) > MAX_LENSES_PER_CATEGORY:
        lenses = lenses[:MAX_LENSES_PER_CATEGORY]
    rows = []
    for i in range(0, len(lenses), 2):
        row = [{"text": LENS_LIBRARY[l]["name"], "callback_data": f"lens:{l}"} for l in lenses[i:i+2]]
        rows.append(row)
    rows.append([{"text": "<< Back to categories", "callback_data": "mode:categories"}])
    return {"inline_keyboard": rows}

def build_entry_keyboard():
    return {"inline_keyboard": [
        [{"text": "Answer and deepen", "callback_data": "self_inquiry:answer"}],
        [{"text": "Look from another angle", "callback_data": "self_inquiry:lenses"}],
    ]}

def build_full_lenses_keyboard():
    return {"inline_keyboard": [
        [{"text": "Neuro", "callback_data": "lens:neuro"},
         {"text": "CBT", "callback_data": "lens:cbt"}],
        [{"text": "Jung", "callback_data": "lens:jung"},
         {"text": "Shaman", "callback_data": "lens:shaman"}],
        [{"text": "Tarot", "callback_data": "lens:tarot"},
         {"text": "Yoga", "callback_data": "lens:yoga"}],
        [{"text": "Advaita", "callback_data": "lens:hindu"},
         {"text": "Field", "callback_data": "lens:field"}],
        [{"text": "Observer", "callback_data": "lens:witness"},
         {"text": "Stalker", "callback_data": "lens:stalker"}],
        [{"text": "Architect", "callback_data": "lens:architect"},
         {"text": "Action", "callback_data": "lens:action"}],
        [{"text": "Relationships", "callback_data": "lens:relational"},
         {"text": "Internal Parts", "callback_data": "lens:parts"}],
        [{"text": "Time Perspective", "callback_data": "lens:temporal"},
         {"text": "Conflict", "callback_data": "lens:conflict"}],
        [{"text": "Social Field", "callback_data": "lens:social_field"}],
        [{"text": "PNI View", "callback_data": "self_inquiry:pni"}],
        [{"text": "New experience", "callback_data": "reset"},
         {"text": "Finish", "callback_data": "self_inquiry:end"}],
    ]}

def build_continue_keyboard():
    return {"inline_keyboard": [
        [{"text": "Continue deeper", "callback_data": "self_inquiry:deep"}],
        [{"text": "Look through a lens", "callback_data": "self_inquiry:lenses"}],
        [{"text": "New experience", "callback_data": "reset"},
         {"text": "Finish", "callback_data": "self_inquiry:end"}],
    ]}

# ================= ANTI-REPEAT =================
STOP_WORDS = {"now", "right", "for you", "in this", "this", "you", "your",
              "how", "what", "where", "when", "why"}

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
            {"role": "system", "content": "Ask ONE deep question. Be unique. Speak Russian."},
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
        system_prompt += f"\n\nUSER SUMMARY:\n{summary}"

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": experience[:EXPERIENCE_SWEET_SPOT]}
    ]

    result = safe_llm(messages, max_tokens=UNIFIED_MAX_TOKENS, temp=0.35) or (
        "This experience activates the system of internal reaction and attention. "
        "Try to notice where it lives in the body right now."
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
        return "Tell me more — what do you feel?"

    system_prompt = CONTINUATION_PROMPT

    if summary:
        system_prompt += f"\n\nUSER SUMMARY:\n{summary}"

    result = safe_llm([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": (
            f"Initial experience: {experience[:800]}\n\n"
            f"I asked: {question}\n\n"
            f"The person answered: {answer}\n\n"
            f"Continue the exploration."
        )}
    ], max_tokens=800, temp=0.5) or "Thank you for your answer. What else do you notice in this experience?"

    return ensure_complete_sentence(result)

def run_lens(lens_key: str, experience: str, user: dict = None) -> str:
    lens = LENS_LIBRARY.get(lens_key, {})
    if not lens:
        return "Lens not found."

    if lens.get("static_text"):
        return lens["static_text"]

    prompt = lens.get("prompt", "")
    if not prompt:
        return "Lens not configured."

    result = safe_llm([
        {"role": "system", "content": prompt},
        {"role": "user", "content": experience[:EXPERIENCE_SWEET_SPOT]}
    ], max_tokens=LENS_MAX_TOKENS, temp=0.7) or "Failed to apply lens."

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
        "text": "I am a guide.\n\nDescribe what you experienced — I will help you understand it through science and meaning.",
        "keyboard": build_start_keyboard()
    }

def handle_reject_short() -> dict:
    return {"text": "Describe in a bit more detail.\n\nWhat did you feel? What was happening in your body?"}

def handle_unified(uid: int, text: str) -> dict:
    user = get_user(uid)
    # v14.2: batch update — один lock вместо трёх
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
        "text": f"{result}\n\nYou can continue or choose another direction.",
        "keyboard": build_continue_keyboard()
    }

def handle_deep(uid: int, user: dict) -> dict:
    update_user(uid, lambda u: u.__setitem__("deep_count", u.get("deep_count", 0) + 1))
    depth = user.get("deep_count", 1)
    pool = DEEP_PATTERNS[:3] if depth <= 2 else DEEP_PATTERNS[2:5] if depth <= 4 else DEEP_PATTERNS[3:]
    question = generate_unique_question(user, pool, uid)
    return {
        "text": f"{question}\n\nYou can answer or simply choose where to go next.",
        "keyboard": build_continue_keyboard()
    }

def handle_pni(user: dict, uid: int) -> dict:
    if not user.get("last_experience"):
        return {"text": "First describe the experience."}
    update_user(uid, lambda u: u.__setitem__("state", STATE_PNI))
    result = safe_llm([
        {"role": "system", "content": PNI_DEEP_PROMPT},
        {"role": "user", "content": user["last_experience"][:1000]}
    ], max_tokens=PNI_MAX_TOKENS, temp=0.5) or (
        "The nervous system activates cortisol and adrenaline. "
        "This affects immune cells. After resolution — the recovery phase."
    )
    return {
        "text": (
            f"PSYCHONEUROIMMUNOLOGY VIEW\n\n"
            f"{ensure_complete_sentence(result)}\n\n"
            f"---\n\n"
            f"This is a view through the lens of body-mind-immune connection."
        ),
        "keyboard": build_continue_keyboard()
    }

def handle_lens(user: dict, uid: int, lens_key: str) -> dict:
    if not user.get("last_experience"):
        return {"text": "First describe the experience."}

    result = run_lens(lens_key, user["last_experience"], user)

    update_user(uid, lambda u: u.setdefault("used_lenses", []).append(lens_key))
    schedule_save()

    name = LENS_LIBRARY.get(lens_key, {}).get("name", lens_key)
    return {
        "text": f"Looking through '{name}'.\n\n{ensure_complete_sentence(result)}",
        "keyboard": build_continue_keyboard()
    }

def handle_end(uid: int, user: dict) -> dict:
    ic = len(user.get("identity_story", []))
    dc = user.get("deep_count", 0)
    reset_user(uid)
    return {"text": f"Cycle completed.\n\nYou deepened {dc} time(s). Total — {ic} experiences."}

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
    if action == "reset_state": reset_user(uid); schedule_save(); return {"text": "Space cleared. Describe a new experience."}
    if action == "reject_short": return handle_reject_short()
    if action == "unified": return handle_unified(uid, text)
    if action == "user_answer": return handle_user_answer(uid, text)
    return None

def execute_callback(uid: int, action: str) -> dict | None:
    user = get_user(uid)

    if action == "mode:auto":
        if not user.get("last_experience"):
            return {"text": "First describe your experience, then I will automatically select a lens."}
        category_id, lens_key = auto_select_lens(user["last_experience"])
        result = run_lens(lens_key, user["last_experience"], user)
        name = LENS_LIBRARY.get(lens_key, {}).get("name", lens_key)
        cat_name = LENS_CATEGORIES.get(category_id, category_id)
        update_user(uid, lambda u: u.setdefault("used_lenses", []).append(lens_key))
        schedule_save()
        return {
            "text": f"Auto-selected: {cat_name} > '{name}'.\n\n{ensure_complete_sentence(result)}",
            "keyboard": build_continue_keyboard()
        }

    if action == "mode:categories":
        return {"text": "Choose a direction:", "keyboard": build_categories_keyboard()}

    if action.startswith("cat:"):
        category_id = action.replace("cat:", "")
        update_user(uid, lambda u: u.__setitem__("selected_category", category_id))
        return {
            "text": f"Category: {LENS_CATEGORIES.get(category_id, category_id)}\nChoose a lens:",
            "keyboard": build_lenses_keyboard(category_id)
        }

    if action == "reset": reset_user(uid); schedule_save(); return {"text": "Space cleared. Describe a new experience."}
    if action == "self_inquiry:deep": return handle_deep(uid, user)
    if action == "self_inquiry:pni": return handle_pni(user, uid)
    if action == "self_inquiry:end": return handle_end(uid, user)

    if action == "self_inquiry:answer":
        update_user(uid, lambda u: u.__setitem__("state", STATE_AWAIT_ANSWER))
        question = user.get("last_bot_question", "Tell me more — what do you feel?")
        return {"text": question, "keyboard": build_continue_keyboard()}

    if action == "self_inquiry:lenses":
        return {"text": "Choose a lens:", "keyboard": build_full_lenses_keyboard()}

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
    # v14.2: явная передача uid
    if is_rate_limited(chat_id, user): send_long_message(chat_id, "Wait a second..."); return
    action = route_message(user, text)
    log(f"[MSG] uid={chat_id} action={action}")
    if action in ("unified", "user_answer"): send_long_message(chat_id, "...looking at this")
    r = execute_message(chat_id, action, text)
    if r: send_long_message(chat_id, r.get("text", ""), r.get("keyboard"))

def process_callback(chat_id: int, data: str) -> None:
    user = get_user(chat_id)
    if not data: return
    # v14.2: явная передача uid
    if is_rate_limited(chat_id, user): send_long_message(chat_id, "Wait a second..."); return
    action = route_callback(data)
    if not action: return
    log(f"[CB] uid={chat_id} action={action}")
    if action in ("self_inquiry:deep", "self_inquiry:pni", "self_inquiry:answer", "self_inquiry:lenses") or action.startswith("lens:") or action.startswith("cat:") or action.startswith("mode:"):
        send_long_message(chat_id, "...looking deeper")
    r = execute_callback(chat_id, action)
    if r: send_long_message(chat_id, r.get("text", ""), r.get("keyboard"))

# ================= WEBHOOK =================
class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/14.2"
    def _send_json(self, code, payload):
        d = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(d)))
        self.end_headers()
        self.wfile.write(d)
    def do_GET(self):
        self._send_json(200, {"ok": True, "service": "shaman-bot", "version": "14.2", "users": len(users)}) if self.path in ("/", "/health") else self._send_json(404, {"error": "Not found"})
    def do_POST(self):
        if self.path != "/webhook": return self._send_json(404, {"error": "Not found"})
        if WEBHOOK_SECRET and self.headers.get("X-Telegram-Bot-Api-Secret-Token", "") != WEBHOOK_SECRET:
            return self._send_json(403, {"error": "Invalid webhook secret"})
        try:
            # v14.2: ограничение размера body
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
    log(f"ShamanBot v14.2 STABLE on {HOST}:{PORT}")
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
