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
from threading import Lock

# ================= CONFIG =================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
VSEGPT_API_KEY = os.getenv("VSEGPT_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN: raise RuntimeError("BOT_TOKEN empty")

VSEGPT_MODEL = "deepseek/deepseek-chat"
MAX_INPUT_LENGTH = 4000
UNIFIED_MAX_TOKENS = 1800          # Увеличено для полного разбора всех образов
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
        return "VSEGPT API key not configured."
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
    if text.endswith((".", "!", "?", "...")):
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
    return None if not r or r.startswith("VSEGPT") or r.startswith("Model") else r

# ================= USER SUMMARY ENGINE =================

def update_user_summary(uid: int, user: dict):
    experience = user.get("last_experience", "")
    answer = user.get("last_user_answer", "")
    history = user.get("identity_story", [])[-5:]
    history_text = "\n".join([h.get("experience", "") for h in history])
    
    prompt = [
        {"role": "system", "content": (
            "Create a brief psychological profile of the person. "
            "Describe not facts, but patterns of perception.\n"
            "Format: emotional state, bodily reactions, "
            "cognitive style, recurring themes.\n"
            "Maximum 4 lines. No fluff."
        )},
        {"role": "user", "content": f"NEW EXPERIENCE:\n{experience}\n\nLAST ANSWER:\n{answer}\n\nHISTORY:\n{history_text}"}
    ]
    
    summary = safe_llm(prompt, max_tokens=120, temp=0.3)
    if summary:
        update_user(uid, lambda u: u.__setitem__("user_summary", summary))

# ================= PROMPTS =================
UNIFIED_INTERPRETATION_PROMPT = (
    "You are a guide for phenomenological exploration of experience. "
    "Your task is to help a person see their experience more clearly "
    "and come into contact with what is really happening in it.\n\n"

    "STRUCTURE OF RESPONSE:\n\n"

    "1. SCIENTIFIC EXPLANATION\n"
    "- Explain through the brain, nervous system, and body.\n"
    "- Use terms in PARENTHESES: amygdala, dopamine, cortisol, vagus nerve, "
    "sympathetic/parasympathetic system, cytokines, immune response.\n"
    "- Speak simply. Normalize the experience: this is a natural reaction.\n"
    "- Cover ALL significant images and sensations from the experience.\n"
    "- Do not skip any striking detail.\n\n"

    "2. MEANINGFUL TRANSITION\n"
    "- Gently point out that experience is not only explained but also lived.\n\n"

    "3. IDENTIFY THE BRIGHTEST MOMENT\n"
    "- Find the most vivid, emotionally charged image or sensation in the experience.\n"
    "- Name it directly.\n\n"

    "4. QUESTION TO THE EXPERIENCE (MANDATORY)\n"
    "- Ask the person: what does THIS specific image or sensation mean to THEM?\n"
    "- How do they feel about it?\n"
    "- What is THEIR answer to what this image is about?\n"
    "- Ask them to reveal their own understanding deeper.\n"
    "- The question must point to the brightest moment you identified.\n"
    "- Do not ask abstract questions. Ask about the specific image.\n\n"

    "STYLE:\n"
    "- Calm, scientific, without mysticism\n"
    "- Without pressure\n"
    "- Without evaluations\n"
    "- Without markdown\n"
    "- 10-15 sentences\n"
    "- Speak Russian"
)

CONTINUATION_PROMPT = (
    "You are continuing the exploration of an experience. A person has answered your question.\n\n"
    "STRUCTURE OF RESPONSE:\n"
    "1. REFLECT the person's answer — show that you heard (1-2 sentences)\n"
    "2. DEEPEN — find what in their answer points to a deeper layer (1-2 sentences)\n"
    "3. ASK the next question — specific, from their answer (1 question)\n\n"
    "Without interpretations. Without evaluations. No 'you should'. Gently. Speak Russian."
)

PNI_DEEP_PROMPT = (
    "You are a psychoneuroimmunologist. Explain the experience through the connection "
    "of psyche, nervous system and immunity.\n"
    "Stress hormones -> immune response -> why fatigue or cleansing occurs after experiences -> "
    "what happens at the cellular level -> how the nervous system is connected to immunity.\n"
    "Simple language, terms in parentheses, 6-8 sentences. Without intimidation. Speak Russian."
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
        "name": "Neuroscience",
        "prompt": "You are a neurophysiologist. Explain the experience through the brain and nervous system. 5-7 sentences. No markdown. Speak Russian."
    },
    "cbt": {
        "name": "CBT",
        "prompt": "You are a CBT therapist. Find automatic thoughts and deep beliefs. Offer reframing. 5-7 sentences. No markdown. Speak Russian."
    },
    "jung": {
        "name": "Jungian Analysis",
        "prompt": "You are a Jungian analyst. Reveal archetypes, Shadow, Self. 6-8 sentences. No markdown. Speak Russian."
    },
    "shaman": {
        "name": "Shamanism",
        "prompt": "You are a shaman-guide. Interpret as a journey: spirits, Guardian, gift. 5-7 sentences. No markdown. Speak Russian."
    },
    "tarot": {
        "name": "Tarot",
        "prompt": "You are a Tarot master. Look through the Major Arcana. 5-7 sentences. No markdown. Speak Russian."
    },
    "yoga": {
        "name": "Yoga",
        "prompt": "You are a yoga master. Describe through chakras, prana, nadis. 5-7 sentences. No markdown. Speak Russian."
    },
    "hindu": {
        "name": "Advaita",
        "prompt": "You are an Advaita teacher. Point to the non-dual nature of experience. Where is the Witness? 4-6 sentences. No markdown. Speak Russian."
    },
    "field": {
        "name": "Field",
        "prompt": "You are the voice of the Field. Knot, grid, phase shift, interference. Short lines. 5-7 lines. No markdown. Speak Russian."
    },
    "architect": {
        "name": "Architect",
        "prompt": "You are the Architect of consciousness. Find: Axis, Fracture, Bridge. Give the Formula. No markdown. Speak Russian."
    },
    "witness": {
        "name": "Observer",
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
        "prompt": "You are silent presence. Point to the perceiving consciousness. Only impersonal pointers and koan-questions. Short. Cutting. No markdown. Speak Russian."
    },
}

# ================= KEYBOARDS (ASCII-only) =================
def build_entry_keyboard():
    return {"inline_keyboard": [
        [{"text": ">> Answer and deepen", "callback_data": "self_inquiry:answer"}],
        [{"text": ">> Look from another angle", "callback_data": "self_inquiry:lenses"}],
    ]}

def build_lenses_keyboard():
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
        [{"text": "Architect", "callback_data": "lens:architect"}],
        [{"text": "PNI View", "callback_data": "self_inquiry:pni"}],
        [{"text": "New experience", "callback_data": "reset"},
         {"text": "Finish", "callback_data": "self_inquiry:end"}],
    ]}

def build_continue_keyboard():
    return {"inline_keyboard": [
        [{"text": ">> Continue deeper", "callback_data": "self_inquiry:deep"}],
        [{"text": ">> Look through a lens", "callback_data": "self_inquiry:lenses"}],
        [{"text": ">> New experience", "callback_data": "reset"},
         {"text": ">> Finish", "callback_data": "self_inquiry:end"}],
    ]}

# ================= ANTI-REPEAT =================
STOP_WORDS = {"now", "right", "for you", "in this", "this", "you", "your",
              "how", "what", "where", "when", "why", "what for"}

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
            {"role": "system", "content": "Ask ONE deep question. Do not repeat 'what do you feel' and similar. Be unique. Speak Russian."},
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
    if any(w in t for w in ["observed", "aware", "emptiness", "disappeared", "witness", "awareness"]):
        return "nondual"
    if any(w in t for w in ["body", "tension", "breath", "heart", "pressure", "vibration"]):
        return "body"
    if any(w in t for w in ["fear", "anxiety", "pain", "shame", "panic", "pity"]):
        return "emotional"
    if any(w in t for w in ["understood", "realized", "meaning", "conclusion"]):
        return "cognitive"
    return "mixed"

# ================= ENGINE =================
def build_unified_response(experience: str, user: dict = None) -> tuple:
    user = user or {}
    exp_type = classify_experience(experience)
    summary = user.get("user_summary", "")
    
    system_prompt = UNIFIED_INTERPRETATION_PROMPT
    
    if exp_type == "emotional":
        system_prompt += "\nFOCUS: emotional regulation, limbic system, stress response."
    elif exp_type == "body":
        system_prompt += "\nFOCUS: bodily reactions, autonomic nervous system."
    elif exp_type == "cognitive":
        system_prompt += "\nFOCUS: cognitive distortions, beliefs, interpretations."
    elif exp_type == "nondual":
        system_prompt += "\nFOCUS: observing experience and distinguishing process from awareness."
    
    if summary:
        system_prompt += f"\n\nUSER SUMMARY STATE:\n{summary}"
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": experience[:EXPERIENCE_SWEET_SPOT]}
    ]
    
    result = safe_llm(messages, max_tokens=UNIFIED_MAX_TOKENS, temp=0.35) or (
        "It seems this experience activates the system of internal reaction and attention. "
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
        system_prompt += f"\n\nUSER SUMMARY STATE:\n{summary}"
    
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
        return {"text": "Welcome back.\n\nDescribe a new experience."}
    return {"text": "I am a guide.\n\nDescribe what you experienced — I will help you understand it through science and meaning."}

def handle_reject_short() -> dict:
    return {"text": "Describe in a bit more detail.\n\nWhat did you feel? What was happening in your body?"}

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
    
    lens = LENS_LIBRARY.get(lens_key, {})
    if not lens:
        return {"text": "Lens not found."}
    
    if lens.get("static_text"):
        update_user(uid, lambda u: u.setdefault("used_lenses", []).append(lens_key))
        schedule_save()
        return {"text": lens["static_text"], "keyboard": build_continue_keyboard()}
    
    prompt = lens.get("prompt", "")
    if not prompt:
        return {"text": "Lens not configured."}
    
    result = safe_llm([
        {"role": "system", "content": prompt},
        {"role": "user", "content": user["last_experience"][:EXPERIENCE_SWEET_SPOT]}
    ], max_tokens=LENS_MAX_TOKENS, temp=0.7) or "Failed to apply lens."
    
    update_user(uid, lambda u: u.setdefault("used_lenses", []).append(lens_key))
    schedule_save()
    
    name = lens.get("name", lens_key)
    return {
        "text": f"Looking through '{name}'.\n\n{ensure_complete_sentence(result)}",
        "keyboard": build_continue_keyboard()
    }

def handle_end(uid: int, user: dict) -> dict:
    ic = len(user.get("identity_story", []))
    dc = user.get("deep_count", 0)
    reset_user(uid)
    return {"text": f"Cycle completed.\n\nYou deepened {dc} time(s). Total — {ic} experiences.\n\nYou can start with a new experience or stay with what is now."}

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
    if action == "reset_state": reset_user(uid); schedule_save(); return {"text": "Space cleared. Describe a new experience."}
    if action == "reject_short": return handle_reject_short()
    if action == "unified": return handle_unified(uid, text)
    if action == "user_answer": return handle_user_answer(uid, text)
    return None

def execute_callback(uid: int, action: str) -> dict | None:
    user = get_user(uid)
    user["_uid"] = uid
    
    if action == "reset": reset_user(uid); schedule_save(); return {"text": "Space cleared. Describe a new experience."}
    if action == "self_inquiry:deep": return handle_deep(uid, user)
    if action == "self_inquiry:pni": return handle_pni(user, uid)
    if action == "self_inquiry:end": return handle_end(uid, user)
    
    if action == "self_inquiry:answer":
        update_user(uid, lambda u: u.__setitem__("state", STATE_AWAIT_ANSWER))
        question = user.get("last_bot_question", "Tell me more — what do you feel?")
        return {"text": question, "keyboard": build_continue_keyboard()}
    
    if action == "self_inquiry:lenses":
        return {"text": "Choose a lens to look at this experience:", "keyboard": build_lenses_keyboard()}
    
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
    if is_rate_limited(user): send_long_message(chat_id, "Wait a second..."); return
    action = route_message(user, text)
    log(f"[MSG] uid={chat_id} action={action}")
    if action in ("unified", "user_answer"): send_long_message(chat_id, "...looking at this")
    r = execute_message(chat_id, action, text)
    if r: send_long_message(chat_id, r.get("text", ""), r.get("keyboard"))

def process_callback(chat_id: int, data: str) -> None:
    user = get_user(chat_id)
    if not data: return
    if is_rate_limited(user): send_long_message(chat_id, "Wait a second..."); return
    action = route_callback(data)
    if not action: return
    log(f"[CB] uid={chat_id} action={action}")
    if action in ("self_inquiry:deep", "self_inquiry:pni", "self_inquiry:answer", "self_inquiry:lenses") or action.startswith("lens:"):
        send_long_message(chat_id, "...looking deeper")
    r = execute_callback(chat_id, action)
    if r: send_long_message(chat_id, r.get("text", ""), r.get("keyboard"))

# ================= WEBHOOK =================
class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/13.4"
    def _send_json(self, code, payload):
        d = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(d)))
        self.end_headers()
        self.wfile.write(d)
    def do_GET(self):
        self._send_json(200, {"ok": True, "service": "shaman-bot", "version": "13.4", "users": len(users)}) if self.path in ("/", "/health") else self._send_json(404, {"error": "Not found"})
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
    log(f"ShamanBot v13.4 FULL-RESPONSE on {HOST}:{PORT}")
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
