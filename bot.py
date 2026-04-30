import json
import os
import sys
import time
import re
import random
import traceback
import httpx
from urllib.parse import quote
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib import request, error
from collections import deque, defaultdict

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
MAX_SELF_INQUIRY_DEPTH = 5

MIRROR_MAX_TOKENS = 1000
MIRROR_MAX_CHARS = 1800
EXPERIENCE_SWEET_SPOT = 800

# ================= LOGGING =================
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def log(message: str) -> None:
    print(f"[{utc_now()}] {message}", flush=True)

def log_event(event: str, uid: int = 0, **kwargs):
    log(json.dumps({"event": event, "uid": uid, **kwargs}, ensure_ascii=False))

# ================= STATE MACHINE =================
STATE_IDLE = "idle"
STATE_REFLECTION = "reflection"
STATE_FOCUS = "focus"
STATE_EMOTION = "emotion"
STATE_PATTERN = "pattern"
STATE_SELF_INQUIRY = "self_inquiry"
STATE_DEEP = "deep"
STATE_EXPERIENCE_RETURN = "experience_return"

users = {}
user_rate_limit = {}
last_request_time = {}

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
            "used_lenses": [],
            "returning_user": False,
            "self_inquiry_depth": 0,
            "user_vector": {"depth": 5, "clarity": 5, "resistance": 5, "stability": 5},
            "guide_focus": "", "guide_emotion": "", "guide_control": "",
            "collected_patterns": [],
            "user_world": {"patterns": [], "events": [], "interpretation_nodes": []}
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
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if keyboard:
        payload["reply_markup"] = keyboard
    result = telegram_api("sendMessage", payload)
    return result is not None and result.get("ok", False)

def answer_callback(callback_id: str) -> None:
    telegram_api("answerCallbackQuery", {"callback_query_id": callback_id})

# ================= LLM =================
def call_llm_sync(messages, temp=0.7, max_tokens=1200, user=None) -> str:
    if not VSEGPT_API_KEY:
        return "⚠️ API ключ не настроен."
    
    if user:
        v = user.get("user_vector", {})
        style = "balanced"
        if v.get("resistance", 0) >= 7: style = "soft"
        elif v.get("clarity", 0) >= 8: style = "precise"
        elif v.get("depth", 0) >= 7: style = "symbolic"
        elif v.get("stability", 0) <= 3: style = "grounding"
        
        style_prompts = {
            "soft": "Говори мягко, не дави.",
            "precise": "Будь точным и структурным.",
            "symbolic": "Допускай символы и архетипы.",
            "grounding": "Фокус на теле и реальности.",
            "balanced": "Нейтральный аналитический стиль."
        }
        if style != "balanced":
            messages = [{"role": "system", "content": style_prompts[style]}] + messages
    
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

# ================= PROMPTS =================
GUIDE_REFLECTION_PROMPT = (
    "Ты — внимательный проводник.\n\n"
    "1. Коротко (2-3 предложения) отрази, что человек пережил — конкретно, без мистики.\n"
    "2. Выдели 2-3 ключевых момента опыта (телесное, эмоции, образы).\n"
    "3. Задай один простой вопрос: «Где в этом опыте было самое сильное место?»\n\n"
    "Без поэзии. Без абстракций. Ясно и по делу. Чистый русский, без маркдауна."
)

GUIDE_PATTERN_PROMPT = "Сформулируй один простой паттерн (1-2 предложения). Чистый русский, коротко."

MIRROR_PROMPT_V2 = (
    "Ты даёшь интерпретацию опыта человека через тело и глубинную психику.\n\n"

    "СТРУКТУРА ОТВЕТА (строго):\n\n"

    "1. НЕЙРОФИЗИОЛОГИЯ (3–4 предложения)\n"
    "- Опиши, как мозг и тело прошли через этот опыт: какие волны (гамма, тета, дельта) включались, "
    "как сменялись напряжение и расслабление.\n"
    "- Используй простые образы: «мозг переключился», «тело зависло между», «нервная система выдохнула».\n"
    "- Не перегружай терминами. Говори о процессе, а не об анатомии.\n"
    "- Свяжи с тем, что человек чувствовал: «ты ждал разрядки, но тело не успело».\n\n"

    "2. ЮНГИАНСКИЙ СЛОЙ (6–10 предложений — это главный блок)\n"
    "- Возьми КАЖДЫЙ значимый образ из опыта и раскрой его как архетип:\n"
    "  • Звук, колокольчик, бубен → архетип Проводника, Зовущего\n"
    "  • Вибрация, желание поднять её → архетип Трансформации, Кундалини\n"
    "  • Лягушка с монеткой → архетип Хранителя ресурса, Перехода\n"
    "  • Череп → дракон → архетип Смерти-Возрождения, Тени\n"
    "  • Бунгало, костёр → архетип Убежища, Центра\n"
    "- Покажи СВЯЗЬ между образами: как они выстраиваются в историю.\n"
    "- Используй формулировки: «похоже на», «может указывать», «как будто».\n"
    "- Говори о человеке, а не о мифах: «в тебе есть ресурс, который пока не проявлен».\n\n"

    "3. СИНТЕЗ (1–2 предложения)\n"
    "- Одной фразой: какой внутренний процесс идёт прямо сейчас.\n\n"

    "ФОРМАТ:\n"
    "Разделяй блоки заголовками с эмодзи.\n"
    "Пиши живым, тёплым русским языком.\n"
    "Общий объём: не больше 20 предложений.\n"
    "Это гипотеза, а не истина — не утверждай, а приглашай к размышлению.\n\n"

    "После интерпретации добавь ОБЯЗАТЕЛЬНЫЙ блок:\n\n"

    "4. ПРИГЛАШЕНИЕ (3–4 предложения)\n"
    "- Напомни: эти образы — сугубо индивидуальны. Только сам человек знает, что они значат.\n"
    "- Все интерпретации — лишь зеркало. Что-то откликнется, что-то нет.\n"
    "- Задай ОДИН уточняющий вопрос про конкретный момент из опыта — "
    "про чувство, телесное ощущение или образ, где не хватает ясности.\n"
    "- Предложи: «Если хочешь — можем разобрать это глубже. Или посмотреть через другую линзу.»\n\n"

    "Финальная фраза (обязательно, после приглашения):\n"
    "«Это только зеркало. Важно — что ты сам узнаёшь в этом.»"
)

SELF_INQUIRY_PROMPT = (
    "Верни пользователя к его субъективному опыту.\n"
    "Задай вопросы: Что ты сам чувствуешь? Что здесь твоё? С чем ты согласен?\n"
    "2-3 коротких вопроса. Чистый русский."
)

GUIDE_DEEP_PROMPT = "Задай один глубокий вопрос: «Если убрать ожидание — что ты на самом деле хотел почувствовать?»"

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
    "witness": {"name": "Наблюдатель", "static_text": "Что бы ты ни переживал — это осознаётся."},
    "stalker": {"name": "Сталкер", "prompt": "Ты — безмолвное присутствие."},
}

# ================= KEYBOARDS =================
def build_menu_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🧠 Нейро", "callback_data": "neuro"}, {"text": "💭 КПТ", "callback_data": "cbt"}],
        [{"text": "🏺 Юнг", "callback_data": "jung"}, {"text": "🦅 Шаман", "callback_data": "shaman"}],
        [{"text": "🃏 Таро", "callback_data": "tarot"}, {"text": "🧘 Йога", "callback_data": "yoga"}],
        [{"text": "🕉️ Адвайта", "callback_data": "hindu"}, {"text": "🌐 Поле", "callback_data": "field"}],
        [{"text": "👁️ Наблюдатель", "callback_data": "witness"}, {"text": "🎯 Сталкер", "callback_data": "stalker"}],
        [{"text": "🏛️ Архитектор", "callback_data": "architect"}],
    ]}

def build_post_analysis_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🕳 Углубиться", "callback_data": "self_inquiry:deep"}],
        [{"text": "🔍 Посмотреть через линзу", "callback_data": "self_inquiry:lens"}],
        [{"text": "🌿 Завершить", "callback_data": "self_inquiry:end"}],
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

# ================= ROUTING =================
def route(user: dict, text: str) -> str:
    state = user["state"]
    
    log(f"Route: state={state} text={text[:50]}")

    if text.startswith("self_inquiry:"): return "self_inquiry_action"
    if text.startswith("emotion:"): return "emotion"
    if text.startswith("control:"): return "pattern"

    if text == "/start" or text == "/new": return "start"
    if text == "/menu": return "show_menu"
    if text == "/reset": return "reset_state"
    if text == "/patterns": return "show_patterns"

    lens_cmd = text[1:] if text.startswith("/") else None
    if lens_cmd and lens_cmd in LENS_LIBRARY:
        if user.get("last_experience"):
            return f"lens_{lens_cmd}"
        return "start"

    if state == STATE_REFLECTION:
        return "focus"
    
    if state == STATE_FOCUS:
        return "emotion"
    
    if state == STATE_EMOTION:
        return "pattern"
    
    if state == STATE_PATTERN:
        return "mirror_entry"
    
    if state == STATE_SELF_INQUIRY:
        if text.lower() in ["всё", "хватит", "понял", "ясно"]:
            return "self_inquiry_action"
        return "self_inquiry_response"
    
    if state == STATE_EXPERIENCE_RETURN:
        if text.startswith("/"):
            lc = text[1:]
            if lc in LENS_LIBRARY and user.get("last_experience"):
                return f"lens_{lc}"
        return "self_inquiry_response"
    
    if state == STATE_DEEP:
        return "end"
    
    if state == STATE_IDLE:
        return "input_experience" if len(text.split()) >= 5 else "short_input"

    return "start"

# ================= HANDLERS =================
def handle_start(user: dict) -> dict:
    user.update({
        "state": STATE_IDLE, "last_experience": "", "used_lenses": [],
        "self_inquiry_depth": 0,
        "guide_focus": "", "guide_emotion": "", "guide_control": ""
    })
    save_users_sync()
    
    if user.get("returning_user"):
        return {
            "text": (
                "🌿 С возвращением.\n\n"
                "Ты можешь описать новый опыт.\n\n"
                "Или сразу посмотреть прошлый через линзу:\n"
                "/neuro /jung /cbt /shaman\n\n"
                "Начни с того, что сейчас важно."
            )
        }
    
    return {
        "text": (
            "🌿 Я — проводник осознания.\n\n"
            "Ты описываешь опыт — я помогаю понять, что с тобой происходило.\n\n"
            "Опиши любой недавний опыт:\n"
            "— ситуация\n"
            "— ощущение в теле\n"
            "— что тебя зацепило"
        )
    }

def handle_experience_input(user: dict, text: str) -> dict:
    user["last_experience"] = text[:MAX_INPUT_LENGTH]
    result = call_llm_sync([
        {"role": "system", "content": GUIDE_REFLECTION_PROMPT},
        {"role": "user", "content": text[:MAX_INPUT_LENGTH]}
    ], max_tokens=300, user=user)
    user["state"] = STATE_REFLECTION
    save_users_sync()
    return {"text": result}

def handle_focus(user: dict, text: str) -> dict:
    user["guide_focus"] = text
    user["state"] = STATE_FOCUS
    save_users_sync()
    return {"text": "🪨 Ты выбрал это место.\n\nКакое чувство здесь было сильнее всего?", "keyboard": build_emotion_keyboard()}

def handle_emotion(user: dict, text: str) -> dict:
    clean = text.replace("emotion:", "") if text.startswith("emotion:") else text
    user["guide_emotion"] = clean
    user["state"] = STATE_EMOTION
    save_users_sync()
    return {"text": "🌬️ Это больше про контроль или про отпускание?", "keyboard": build_control_keyboard()}

def handle_pattern(user: dict, text: str) -> dict:
    clean = text.replace("control:", "") if text.startswith("control:") else text
    user["guide_control"] = clean
    result = call_llm_sync([
        {"role": "system", "content": GUIDE_PATTERN_PROMPT},
        {"role": "user", "content": user["last_experience"][:500]}
    ], max_tokens=300, user=user)
    
    pattern_line = (result or "").strip().split("\n")[0]
    if not pattern_line or len(pattern_line) < 3:
        pattern_line = "неоформленный опыт"
    
    patterns = user.setdefault("collected_patterns", [])
    patterns.append(pattern_line)
    if len(patterns) > 50:
        patterns.pop(0)
    
    user["state"] = STATE_SELF_INQUIRY
    user["returning_user"] = True
    user["self_inquiry_depth"] = 0
    
    mirror_text = build_mirror(user, pattern_line)
    
    save_users_sync()
    
    return {
        "text": (
            f"🧭 Паттерн:\n\n{pattern_line}\n\n"
            f"────────────────────\n{mirror_text}"
        )
    }

def build_mirror(user: dict, pattern: str) -> str:
    experience = user.get("last_experience", "")[:EXPERIENCE_SWEET_SPOT]
    result = call_llm_sync([
        {"role": "system", "content": MIRROR_PROMPT_V2},
        {"role": "user", "content": f"Паттерн: {pattern}\n\nОпыт:\n{experience}"}
    ], max_tokens=MIRROR_MAX_TOKENS, user=user)
    
    if len(result) > MIRROR_MAX_CHARS:
        result = result[:MIRROR_MAX_CHARS].rsplit(".", 1)[0] + "."
    if len(result) < 200:
        result += "\n\nПопробуй ещё раз описать, что именно ты почувствовал."
    
    return result

def handle_self_inquiry_response(user: dict, text: str) -> dict:
    user["self_inquiry_depth"] = user.get("self_inquiry_depth", 0) + 1
    
    if user["self_inquiry_depth"] > MAX_SELF_INQUIRY_DEPTH:
        user["state"] = STATE_IDLE
        user.update({"guide_focus": "", "guide_emotion": "", "guide_control": "", "self_inquiry_depth": 0})
        save_users_sync()
        return {"text": "🌿 Достаточно. Попробуй понаблюдать это в жизни."}
    
    if "?" in text and len(text.split()) < 10:
        return {
            "text": "Я говорил про ощущения в твоём опыте.\n\nЧто из этого откликается тебе сейчас сильнее?",
            "keyboard": build_post_analysis_keyboard()
        }
    
    v = user.get("user_vector", {})
    mode = "reflect"
    if v.get("resistance", 0) >= 6: mode = "soften"
    elif v.get("depth", 0) < 3: mode = "deepen"
    elif v.get("clarity", 0) < 4: mode = "clarify"
    
    prompts = {
        "deepen": "Задай один вопрос глубже в чувство.",
        "clarify": "Помоги сформулировать точнее. 1 отражение + 1 уточняющий вопрос.",
        "reflect": "1 отражение + 1 мягкий вопрос.",
        "soften": "Человек сопротивляется. Мягкий, безопасный вопрос.",
        "silence": "Ничего не спрашивай. 1 фраза фиксации.",
    }
    
    result = call_llm_sync([
        {"role": "system", "content": prompts[mode]},
        {"role": "user", "content": text[:500]}
    ], max_tokens=200, user=user)
    
    if mode == "silence":
        return {"text": result}
    return {"text": result, "keyboard": build_post_analysis_keyboard()}

def handle_self_inquiry_action(user: dict, text: str) -> dict | None:
    sub = text.split(":", 1)[1] if ":" in text else text
    if sub in ["всё", "хватит", "понял", "ясно"]:
        sub = "end"
    
    if sub == "deep":
        user["state"] = STATE_DEEP
        result = call_llm_sync([
            {"role": "system", "content": GUIDE_DEEP_PROMPT},
            {"role": "user", "content": user["last_experience"][:500]}
        ], max_tokens=150, user=user)
        save_users_sync()
        return {"text": result}
    elif sub == "lens":
        user["state"] = STATE_IDLE
        return {"text": "Хорошо. Давай посмотрим на этот же опыт с другой стороны.\n\nВыбери линзу:", "keyboard": build_menu_keyboard()}
    elif sub == "end":
        user["state"] = STATE_IDLE
        user.update({"guide_focus": "", "guide_emotion": "", "guide_control": "", "self_inquiry_depth": 0})
        save_users_sync()
        return {"text": "🌿 Принято. Ты можешь понаблюдать это в жизни или принести новый опыт."}
    return None

def handle_lens(user: dict, lens_key: str) -> dict:
    if not user.get("last_experience"):
        return {"text": "Сначала нужен опыт.\n\nОпиши, что ты пережил — и я разберу это через выбранную линзу."}
    
    lens = LENS_LIBRARY[lens_key]
    if lens.get("static_text"):
        result = lens["static_text"]
    else:
        result = call_llm_sync([
            {"role": "system", "content": lens["prompt"]},
            {"role": "user", "content": user["last_experience"]}
        ], user=user)
    
    lst = user.setdefault("used_lenses", [])
    lst.append(lens_key)
    if len(lst) > 50:
        lst.pop(0)
    user["state"] = STATE_IDLE
    save_users_sync()
    
    return {"text": f"Смотрю через «{lens['name']}».\n\n{result}\n\nЧто ты возьмёшь из этого в следующий раз?"}

def add_experience_return_block() -> dict:
    return {
        "text": (
            "🌿 Теперь самое важное:\n\n"
            "Эти образы — сугубо индивидуальны. Только ты знаешь, что они значат для тебя.\n"
            "Все интерпретации — лишь зеркало. Что-то откликнется, что-то нет.\n\n"
            "Если хочешь — можем разобрать глубже или посмотреть через другую линзу."
        ),
        "keyboard": build_post_analysis_keyboard()
    }

# ================= EXECUTE =================
def execute(uid: int, action: str, text: str) -> dict | None:
    user = get_user(uid)
    log(f"Execute: uid={uid} action={action} text={text[:50]} state={user['state']}")
    
    if action == "start": return handle_start(user)
    if action == "reset_state":
        user.update({
            "state": STATE_IDLE, "last_experience": "",
            "used_lenses": [], "self_inquiry_depth": 0,
            "guide_focus": "", "guide_emotion": "", "guide_control": ""
        })
        save_users_sync()
        return {"text": "🔄 Пространство очищено."}
    if action == "show_menu": return {"text": "🎯 Выбери линзу:", "keyboard": build_menu_keyboard()}
    if action == "show_patterns":
        patterns = user.get("collected_patterns", [])
        if not patterns: return {"text": "Узоры ещё не проявились."}
        return {"text": "🕸️ Твои узоры:\n\n" + "\n\n".join(f"{i+1}. {p}" for i, p in enumerate(patterns[-10:]))}
    
    if action == "input_experience": return handle_experience_input(user, text)
    if action == "short_input":
        user["last_experience"] = text
        user["state"] = STATE_REFLECTION
        result = call_llm_sync([
            {"role": "system", "content": GUIDE_REFLECTION_PROMPT},
            {"role": "user", "content": text}
        ], max_tokens=300, user=user)
        return {"text": result}
    
    if action == "focus": return handle_focus(user, text)
    if action == "emotion": return handle_emotion(user, text)
    if action == "pattern":
        response = handle_pattern(user, text)
        return response
    if action == "mirror_entry":
        user["state"] = STATE_SELF_INQUIRY
        patterns = user.get("user_world", {}).get("patterns", [])
        last_pattern = patterns[-1] if patterns else "неоформленный опыт"
        mirror_text = build_mirror(user, last_pattern)
        save_users_sync()
        return {"text": f"🧠 Интерпретация:\n{mirror_text}"}
    
    if action == "self_inquiry_response": return handle_self_inquiry_response(user, text)
    if action == "self_inquiry_action": return handle_self_inquiry_action(user, text)
    if action == "end":
        user["state"] = STATE_IDLE
        user.update({"guide_focus": "", "guide_emotion": "", "guide_control": "", "self_inquiry_depth": 0})
        save_users_sync()
        return {"text": "Ты можешь понаблюдать это в жизни или принести новый опыт. Я здесь."}
    
    if action.startswith("lens_"): return handle_lens(user, action.replace("lens_", ""))
    
    return None

# ================= PROCESS MESSAGE =================
def process_message(chat_id: int, text: str) -> None:
    user = get_user(chat_id)
    
    if not text:
        return
    
    text = text.strip()
    action = route(user, text)
    response = execute(chat_id, action, text)
    
    if response:
        send_message(chat_id, response.get("text", ""), response.get("keyboard"))
    
    # 🔥 ГАРАНТИРОВАННАЯ отправка меню после pattern
    if action == "pattern":
        user["state"] = STATE_EXPERIENCE_RETURN
        save_users_sync()
        time.sleep(0.5)
        return_block = add_experience_return_block()
        send_message(chat_id, return_block["text"], return_block["keyboard"])
    
    # 🔥 ГАРАНТИРОВАННАЯ отправка меню после mirror_entry
    if action == "mirror_entry":
        user["state"] = STATE_EXPERIENCE_RETURN
        save_users_sync()
        time.sleep(0.5)
        return_block = add_experience_return_block()
        send_message(chat_id, return_block["text"], return_block["keyboard"])

def process_callback(chat_id: int, data: str) -> None:
    user = get_user(chat_id)
    
    if not data:
        return
    
    action = route(user, data)
    response = execute(chat_id, action, data)
    
    if response:
        send_message(chat_id, response.get("text", ""), response.get("keyboard"))

# ================= WEBHOOK HANDLER =================
class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/10.1-MIRROR"
    
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
                log("Rejected: invalid secret")
                self._send_json(403, {"ok": False, "error": "Invalid webhook secret"})
                return
        
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        body = raw.decode("utf-8", errors="replace")
        
        log(f"Webhook: {len(raw)} bytes")
        log(f"Payload: {body[:300]}")
        
        try:
            update = json.loads(body) if body else {}
        except json.JSONDecodeError:
            self._send_json(400, {"ok": False, "error": "Invalid JSON"})
            return
        
        callback = update.get("callback_query")
        if callback:
            cid = callback.get("id", "")
            msg = callback.get("message", {})
            chat_id = msg.get("chat", {}).get("id")
            data = callback.get("data", "")
            
            log(f"Callback: chat={chat_id} data={data}")
            
            answer_callback(cid)
            if chat_id and data:
                process_callback(chat_id, data)
            
            self._send_json(200, {"ok": True})
            return
        
        message = update.get("message") or update.get("edited_message") or {}
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        text = message.get("text") or message.get("caption") or ""
        
        log(f"Message: chat={chat_id} text={text[:100] if text else '(empty)'}")
        
        if chat_id and text:
            process_message(chat_id, text)
        elif chat_id:
            log(f"No text in message. Keys: {list(message.keys())}")
        
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
    log(f"ShamanBot v10.1-MIRROR on {HOST}:{PORT}")
    
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
