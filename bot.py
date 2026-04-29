import os
import json
import time
import asyncio
import re
import traceback
import random
import httpx
from collections import deque, defaultdict
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.staticfiles import StaticFiles

# ================= LIFESPAN (v8.5.2: autonomous world) =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_users()
    log_event("startup", port=int(os.getenv("PORT", "8080")))
    cleanup_task = asyncio.create_task(cleanup_users())
    asyncio.create_task(world_background_tick())
    yield
    await safe_save()
    log_event("shutdown_start")
    cleanup_task.cancel()
    for uid, worker_task in list(workers.items()):
        if not worker_task.done(): worker_task.cancel()
    await telegram_http.aclose()
    await llm_http.aclose()
    await media_http.aclose()
    log_event("shutdown_complete")

app = FastAPI(title="ShamanBot v8.5.2 (autonomous world)", lifespan=lifespan)

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
MAX_WORKERS = 100

http_limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
telegram_http = httpx.AsyncClient(timeout=30, limits=http_limits)
llm_http = httpx.AsyncClient(timeout=60, limits=http_limits)
media_http = httpx.AsyncClient(timeout=60, limits=http_limits)

# ================= PERSISTENCE =================
save_lock = asyncio.Lock()

def load_users():
    global users
    try:
        if os.path.exists("data/users.json"):
            with open("data/users.json", "r", encoding="utf-8") as f:
                loaded = json.load(f)
                for uid, data in loaded.items():
                    users[int(uid)] = data
    except: pass

def save_users():
    os.makedirs("data", exist_ok=True)
    with open("data/users.json", "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)

async def safe_save():
    async with save_lock:
        save_users()

# ================= STATE =================
STATE_IDLE = "idle"
STATE_EXPERIENCE_RECEIVED = "experience_received"
STATE_FOCUS_POINT = "focus_point"
STATE_EMOTION_CLARIFY = "emotion_clarify"
STATE_PATTERN_CHECK = "pattern_check"
STATE_DEPTH_GATE = "depth_gate"
STATE_CAVE = "cave"
STATE_IMAGE = "image"
STATE_ANCHOR_AWAIT = "anchor_await"
STATE_ARCHITECT_ANALYSIS = "architect_analysis"
STATE_ARCHITECT_FORMULA = "architect_formula"
STATE_ARCHITECT_STRATEGY = "architect_strategy"

users = {}; queues = {}; workers = {}; locks = {}

# ================= BROADCAST (in-memory) =================
broadcast_media_data = {}
user_ids_set = set()

def save_user_to_memory(chat_id: int) -> None:
    user_ids_set.add(chat_id)

def save_broadcast_media(media_type: str, file_id: str, caption: str = "") -> None:
    global broadcast_media_data
    broadcast_media_data = {"type": media_type, "file_id": file_id, "caption": caption}

def load_broadcast_media() -> dict:
    return broadcast_media_data

async def broadcast_to_all(media: dict) -> int:
    if not user_ids_set: return 0
    mtype = media.get("type", "photo"); fid = media.get("file_id", ""); cap = media.get("caption", "")
    method = "sendVoice" if mtype == "voice" else "sendPhoto"
    key = "voice" if mtype == "voice" else "photo"
    count = 0
    for uid in list(user_ids_set):
        try:
            await asyncio.wait_for(telegram_http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", data={"chat_id": uid, key: fid, "caption": cap}, timeout=10), timeout=10)
            count += 1; await asyncio.sleep(0.05)
        except: pass
    return count

# ================= INLINE KEYBOARDS =================
def build_menu_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🧠 Нейро", "callback_data": "neuro"}, {"text": "💭 КПТ", "callback_data": "cbt"}],
        [{"text": "🏺 Юнг", "callback_data": "jung"}, {"text": "🦅 Шаман", "callback_data": "shaman"}],
        [{"text": "🃏 Таро", "callback_data": "tarot"}, {"text": "🧘 Йога", "callback_data": "yoga"}],
        [{"text": "🕉️ Адвайта", "callback_data": "hindu"}, {"text": "🌐 Поле", "callback_data": "field"}],
        [{"text": "👁️ Наблюдатель", "callback_data": "witness"}, {"text": "🎯 Сталкер", "callback_data": "stalker"}],
        [{"text": "🏛️ Архитектор", "callback_data": "architect"}],
        [{"text": "✨ Собрать в целое", "callback_data": "/integrate"}, {"text": "🔄 Новый опыт", "callback_data": "/new"}],
    ]}
async def send_menu_with_buttons(chat_id: int) -> None:
    await telegram_http.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": "🎯 Выбери линзу:",
            "reply_markup": build_menu_keyboard()
        }
    )

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

def build_pattern_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "✅ Да, знакомо", "callback_data": "pattern:да"}],
        [{"text": "❌ Нет", "callback_data": "pattern:нет"}],
        [{"text": "🤔 Не уверен", "callback_data": "pattern:не уверен"}],
    ]}

def build_gate_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🧿 Шаманизм", "callback_data": "shaman"}, {"text": "🧠 Нейро", "callback_data": "neuro"}],
        [{"text": "💭 КПТ", "callback_data": "cbt"}, {"text": "🏺 Юнг", "callback_data": "jung"}],
        [{"text": "🌐 Поле", "callback_data": "field"}, {"text": "🏛️ Архитектор", "callback_data": "architect"}],
        [{"text": "➖ Без анализа", "callback_data": "guide:no_lens"}],
    ]}

def build_integration_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🎯 Изменить ожидание", "callback_data": "integrate:ожидание"}],
        [{"text": "🐢 Замедлиться", "callback_data": "integrate:замедлиться"}],
        [{"text": "👁️ Наблюдать без цели", "callback_data": "integrate:наблюдать"}],
        [{"text": "🤷 Не знаю", "callback_data": "integrate:не знаю"}],
    ]}

def build_cave_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "🕳️ Войти в пещеру", "callback_data": "cave:enter"}],
        [{"text": "🌿 Остаться здесь", "callback_data": "cave:stay"}],
    ]}

async def send_inline_keyboard(chat_id: int, text: str, keyboard: dict) -> None:
    await telegram_http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": chat_id, "text": text, "reply_markup": keyboard})

async def answer_callback(callback_id: str) -> None:
    await telegram_http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id": callback_id})

# ================= DEDUP / LOGGING / METRICS =================
processed_updates = set(); processed_order = deque(maxlen=5000)
def dedup(update_id: int) -> bool:
    if update_id in processed_updates: return True
    processed_updates.add(update_id); processed_order.append(update_id)
    if len(processed_updates) > 5000: processed_updates.discard(processed_order.popleft())
    return False

trace_log = deque(maxlen=2000)
def log_event(event: str, uid: int = 0, **kwargs):
    print(json.dumps({"ts": time.time(), "event": event, "uid": uid, **kwargs}, ensure_ascii=False))
def trace(uid: int, action: str, stage: str, meta: dict = None):
    log_event("trace", uid=uid, action=action, stage=stage, meta=meta or {})

action_metrics = defaultdict(int); error_metrics = defaultdict(int)
metrics = {"requests": 0, "llm_calls": 0, "broadcasts": 0, "integrations": 0, "anchor_questions": 0, "architect_sessions": 0, "guide_sessions": 0, "cave_sessions": 0, "spirit_encounters": 0, "guide_appearances": 0, "world_tick_events": 0, "world_voice_changes": 0, "world_scenes": 0}
def record_action(action: str): action_metrics[action] += 1
def record_error(error_type: str): error_metrics[error_type] += 1

# ================= WORLD ENGINE (v8.5.2: autonomous world) =================
LOCATIONS = {
    "forest": {"name": "Лес", "meaning": "неопределённость / вход"},
    "river": {"name": "Река", "meaning": "эмоции / поток"},
    "cave": {"name": "Пещера", "meaning": "страхи / тень"},
    "mirror": {"name": "Зеркало", "meaning": "саморефлексия"},
    "mountain": {"name": "Гора", "meaning": "интеграция / смысл"},
}

ENTITIES = {
    "Контроль": {"type": "protective_pattern", "locations": ["cave", "forest"]},
    "Избегание": {"type": "protective_pattern", "locations": ["cave"]},
    "Сомнение": {"type": "cognitive_pattern", "locations": ["forest", "mirror"]},
    "Наблюдатель": {"type": "awareness", "locations": ["mirror", "mountain"]},
    "Страх": {"type": "emotional_pattern", "locations": ["cave", "river"]},
    "Интерес": {"type": "exploratory", "locations": ["forest", "river"]},
}

ENTITY_MAP = {
    "страх": "Страж", "избегание": "Туман", "сомнение": "Шёпот",
    "контроль": "Стена", "потеря контроля": "Поток",
}

STAGES = ["бессознательное повторение", "замечание паттерна", "осознание причины", "выбор иначе", "закрепление"]
ARCHETYPES = {"бессознательное повторение": "Спящий", "замечание паттерна": "Наблюдатель", "осознание причины": "Исследователь", "выбор иначе": "Архитектор", "закрепление": "Проводник"}
MAX_SYMBOLS_PER_SESSION = 1

def decide_location(emotion: str, control: str) -> str:
    if emotion == "страх": return "cave"
    if emotion == "разочарование": return "river"
    if emotion == "принятие": return "mirror"
    if emotion == "интерес": return "forest"
    if control == "контроль": return "cave"
    if control == "потеря": return "river"
    return "forest"

def decide_entities(location: str, emotion: str, pattern: str) -> list:
    activated = []
    pl = pattern.lower(); el = emotion.lower()
    for name, info in ENTITIES.items():
        if location not in info["locations"]: continue
        if name.lower() in pl or name.lower() in el: activated.append(name)
    if not activated:
        if location == "cave": activated.append("Страх")
        elif location == "forest": activated.append("Сомнение")
        elif location == "mirror": activated.append("Наблюдатель")
    return activated

def should_enter_cave(user: dict, pattern: str) -> bool:
    world = user.get("user_world", {})
    timeline = world.get("timeline", [])
    same = [n for n in timeline if n.get("pattern", "")[:60] in pattern or pattern[:60] in n.get("pattern", "")]
    return len(same) >= 3 and not user.get("_symbol_used_this_session")

def detect_spirit(user: dict) -> str | None:
    world = user.get("user_world", {})
    patterns = world.get("patterns", [])
    if len(patterns) < 5: return None
    last = patterns[-1].lower()
    for key, name in ENTITY_MAP.items():
        if key in last: return name
    return "Неизвестный Дух"

def user_stuck(user: dict) -> bool:
    return (user["state"] == STATE_DEPTH_GATE and len(user.get("used_lenses", [])) == 0 and not user.get("_symbol_used_this_session"))

def detect_stage(pattern_count: int, integrations: int) -> str:
    if pattern_count < 2: return STAGES[0]
    elif pattern_count < 4: return STAGES[1]
    elif integrations < 2: return STAGES[2]
    elif integrations < 5: return STAGES[3]
    else: return STAGES[4]

def detect_archetype(stage: str) -> str:
    return ARCHETYPES.get(stage, "Путник")

def detect_vector(patterns: list) -> str:
    if not patterns: return "исследование себя"
    last = patterns[-1].lower()
    if "страх" in last: return "движение к доверию"
    if "избегание" in last: return "движение к выражению себя"
    if "контроль" in last: return "движение к отпусканию"
    if "сомнение" in last: return "движение к ясности"
    return "исследование себя"

def get_next_step(stage: str) -> str:
    if stage == "замечание паттерна": return "Попробуй в следующий раз не подавить реакцию, а заметить её в моменте."
    elif stage == "осознание причины": return "Спроси себя: чего я на самом деле боюсь?"
    elif stage == "выбор иначе": return "Сделай маленькое новое действие, даже если есть страх."
    return "Продолжай наблюдать."

def find_similar_pattern(new_pattern: str, patterns: list) -> str | None:
    ns = new_pattern[:80]
    for p in patterns:
        ps = p[:80] if len(p) > 80 else p
        if ns[:30] in ps or ps[:30] in ns: return p
    return None

def analyze_world(world: dict) -> dict:
    patterns = world.get("patterns", []); events = world.get("events", [])
    nodes = world.get("nodes", []); timeline = world.get("timeline", [])
    entities = world.get("entities", {})
    freq = {}
    for p in patterns:
        s = p[:80] if len(p) > 80 else p; freq[s] = freq.get(s, 0) + 1
    sp = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    cp = sp[0] if sp else None
    open_loops = [n for n in timeline if n.get("status") == "open"]
    closed_loops = [n for n in timeline if n.get("status") == "closed"]
    de = None; ds = 0
    for name, data in entities.items():
        if data.get("strength", 0) > ds: ds = data["strength"]; de = name
    return {"core_pattern": cp, "core_count": cp[1] if cp else 0, "total_patterns": len(patterns), "total_events": len(events), "total_nodes": len(timeline), "open_loops": len(open_loops), "closed_loops": len(closed_loops), "dominant_entity": de, "dominant_strength": ds, "entities": entities}

def format_world_view(world: dict) -> str:
    analysis = analyze_world(world)
    state = world.get("world_state", {})
    cl = state.get("location", "forest"); loc_name = LOCATIONS.get(cl, {}).get("name", "Лес")
    lines = ["🌲 ТВОЙ МИР\n"]
    lines.append(f"Ты сейчас в: {loc_name}.\n")

    # Атмосфера
    if state.get("fog", 0) > 0.5: lines.append("🌫️ В мире стало больше тумана.")
    if state.get("clarity", 0) > 0.5: lines.append("🔍 Пространство становится яснее.")
    if state.get("depth", 0) > 2: lines.append("🕳️ Ты углубился в пещеры своего мира.")
    if state.get("stability", 0) > 0.5: lines.append("🌄 В мире стало больше устойчивости.")
    if any([state.get("fog", 0) > 0.5, state.get("clarity", 0) > 0.5, state.get("depth", 0) > 2, state.get("stability", 0) > 0.5]): lines.append("")

    # Голос мира
    voice = world_voice(world)
    lines.append(f"🎭 Мир говорит: {voice['mode']} (интенсивность: {voice['intensity']:.2f})")

    if analysis["dominant_entity"]:
        lines.append(f"\nДоминирующая сила: {analysis['dominant_entity']} (сила: {analysis['dominant_strength']:.1f})")
    if analysis["core_pattern"]:
        lines.append(f"\n🕸️ Центральный узор:\n→ {analysis['core_pattern'][0]}")
    if analysis["open_loops"] > 0: lines.append(f"\n🌀 Открытых узлов: {analysis['open_loops']}")
    if analysis["closed_loops"] > 0: lines.append(f"✅ Завершённых узлов: {analysis['closed_loops']}")

    # Дух
    if len(world.get("patterns", [])) >= 5:
        spirit = detect_spirit_from_world(world)
        if spirit: lines.append(f"\n👁 В твоём мире проявился Дух: {spirit}")

    # Тропы
    if world.get("edges"):
        fe = defaultdict(int)
        for e in world["edges"]: fe[f"{e.get('from', '')} → {e.get('to', '')}"] += 1
        te = sorted(fe.items(), key=lambda x: x[1], reverse=True)[:3]
        if te:
            lines.append("\n🛤 Протоптанные тропы:")
            for edge, count in te: lines.append(f"— {edge} ({count} раз)")

    # Память мира
    memory = state.get("memory", {})
    if memory.get("ignored_signals"): lines.append(f"\n🌫️ Игнорируется: {len(memory['ignored_signals'])} сигналов")
    if memory.get("resolved_tensions"): lines.append(f"✅ Разрешено: {len(memory['resolved_tensions'])} напряжений")

    # Намерение
    intention = world_intention(world)
    if intention: lines.append(f"\n🧭 Направленность мира: {intention}")

    lines.append("\n\n/menu — линзы | /patterns — узоры | /timeline — путь | /world — карта")
    return "\n\n".join(lines)

def detect_spirit_from_world(world: dict) -> str | None:
    patterns = world.get("patterns", [])
    if len(patterns) < 5: return None
    last = patterns[-1].lower()
    for key, name in ENTITY_MAP.items():
        if key in last: return name
    return "Неизвестный Дух"

def format_timeline_view(user: dict) -> str:
    world = user.get("user_world", {})
    timeline = world.get("timeline", [])
    if not timeline: return "Твой путь ещё не начался."
    patterns = world.get("patterns", [])
    integrations = user.get("integration_count", 0)
    stage = detect_stage(len(patterns), integrations)
    archetype = detect_archetype(stage)
    vector = detect_vector(patterns)
    next_step = get_next_step(stage)
    lines = ["🧭 ТВОЙ ПУТЬ\n"]
    lines.append(f"Ты проходишь через тему:")
    last_pattern = patterns[-1][:80] if patterns else "исследование себя"
    lines.append(f"«{last_pattern}»")
    cl = world.get("world_state", {}).get("location", "forest")
    lines.append(f"\nТы сейчас в: {LOCATIONS.get(cl, {}).get('name', 'Лес')}")
    lines.append(f"Архетип: {archetype}")
    lines.append(f"Стадия: {stage}")
    lines.append(f"Движение: {vector}")
    lines.append(f"\nСледующий шаг:\n{next_step}")
    spirit = detect_spirit(user)
    if spirit: lines.append(f"\n👁 В твоём мире проявился Дух: {spirit}")
    open_count = sum(1 for n in timeline if n.get("status") == "open")
    if open_count > 0: lines.append(f"\n🌀 Открыто узлов: {open_count}")
    lines.append("\n\n/menu — линзы | /world — карта | /patterns — узоры")
    return "\n".join(lines)

def check_world_insight(user: dict) -> str | None:
    world = user.get("user_world", {})
    analysis = analyze_world(world)
    if analysis["dominant_entity"] and analysis["dominant_strength"] >= 0.6:
        return f"🌀 В твоём мире усиливается {analysis['dominant_entity']}.\n\nЭта сила проявилась уже несколько раз. Хочешь посмотреть на неё глубже? /menu — выбери линзу."
    if analysis["open_loops"] >= 2:
        return f"🌿 У тебя есть {analysis['open_loops']} незавершённых узла.\n\nХочешь вернуться к ним? /timeline — посмотри свой путь."
    return None

def update_entities(world: dict, location: str, pattern: str, emotion: str) -> None:
    entities = world.setdefault("entities", {})
    activated = decide_entities(location, emotion, pattern)
    for name in activated:
        if name not in entities: entities[name] = {"strength": 0.3, "triggers": [], "locations": [], "activity": 0}
        entities[name]["strength"] = min(1.0, entities[name].get("strength", 0.3) + 0.15)
        entities[name]["activity"] = entities[name].get("activity", 0) + 1
        if location not in entities[name].setdefault("locations", []): entities[name]["locations"].append(location)
    for name in entities:
        if name not in activated: entities[name]["strength"] = max(0.1, entities[name].get("strength", 0.3) - 0.05)

# ================= CONSEQUENCES (v8.5.2) =================
def apply_consequence(user: dict, action: str) -> str | None:
    world = user.get("user_world", {})
    state = world.setdefault("world_state", {})
    result = None

    if action == "integrate":
        state["stability"] = state.get("stability", 0) + 0.1
        if state["stability"] > 0.5: result = "🌄 В мире стало больше устойчивости."
        # Обновляем память
        memory = state.setdefault("memory", {})
        last_pattern = world.get("patterns", [])[-1] if world.get("patterns") else ""
        if last_pattern and last_pattern in memory.get("ignored_signals", []):
            memory.setdefault("resolved_tensions", []).append(last_pattern[:80])

    elif action == "avoid":
        state["fog"] = state.get("fog", 0) + 0.1
        # Усиливаем Избегание
        entities = world.setdefault("entities", {})
        if "Избегание" not in entities: entities["Избегание"] = {"strength": 0.3, "triggers": [], "locations": [], "activity": 0}
        entities["Избегание"]["strength"] = min(1.0, entities["Избегание"].get("strength", 0.3) + 0.1)
        # Обновляем память
        memory = state.setdefault("memory", {})
        last_pattern = world.get("patterns", [])[-1] if world.get("patterns") else ""
        if last_pattern: memory.setdefault("ignored_signals", []).append(last_pattern[:80])
        if state["fog"] > 0.5: result = "🌫️ Пещера осталась позади, но туман стал плотнее."

    elif action == "enter_cave":
        state["depth"] = state.get("depth", 0) + 1
        entities = world.setdefault("entities", {})
        if "Страх" not in entities: entities["Страх"] = {"strength": 0.3, "triggers": [], "locations": [], "activity": 0}
        entities["Страх"]["strength"] = min(1.0, entities["Страх"].get("strength", 0.3) + 0.2)
        if state["depth"] > 2: result = "🕳️ Ты входишь глубже. Воздух становится плотнее — но ты начинаешь видеть яснее."

    elif action == "observe":
        state["clarity"] = state.get("clarity", 0) + 0.1
        if state["clarity"] > 0.5: result = "🔍 Пространство становится яснее."

    # Обновляем память повторяющихся паттернов
    memory = state.setdefault("memory", {})
    patterns = world.get("patterns", [])
    if patterns:
        last = patterns[-1][:80]
        rep = memory.setdefault("repeated_patterns", [])
        if last not in rep: rep.append(last)

    return result

# ================= WORLD VOICE (v8.5.2) =================
def world_voice(world: dict) -> dict:
    state = world.get("world_state", {})
    pressure = state.get("pressure", 0)
    depth = state.get("depth", 0)
    clarity = state.get("clarity", 0)
    voice = {"mode": "neutral", "intensity": 0.3}
    if pressure > 0.7: voice["mode"] = "mythic"
    if depth > 3: voice["mode"] = "whisper"
    if clarity > 0.6: voice["mode"] = "technical"
    if pressure > 0.8 and depth > 2: voice["mode"] = "symbolic"
    if any([pressure > 0.7, depth > 3, clarity > 0.6]): voice["intensity"] = 0.7
    return voice

def render_world_message(world: dict, message: str) -> str:
    voice = world_voice(world)
    mode = voice["mode"]
    if mode == "neutral": return message
    if mode == "whisper": return f"… {message} …"
    if mode == "technical": return f"[анализ] {message}"
    if mode == "mythic": return f"🌌 {message} — как знак"
    if mode == "symbolic": return f"🜂 {message.upper()}"
    return message

def world_archetype(world: dict) -> str:
    state = world.get("world_state", {})
    if state.get("depth", 0) > 3: return "Проводник"
    if state.get("pressure", 0) > 0.7: return "Тень"
    if state.get("clarity", 0) > 0.6: return "Архитектор"
    return "Наблюдатель"

# ================= WORLD MEMORY & INTENTION (v8.5.2) =================
def world_intention(world: dict) -> str | None:
    memory = world.get("world_state", {}).get("memory", {})
    ignored = memory.get("ignored_signals", [])
    repeated = memory.get("repeated_patterns", [])
    resolved = memory.get("resolved_tensions", [])
    if len(ignored) > len(resolved) + 2: return "завершение игнорируемого"
    if len(repeated) > 5: return "усиление паттерна"
    return None

# ================= WORLD AUTONOMY (v8.5.2) =================
def spontaneous_scene(world: dict) -> str | None:
    state = world.get("world_state", {})
    if state.get("pressure", 0) > 0.8: return "🌩️ В тишине леса слышится повторяющийся звук"
    if state.get("restlessness", 0) > 0.7: return "🪶 Появляется узел, который не связан ни с одним твоим выбором"
    if state.get("depth", 0) > 3: return "🕳️ Пещера словно становится ближе сама по себе"
    return None

def world_tick(world: dict) -> list:
    events = []
    state = world.setdefault("world_state", {})
    state["restlessness"] = state.get("restlessness", 0) + 0.05
    if state.get("pressure", 0) > 0.7: events.append("🌩️ Внутри мира нарастает напряжение")
    if state.get("attention", 0) > 0.6: events.append("👁 Что-то смотрит на тебя из леса")
    if state.get("restlessness", 0) > 0.8: events.append("🪶 Появляется новый узел, которого ты не вызывал")
    scene = spontaneous_scene(world)
    if scene: events.append(scene)
    return events

async def world_background_tick():
    while True:
        for uid, user in list(users.items()):
            world = user.get("user_world", {})
            events = world_tick(world)
            if events:
                metrics["world_tick_events"] = metrics.get("world_tick_events", 0) + 1
                q = queues.get(uid)
                if q:
                    for e in events:
                        try: q.put_nowait(e)
                        except: pass
        await asyncio.sleep(60)

# ================= USER =================
def get_user(uid: int):
    uid = int(uid)
    if uid not in users:
        users[uid] = {
            "state": STATE_IDLE, "last_experience": "", "last_active": time.time(),
            "used_lenses": [], "micro_states": [], "integration_count": 0,
            "pending_anchor_lens": None, "last_integration_action": None,
            "_architect_raw": None, "_architect_analysis": None,
            "_symbol_used_this_session": False,
            "guide_focus": "", "guide_emotion": "", "guide_control": "", "guide_pattern": "",
            "collected_patterns": [],
            "user_world": {
                "world_state": {
                    "location": "forest", "depth": 0, "node_id": None,
                    "fog": 0, "clarity": 0, "stability": 0, "pressure": 0, "attention": 0, "restlessness": 0,
                    "memory": {"repeated_patterns": [], "ignored_signals": [], "resolved_tensions": []}
                },
                "timeline": [], "patterns": [], "events": [], "nodes": [], "edges": [], "entities": {}
            }
        }
        asyncio.create_task(safe_save())
    users[uid]["last_active"] = time.time()
    return users[uid]

async def cleanup_users():
    while True:
        now = time.time()
        for uid in list(users.keys()):
            if now - users[uid]["last_active"] > USER_TTL:
                users.pop(uid, None); queues.pop(uid, None); locks.pop(uid, None)
                w = workers.pop(uid, None)
                if w and not w.done(): w.cancel()
        await safe_save()
        await asyncio.sleep(600)

# ================= TELEGRAM =================
async def send(chat_id: int, text: str):
    for i in range(0, len(text), 4000):
        try: await asyncio.wait_for(telegram_http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": chat_id, "text": text[i:i+4000]}), timeout=10)
        except: pass

async def send_photo(chat_id: int, img: bytes, caption: str = ""):
    try: await asyncio.wait_for(telegram_http.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto", data={"chat_id": str(chat_id), "caption": caption}, files={"photo": ("img.jpg", img, "image/jpeg")}), timeout=15)
    except: pass

# ================= LLM =================
async def call_llm(messages, temp=0.7, max_tokens=2000):
    metrics["llm_calls"] += 1
    for attempt in range(2):
        try:
            r = await llm_http.post("https://api.vsegpt.ru:6070/v1/chat/completions", json={"model": VSEGPT_MODEL, "messages": messages, "temperature": temp, "max_tokens": max_tokens}, headers={"Authorization": f"Bearer {VSEGPT_API_KEY}"})
            if r.status_code == 200:
                data = r.json()
                if "choices" in data and data["choices"]: return data["choices"][0]["message"]["content"]
            if attempt < 1: await asyncio.sleep(1)
        except: await asyncio.sleep(1)
    return "⚠️ Модель временно недоступна."

# ================= PROMPTS =================
GUIDE_FACTUAL_MAP_PROMPT = (
    "Ты — проводник. Пространство начинает проясняться.\n"
    "Выдели 3-4 линии: Тело, Образы, События, Ожидания.\n"
    "Спроси: «Где здесь было самое сильное место?»\n"
    "Чистый русский, без маркдауна."
)
GUIDE_PATTERN_PROMPT = (
    "Опыт: {focus}. Чувство: {emotion}. Движение: {control}.\n"
    "Сформулируй ОДИН узор. Спроси: «Ты уже встречал это в жизни?»\n"
    "Чистый русский, коротко, без маркдауна."
)
CAVE_PROMPT = "Если убрать поведение — что ты на самом деле боишься почувствовать? Задай этот вопрос мягко. Чистый русский."
GUIDE_PROMPT_TEXT = "Ты остановился. Иногда в такие моменты появляется проводник. Задай вопрос: «Что ты сейчас избегаешь увидеть?» Чистый русский."

# ================= LENS LIBRARY =================
LENS_LIBRARY = {
    "neuro": {"name": "Нейрофизиология", "prompt": "Ты — нейрофизиолог. Объясни опыт через мозг. Чистый русский."},
    "cbt": {"name": "КПТ", "prompt": "Ты — КПТ-терапевт. Раздели факты и мысли. Чистый русский."},
    "jung": {"name": "Юнг", "prompt": "Ты — юнгианский аналитик. Раскрой архетипы. Чистый русский."},
    "shaman": {"name": "Шаманизм", "prompt": "Ты — шаман. Интерпретируй опыт. Чистый русский."},
    "tarot": {"name": "Таро", "prompt": "Ты — мастер Таро. Посмотри через Арканы. Чистый русский."},
    "yoga": {"name": "Йога", "prompt": "Ты — мастер йоги. Опиши энергетику. Чистый русский."},
    "hindu": {"name": "Адвайта", "prompt": "Ты — учитель адвайты. Где Свидетель? Чистый русский."},
    "field": {"name": "Поле", "prompt": "Ты — голос Поля. Узел, решётка, фазовый сдвиг. Чистый русский."},
    "architect": {"name": "Архитектор", "prompt": "Ты — Архитектор сознания. Ось, Разлом, Мост. Чистый русский."},
    "witness": {"name": "Наблюдатель", "prompt": None, "static_text": "Что бы ты ни переживал — это осознаётся."},
    "stalker": {"name": "Сталкер", "prompt": "Ты — безмолвное присутствие. Указывай на Осознавание."},
}

async def apply_lens(lens_key: str, experience_text: str) -> tuple:
    lens = LENS_LIBRARY[lens_key]
    if lens.get("static_text"): return lens_key, lens["name"], lens["static_text"]
    result = await call_llm([{"role": "system", "content": lens["prompt"]}, {"role": "user", "content": experience_text}])
    return lens_key, lens["name"], result

async def generate_image(prompt: str) -> bytes:
    r = await asyncio.wait_for(media_http.get(f"https://image.pollinations.ai/prompt/{httpx.quote(prompt)}"), timeout=40)
    if r.status_code != 200 or len(r.content) < 1000: raise Exception("bad image")
    return r.content

async def run_integrator(user: dict) -> str:
    metrics["integrations"] = metrics.get("integrations", 0) + 1
    user["last_integration_action"] = user.get("last_action")
    return await call_llm([{"role": "system", "content": "Собери опыт в единую картину."}, {"role": "user", "content": user["last_experience"]}])

# ================= ROUTE + EXECUTE =================
def route(user: dict, text: str) -> str:
    state = user["state"]
    if text == "/start" or text == "/new": return "new_experience"
    if text == "/menu": return "show_menu"
    if text == "/art": return "art"
    if text == "/integrate": return "manual_integrate"
    if text == "/reset": return "reset_state"
    if text == "/patterns": return "show_patterns"
    if text == "/world": return "show_world"
    if text == "/timeline": return "show_timeline"
    lens_cmd = text.lstrip("/")
    if lens_cmd in LENS_LIBRARY: return f"lens_{lens_cmd}"
    if state == STATE_IMAGE: return "image"
    if state == STATE_ANCHOR_AWAIT: return "anchor_response"
    if state == STATE_CAVE: return "cave_response"
    if state == STATE_FOCUS_POINT: return "guide_focus_response"
    if state == STATE_EMOTION_CLARIFY: return "guide_emotion_response"
    if state == STATE_PATTERN_CHECK: return "guide_pattern_response"
    if state == STATE_DEPTH_GATE: return "guide_gate_response"
    if state == STATE_ARCHITECT_ANALYSIS: return "architect_analysis_response"
    if state == STATE_ARCHITECT_FORMULA: return "architect_formula_response"
    if state == STATE_ARCHITECT_STRATEGY: return "architect_strategy_response"
    if state == STATE_EXPERIENCE_RECEIVED: return "new_experience_silent" if len(text.split()) >= 5 else "short_input_with_state"
    return "guide_start" if len(text.split()) >= 5 else "short_input"

async def execute(uid: int, action: str, text: str) -> str:
    user = get_user(uid)
    trace(uid, action, "exec_start")

    if action == "new_experience":
        insight = check_world_insight(user)
        user.update({"state": STATE_IDLE, "last_experience": "", "used_lenses": [], "micro_states": [], "integration_count": 0,
                     "pending_anchor_lens": None, "last_integration_action": None,
                     "_architect_raw": None, "_architect_analysis": None, "_symbol_used_this_session": False})
        asyncio.create_task(send_menu_with_buttons(uid))
        if insight: return f"🌿 Ты возвращаешься в пространство.\n\n{insight}\n\nИли расскажи новый опыт."
        return "🌿 Ты входишь в пространство.\n\nЧто-то в тебе привело тебя сюда. Опиши, что произошло.\n\n👆 Или выбери линзу на кнопках выше."

    if action == "reset_state":
        user.update({"used_lenses": [], "micro_states": [], "integration_count": 0, "state": STATE_IDLE})
        asyncio.create_task(safe_save())
        return "🔄 Пространство очищено."

    if action == "show_menu":
        await send_menu_with_buttons(uid)
        return None

    if action == "show_patterns":
        patterns = user.get("collected_patterns", [])
        if not patterns: return "Узоры ещё не проявились."
        return "🕸️ Твои узоры:\n\n" + "\n\n".join(f"{i+1}. {p}" for i, p in enumerate(patterns[-10:]))

    if action == "show_world":
        return format_world_view(user.get("user_world", {}))

    if action == "show_timeline":
        return format_timeline_view(user)

    if action == "art":
        user["state"] = STATE_IMAGE
        return "🎨 Опиши образ."

    if action == "image":
        try:
            img = await generate_image(text[:MAX_INPUT_LENGTH])
            await send_photo(uid, img, f"✨ {text}")
            user["state"] = STATE_EXPERIENCE_RECEIVED
            return "✨ Образ проявился."
        except:
            user["state"] = STATE_EXPERIENCE_RECEIVED
            return "🌫️ Образ не смог проявиться."

    # === AWARENESS GUIDE FLOW ===
    if action == "guide_start":
        metrics["guide_sessions"] = metrics.get("guide_sessions", 0) + 1
        user["last_experience"] = text[:MAX_INPUT_LENGTH]
        result = await call_llm([{"role": "system", "content": GUIDE_FACTUAL_MAP_PROMPT}, {"role": "user", "content": text[:MAX_INPUT_LENGTH]}], max_tokens=300)
        user["state"] = STATE_FOCUS_POINT
        asyncio.create_task(safe_save())
        return result

    if action == "guide_focus_response":
        user["guide_focus"] = text; user["state"] = STATE_EMOTION_CLARIFY
        asyncio.create_task(safe_save())
        await send_inline_keyboard(uid, "🪨 Какое чувство здесь сильнее всего?", build_emotion_keyboard())
        return None

    if action == "guide_emotion_response":
        user["guide_emotion"] = text; user["state"] = STATE_PATTERN_CHECK
        asyncio.create_task(safe_save())
        await send_inline_keyboard(uid, "🌬️ Это про контроль или про отпускание?", build_control_keyboard())
        return None

    if action == "guide_pattern_response":
        user["guide_control"] = text
        result = await call_llm([{"role": "system", "content": GUIDE_PATTERN_PROMPT.format(focus=user.get("guide_focus", ""), emotion=user.get("guide_emotion", ""), control=text)}, {"role": "user", "content": user["last_experience"][:500]}], max_tokens=300)

        pattern_line = result.split("\n")[0] if "\n" in result else result
        user.setdefault("collected_patterns", []).append(pattern_line)
        world = user.setdefault("user_world", {})
        world.setdefault("world_state", {"location": "forest", "depth": 0, "node_id": None, "fog": 0, "clarity": 0, "stability": 0, "pressure": 0, "attention": 0, "restlessness": 0, "memory": {"repeated_patterns": [], "ignored_signals": [], "resolved_tensions": []}})
        world.setdefault("patterns", []).append(pattern_line)
        if user.get("guide_focus"): world.setdefault("events", []).append(user["guide_focus"])

        new_location = decide_location(user.get("guide_emotion", ""), user.get("guide_control", ""))
        old_location = world.get("world_state", {}).get("location", "forest")
        entities_activated = decide_entities(new_location, user.get("guide_emotion", ""), pattern_line)

        node = {"id": str(time.time()), "timestamp": time.time(), "event": user["last_experience"][:200], "focus": user.get("guide_focus", ""), "emotion": user.get("guide_emotion", ""), "control": text, "pattern": pattern_line, "location": new_location, "lenses": user.get("used_lenses", []), "entities": entities_activated, "status": "open"}
        world.setdefault("timeline", []).append(node)
        world["timeline"] = world["timeline"][-50:]

        if old_location != new_location:
            edge = {"from": old_location, "to": new_location, "trigger": user.get("guide_emotion", ""), "event_id": node["id"], "timestamp": time.time()}
            world.setdefault("edges", []).append(edge); world["edges"] = world["edges"][-100:]

        world["world_state"]["location"] = new_location
        world["world_state"]["depth"] = world["world_state"].get("depth", 0) + 1
        world["world_state"]["node_id"] = node["id"]
        update_entities(world, new_location, pattern_line, user.get("guide_emotion", ""))

        # --- WORLD LAYERS ---
        spirit = detect_spirit(user)
        if spirit and not user.get("_symbol_used_this_session"):
            metrics["spirit_encounters"] = metrics.get("spirit_encounters", 0) + 1
            user["_symbol_used_this_session"] = True; user["state"] = STATE_DEPTH_GATE
            asyncio.create_task(safe_save())
            return f"👁 Это не просто реакция. Это стало духом внутри твоего мира: **{spirit}**.\n\nТы уже встречал это в жизни?"

        if should_enter_cave(user, pattern_line) and not user.get("_symbol_used_this_session"):
            metrics["cave_sessions"] = metrics.get("cave_sessions", 0) + 1
            user["_symbol_used_this_session"] = True; user["state"] = STATE_CAVE
            asyncio.create_task(safe_save())
            await send_inline_keyboard(uid, "🕳️ Ты уже встречал это раньше. Здесь есть вход глубже — в Пещеру.\n\nХочешь войти?", build_cave_keyboard())
            return None

        if user_stuck(user) and not user.get("_symbol_used_this_session"):
            metrics["guide_appearances"] = metrics.get("guide_appearances", 0) + 1
            user["_symbol_used_this_session"] = True
            result = await call_llm([{"role": "system", "content": GUIDE_PROMPT_TEXT}, {"role": "user", "content": user["last_experience"][:500]}], max_tokens=150)
            user["state"] = STATE_DEPTH_GATE; asyncio.create_task(safe_save())
            return f"🧭 Ты остановился. Иногда в такие моменты появляется проводник.\n\n{result}"

        existing = find_similar_pattern(pattern_line, world["patterns"][:-1])
        if existing:
            world.setdefault("nodes", []).append(f"Узел: {pattern_line[:100]}"); world["nodes"] = world["nodes"][-20:]
            user["state"] = STATE_DEPTH_GATE; asyncio.create_task(safe_save())
            await send_inline_keyboard(uid, f"🕸️ Ты в {LOCATIONS.get(new_location, {}).get('name', 'Лесу').upper()}.\n\nЭтот узор тебе знаком. Он уже проявлялся раньше.\n\nТы встречал это в жизни?", build_pattern_keyboard())
            return None

        if len(world["patterns"]) >= 3:
            analysis = analyze_world(world)
            if analysis["core_pattern"]:
                world.setdefault("nodes", []).append(f"Узел: {analysis['core_pattern'][0][:100]}"); world["nodes"] = world["nodes"][-20:]
        user["state"] = STATE_DEPTH_GATE; asyncio.create_task(safe_save())
        await send_inline_keyboard(uid, f"🕸️ Ты в {LOCATIONS.get(new_location, {}).get('name', 'Лесу').upper()}.\n\nИз проявленного начинает складываться узор.\n\n{result}", build_pattern_keyboard())
        return None

    if action == "cave_response":
        if text == "cave:enter":
            user["state"] = STATE_EXPERIENCE_RECEIVED
            consequence = apply_consequence(user, "enter_cave")
            result = await call_llm([{"role": "system", "content": CAVE_PROMPT}, {"role": "user", "content": user["last_experience"][:500]}], max_tokens=150)
            asyncio.create_task(safe_save())
            msg = f"🕳️ Ты входишь в Пещеру.\n\n{result}"
            if consequence: msg += f"\n\n{consequence}"
            return msg
        else:
            user["state"] = STATE_DEPTH_GATE
            consequence = apply_consequence(user, "avoid")
            asyncio.create_task(safe_save())
            msg = "Ты остаёшься на месте."
            if consequence: msg += f"\n\n{consequence}"
            msg += "\n\nХочешь посмотреть через линзу?"
            await send_inline_keyboard(uid, msg, build_gate_keyboard())
            return None

    if action == "guide_gate_response":
        user["guide_pattern"] = text; user["state"] = STATE_EXPERIENCE_RECEIVED
        asyncio.create_task(safe_save())
        await send_inline_keyboard(uid, "Хочешь посмотреть на этот узор через одну из систем?", build_gate_keyboard())
        return None

    # === LENSES ===
    if action.startswith("lens_"):
        lens_key = action.replace("lens_", "")
        if not user.get("last_experience"): return "Сначала расскажи опыт."
        if lens_key == "architect":
            metrics["architect_sessions"] = metrics.get("architect_sessions", 0) + 1
            user["state"] = STATE_ARCHITECT_ANALYSIS
            return "🧩 Архитектор. Назови три вещи:\n1. Что твоя Ось?\n2. Как устроена вторая сторона?\n3. Где линия разлома?"
        lens_key, lens_name, result = await apply_lens(lens_key, user["last_experience"])
        record_action(f"lens_{lens_key}")
        user.setdefault("used_lenses", []).append(lens_key)
        user["state"] = STATE_EXPERIENCE_RECEIVED
        world = user.get("user_world", {})
        timeline = world.get("timeline", [])
        if timeline and timeline[-1].get("status") == "open": timeline[-1]["status"] = "closing"
        consequence = apply_consequence(user, "observe")
        asyncio.create_task(safe_save())
        msg = f"Смотрю через «{lens_name}».\n\n{result}"
        if consequence: msg += f"\n\n{consequence}"
        msg += "\n\nЧто ты возьмёшь из этого в следующий раз?"
        await send_inline_keyboard(uid, msg, build_integration_keyboard())
        return None

    # === ARCHITECT ===
    if action == "architect_analysis_response":
        user["_architect_raw"] = user["last_experience"] + "\n\n" + text
        result = await call_llm([{"role": "system", "content": LENS_LIBRARY["architect"]["prompt"]}, {"role": "user", "content": user["_architect_raw"]}], max_tokens=2500)
        user["_architect_analysis"] = result; user["state"] = STATE_ARCHITECT_FORMULA
        return result + "\n\nСтоит эта конструкция?"

    if action == "architect_formula_response":
        if any(w in text.lower() for w in ["да", "стоит", "принимаю"]):
            user["state"] = STATE_ARCHITECT_STRATEGY
            return await call_llm([{"role": "system", "content": "Выдай алгоритм из 3 шагов."}, {"role": "user", "content": user.get("_architect_analysis", "")}], max_tokens=2000)
        user["state"] = STATE_ARCHITECT_ANALYSIS; return "Что именно не держится?"

    if action == "architect_strategy_response":
        user["state"] = STATE_EXPERIENCE_RECEIVED; user["_architect_raw"] = None; user["_architect_analysis"] = None
        return "Цикл Архитектора завершён. /menu для линз."

    # === ANCHOR / INTEGRATE ===
    if action == "anchor_response":
        user["pending_anchor_lens"] = None; user["state"] = STATE_EXPERIENCE_RECEIVED
        return "Я запомнил это. /menu для линз."

    if action == "manual_integrate":
        if not user.get("last_experience"): return "Сначала расскажи опыт."
        integration_text = await run_integrator(user)
        user["integration_count"] = user.get("integration_count", 0) + 1; user["state"] = STATE_EXPERIENCE_RECEIVED
        world = user.get("user_world", {}); timeline = world.get("timeline", [])
        if timeline and timeline[-1].get("status") in ("open", "closing"): timeline[-1]["status"] = "closed"
        consequence = apply_consequence(user, "integrate")
        asyncio.create_task(safe_save())
        msg = f"{integration_text}"
        if consequence: msg += f"\n\n{consequence}"
        msg += "\n\n/menu для линз."
        return msg

    return "🌫️ Что-то не проявилось. /menu"

# ================= WORKER =================
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

# ================= WEBHOOK =================
@app.post("/webhook")
async def webhook(req: Request, x_telegram_bot_api_secret_token: str = Header(None)):
    if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET: raise HTTPException(403)
    try: data = await req.json()
    except: return {"ok": True}
    callback = data.get("callback_query")
    if callback:
        cid = callback.get("id"); chat_id = callback["message"]["chat"]["id"]; dtext = callback.get("data", "")
        await answer_callback(cid)
        if dtext.startswith("cave:"): enqueue(chat_id, dtext)
        elif dtext.startswith("emotion:"): enqueue(chat_id, dtext.replace("emotion:", ""))
        elif dtext.startswith("control:"): enqueue(chat_id, dtext.replace("control:", ""))
        elif dtext.startswith("pattern:"): enqueue(chat_id, dtext.replace("pattern:", ""))
        elif dtext.startswith("guide:"):
            sub = dtext.replace("guide:", "")
            if sub == "no_lens": enqueue(chat_id, "без анализа")
            else: enqueue(chat_id, sub)
        elif dtext.startswith("integrate:"): enqueue(chat_id, dtext.replace("integrate:", ""))
        elif dtext == "/integrate": enqueue(chat_id, "/integrate")
        elif dtext == "/new": enqueue(chat_id, "/new")
        elif dtext in LENS_LIBRARY: enqueue(chat_id, dtext)
        return {"ok": True}
    msg = data.get("message") or data.get("edited_message") or {}
    if not msg: return {"ok": True}
    update_id = data.get("update_id")
    if update_id and dedup(update_id): return {"ok": True}
    chat_id = msg["chat"]["id"]
    save_user_to_memory(chat_id)
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
    return {"status": "ok", "users": len(users), "workers": len(workers), "metrics": metrics, "guide_sessions": metrics.get("guide_sessions", 0)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")), workers=1, log_level="info")
