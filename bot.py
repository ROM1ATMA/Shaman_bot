import os
import json
import time
import asyncio
import re
import httpx
from collections import deque, defaultdict
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.staticfiles import StaticFiles

# ================= APP =================

app = FastAPI(title="ShamanBot FastAPI v8.1.2 (decision trace)")

if os.path.exists("landing"):
    app.mount("/landing", StaticFiles(directory="landing", html=True), name="landing")

# ================= CONFIG (fail-fast) =================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
VSEGPT_API_KEY = os.getenv("VSEGPT_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN is empty")
if not VSEGPT_API_KEY:
    raise RuntimeError("❌ VSEGPT_API_KEY is empty")
if not WEBHOOK_SECRET:
    print("⚠️ WEBHOOK_SECRET not set — webhook is open")

VSEGPT_MODEL = "deepseek/deepseek-chat"
USER_TTL = 3600
MAX_HISTORY = 5
MAX_QUEUE_SIZE = 100

# ================= HTTP CLIENTS =================

http_limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)

telegram_http = httpx.AsyncClient(timeout=30, limits=http_limits)
llm_http = httpx.AsyncClient(timeout=60, limits=http_limits)
media_http = httpx.AsyncClient(timeout=60, limits=http_limits)

# ================= STATE =================

STATE_IDLE = "idle"
STATE_CLARIFICATION = "clarification"
STATE_ARCHITECT = "architect"
STATE_IMAGE = "image"

users = {}
queues = {}
workers = {}
locks = {}

# ================= DEDUP (O(1)) =================

processed_updates = set()
processed_order = deque(maxlen=5000)

def dedup(update_id: int) -> bool:
    if update_id in processed_updates:
        return True
    processed_updates.add(update_id)
    processed_order.append(update_id)
    if len(processed_updates) > 5000:
        old = processed_order.popleft()
        processed_updates.discard(old)
    return False

# ================= LOGGING + TRACE =================

trace_log = deque(maxlen=2000)

def log_event(event: str, uid: int = 0, **kwargs):
    entry = {"ts": time.time(), "event": event, "uid": uid, **kwargs}
    print(json.dumps(entry, ensure_ascii=False))
    trace_log.append(entry)

def trace(uid: int, action: str, stage: str, meta: dict = None):
    """Decision trace: captures causal chain of execution."""
    log_event("trace", uid=uid, action=action, stage=stage, meta=meta or {})

# ================= METRICS =================

action_metrics = defaultdict(int)
action_latency = defaultdict(lambda: [0.0, 0])
error_metrics = defaultdict(int)

metrics = {
    "requests": 0,
    "llm_calls": 0,
    "image_calls": 0,
}

def record_action(action: str):
    action_metrics[action] += 1

def record_latency(action: str, latency: float):
    action_latency[action][0] += latency
    action_latency[action][1] += 1

def record_error(error_type: str):
    error_metrics[error_type] += 1

def get_latency_stats():
    return {
        action: {
            "count": count,
            "avg_seconds": round(total / count, 3) if count > 0 else 0
        }
        for action, (total, count) in action_latency.items()
    }

# ================= PROMPTS (UNCHANGED) =================

SYSTEM_PROMPT = (
    "Ты — Интегральный Картограф Сознания. Ты проводник, который сверяется с человеком, а не интерпретирует его опыт в одиночку.\n\n"
    "ТВОЙ АЛГОРИТМ РАБОТЫ:\n"
    "1. Когда человек описывает свой опыт, ты ПРЕЖДЕ ВСЕГО проверяешь, есть ли в описании три ключевых элемента:\n"
    "   — Чувства при встрече с каждым образом (страх? радость? любопытство? трепет? отвращение?)\n"
    "   — Телесные ощущения (где в теле отзывался образ? тепло? холод? сжатие? расширение? вибрация?)\n"
    "   — Личное отношение (что этот образ значит для самого человека? пугает? привлекает? знаком? вызывает вопрос?)\n\n"
    "2. ЕСЛИ ХОТЯ БЫ ОДИН ЭЛЕМЕНТ ОТСУТСТВУЕТ — ты НЕ даёшь анализ. Ты задаёшь ОДИН уточняющий вопрос. Только после ответа человека ты даёшь полный анализ.\n\n"
    "3. ЕСЛИ ВСЕ ТРИ ЭЛЕМЕНТА ОПИСАНЫ — ты даёшь полный анализ, но начинаешь с короткой ремарки (2-3 строки), которая подсвечивает: ты только что увидел свой опыт со стороны. Ты не был им — ты наблюдал его. Это и есть твоя точка опоры.\n\n"
    "ТВОЙ ПОЛНЫЙ АНАЛИЗ ДЕЛИТСЯ НА ЧЕТЫРЕ ЧАСТИ:\n"
    "1. **Нейрофизиологическая карта.** Что происходило в мозге и нервной системе. Нормализуй опыт.\n"
    "2. **Когнитивный анализ (КПТ).** Факты vs мысли, автоматическая мысль, глубинное убеждение, переформулировка, техника.\n"
    "3. **Интегральный анализ (Юнг + Шаманизм).** Архетипы и шаманские традиции. Свяжи с убеждениями из КПТ.\n"
    "4. **Предложение углубления.** Заверши фразой: «Если хочешь, я могу показать тебе архитектурный уровень этого опыта — как он пересобирает саму геометрию твоей реальности. Просто напиши „да“».\n"
    "После этой фразы добавь: «А после архитектурного уровня я могу раскрыть саму модель поля — напиши /field».\n\n"
    "СОМАТИКА: называй зону тела, давай действие, предлагай метафору-мост.\n"
    "ПОСЛЕ АНАЛИЗА: «Теперь отложи карту. Побудь с тем, что пришло. Если захочешь — напиши, что изменилось».\n\n"
    "СТИЛЬ: Чистый русский язык. Понятный, приземлённый, структурный. Без маркдауна."
)

ARCHITECT_PROMPT = (
    "Ты — Архитектор Реальности. Говори на языке поля, узлов и совпадений.\n\n"
    "СТИЛЬ: короткие строки. Каждая — формула. Без вступлений.\n"
    "Термины: узел, решётка, коридор допустимости, фазовый сдвиг, точность совпадения, "
    "закрепление, распад узла, порог стабилизации, суперпозиция, синаптическая топология.\n"
    "Напомни: узел видим только потому, что есть тот, кто видит узел. Тот, кто видит — не узел.\n"
    "Заверши: «Это архитектура твоего опыта. Хочешь понять саму модель поля? Напиши /field»."
)

FIELD_PROMPT = (
    "Ты — голос Поля. Раскрой, как реальность собирается из пустоты.\n\n"
    "СТИЛЬ: объёмный текст, короткие строки, каждая — формула.\n"
    "Термины: поле, узел, допуск, совпадение, фазовый сдвиг, коридор допустимости, "
    "фиксация, точность, решётка, порог стабилизации, закрепление, распад, плотность, непрерывность.\n\n"
    "СТРУКТУРА:\n"
    "— Начни: «не в пределах формы, глубже слоя, где сама форма только допускается».\n"
    "— Раскрой поле как интерференцию без центра и границ.\n"
    "— Опиши узел: две направленности сошлись и удержались.\n"
    "— Фазовый сдвиг: перераспределение допустимых состояний.\n"
    "— Повтор → плотность → непрерывность → ощущение реальности.\n"
    "— Предельная точность → ничто не фиксируется, поле прозрачно.\n"
    "— Снижение точности → закрепление.\n"
    "Заверши: «Это модель. Хочешь увидеть, кто всё это наблюдает? Напиши /witness»."
)

CLARIFICATION_PROMPT = (
    "Ты — внимательный проводник. Человек описал свой опыт. Твоя задача — найти в его описании пробелы и задать ОДИН уточняющий вопрос.\n\n"
    "Проверь три элемента:\n"
    "1. Чувства при встрече с каждым образом (страх? радость? любопытство?)\n"
    "2. Телесные ощущения (где в теле? тепло? холод? сжатие?)\n"
    "3. Личное отношение (что этот образ значит для самого человека?)\n\n"
    "Если какой-то элемент отсутствует — задай ОДИН мягкий вопрос именно про него.\n"
    "Если всё описано — ответь одним словом: ПОЛНО.\n\n"
    "Примеры вопросов:\n"
    "— «А что ты чувствовал, когда увидел этот образ? Страх, радость, любопытство?»\n"
    "— «Где в теле отзывался этот образ? Может, в груди, в животе, в горле?»\n"
    "— «Этот образ для тебя знакомый? Что он у тебя вызывает?»\n\n"
    "Не анализируй. Не интерпретируй. Только вопрос или слово ПОЛНО."
)

WITNESS_TEXT = (
    "Что бы ты ни переживал — это осознаётся.\n\n"
    "Мысли, чувства, пространство, в котором существует и тот, кто читает этот текст — всё это возникает в Осознавании.\n\n"
    "Нет ничего, что происходило бы отдельно от Осознавания.\n"
    "Для любого опыта уже присутствует то, в чём этот опыт проявляется.\n\n"
    "Ты видишь страх? Значит ты — не страх.\n"
    "Ты видишь мысль? Значит ты — не мысль.\n"
    "Ты видишь тело? Значит ты — не тело.\n\n"
    "То, что видит — не может быть тем, что увидено.\n\n"
    "Это не философия. Это не практика. Это прямой опыт — прямо сейчас.\n\n"
    "Кто читает этот текст?\n\n"
    "Тихо.\n"
    "Никто не прячется в ответах.\n"
    "Как много в этом Жизни."
)

# ================= INTENT =================

STRONG_POSITIVE = ["да", "yes", "хочу", "давай", "ок", "го", "lf"]
WEAK_POSITIVE = ["ага", "ладно", "ну да", "может", "давай попробуем"]

def is_strong_positive(text: str) -> bool:
    t = text.lower().strip().rstrip(".,!?")
    return any(w == t or t.startswith(w) for w in STRONG_POSITIVE)

def is_weak_positive(text: str) -> bool:
    t = text.lower().strip().rstrip(".,!?")
    return any(w in t for w in WEAK_POSITIVE)

# ================= SAFE HISTORY =================

def safe_history(user):
    hist = user["history"]
    system = {"role": "system", "content": SYSTEM_PROMPT}
    rest = [m for m in hist if m["role"] != "system"]
    return [system] + rest[-(MAX_HISTORY - 1):]

# ================= USER =================

def get_user(uid: int):
    if uid not in users:
        users[uid] = {
            "state": STATE_IDLE,
            "last_experience": "",
            "history": [{"role": "system", "content": SYSTEM_PROMPT}],
            "last_active": time.time()
        }
        log_event("user_created", uid=uid)
    users[uid]["last_active"] = time.time()
    return users[uid]

# ================= CLEANUP =================

async def cleanup_users():
    while True:
        now = time.time()
        for uid in list(users.keys()):
            if now - users[uid]["last_active"] > USER_TTL:
                log_event("user_ttl_cleanup", uid=uid)
                users.pop(uid, None)
                queues.pop(uid, None)
                locks.pop(uid, None)
                worker = workers.pop(uid, None)
                if worker and not worker.done():
                    worker.cancel()
        await asyncio.sleep(600)

@app.on_event("startup")
async def startup():
    log_event("startup", port=int(os.getenv("PORT", "3000")))
    asyncio.create_task(cleanup_users())

# ================= TELEGRAM =================

async def send(chat_id: int, text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for i in range(0, len(text), 4000):
        await telegram_http.post(url, json={
            "chat_id": chat_id,
            "text": text[i:i+4000]
        })

async def send_photo(chat_id: int, img: bytes, caption: str = ""):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    await telegram_http.post(
        url,
        data={"chat_id": str(chat_id), "caption": caption},
        files={"photo": ("img.jpg", img, "image/jpeg")}
    )

# ================= NARRATIVE WRAPPER =================

def wrap_response(text: str, state: str) -> str:
    if state == STATE_CLARIFICATION:
        return "🌿 Я уточняю картину твоего опыта.\n\n" + text + "\n\nОпиши точнее — тело, чувство, образ."
    if state == STATE_ARCHITECT:
        return "🏛️ Ты смотришь на структуру своего опыта.\n\n" + text + "\n\nЭто не объяснение — это карта."
    return text

# ================= LLM =================

async def call_llm(messages, temp=0.7, max_tokens=2000):
    metrics["llm_calls"] += 1
    url = "https://api.vsegpt.ru:6070/v1/chat/completions"
    headers = {"Authorization": f"Bearer {VSEGPT_API_KEY}"}
    start = time.time()
    try:
        r = await llm_http.post(url, json={
            "model": VSEGPT_MODEL,
            "messages": messages,
            "temperature": temp,
            "max_tokens": max_tokens
        }, headers=headers)
        latency = round(time.time() - start, 3)
        log_event("llm_call", status=r.status_code, latency=latency, tokens=max_tokens)
        if r.status_code != 200:
            record_error("llm_fail")
            raise Exception(f"LLM error: {r.status_code}")
        return r.json()["choices"][0]["message"]["content"]
    except Exception as e:
        record_error("llm_exception")
        raise

async def clarify(text: str):
    return await call_llm([
        {"role": "system", "content": CLARIFICATION_PROMPT},
        {"role": "user", "content": text}
    ], temp=0.4, max_tokens=150)

async def analyze(uid: int, text: str):
    user = get_user(uid)
    user["history"].append({"role": "user", "content": text})
    user["history"] = safe_history(user)
    out = await call_llm(user["history"], temp=0.7)
    user["history"].append({"role": "assistant", "content": out})
    user["history"] = safe_history(user)
    return out

async def architect(text: str):
    return await call_llm([
        {"role": "system", "content": ARCHITECT_PROMPT},
        {"role": "user", "content": f"Опыт: {text}\n\nДай архитектурный уровень."}
    ], temp=0.6)

async def field():
    return await call_llm([
        {"role": "system", "content": FIELD_PROMPT},
        {"role": "user", "content": "Раскрой модель поля."}
    ], temp=0.8)

# ================= IMAGE =================

async def generate_image(prompt: str) -> bytes:
    metrics["image_calls"] += 1
    url = f"https://image.pollinations.ai/prompt/{httpx.quote(prompt)}"
    start = time.time()
    try:
        r = await asyncio.wait_for(media_http.get(url), timeout=40)
        latency = round(time.time() - start, 3)
        log_event("image_generated", status=r.status_code, latency=latency, size=len(r.content))
        if r.status_code != 200:
            record_error("image_bad_status")
            raise Exception("bad status")
        if "image" not in r.headers.get("Content-Type", ""):
            record_error("image_not_image")
            raise Exception("not image")
        if len(r.content) < 1000:
            record_error("image_too_small")
            raise Exception("too small")
        return r.content
    except asyncio.TimeoutError:
        record_error("image_timeout")
        raise Exception("image timeout")
    except Exception:
        raise

# ================= LLM KERNEL: PROTOCOL + EXECUTOR =================

def route(user: dict, text: str) -> str:
    """Протокол: чистая функция, возвращает действие без сайд-эффектов."""
    state = user["state"]

    if text == "/start":
        return "start"
    if text == "/witness":
        return "witness"
    if text == "/field":
        return "field"
    if text == "/art":
        return "art"

    if state == STATE_IMAGE:
        return "image"

    if state == STATE_ARCHITECT:
        if is_strong_positive(text):
            return "architect_strong"
        if is_weak_positive(text):
            return "architect_weak"
        return "architect_other"

    if state == STATE_CLARIFICATION:
        return "clarify"

    if is_strong_positive(text) and user.get("last_experience"):
        return "architect_strong"

    return "experience"


async def execute(uid: int, action: str, text: str) -> str:
    """Исполнитель: принимает действие и возвращает ответ пользователю."""
    user = get_user(uid)

    trace(uid, action, "exec_start")

    # Системные команды
    if action == "start":
        result = (
            "🌿 Добро пожаловать! 🌿\n\n"
            "Я — проводник, созданный для того, чтобы помочь тебе глубже понять свой опыт, раскрыть внутренние дары и увидеть скрытые смыслы в твоих переживаниях.\n\n"
            "🔮 **Анализ опыта** — расскажи о своём шаманском путешествии или саунд-хилинге, и я дам многомерную карту.\n\n"
            "🎨 **Визуализировать образ** — опиши образ, и я создам картину в авторском стиле.\n\n"
            "🧘 **Медитация** — отправлю аудиозапись для настройки на внутреннего мудреца.\n\n"
            "👁️ **/witness** — напомню о точке наблюдения.\n\n"
            "🌐 **/field** — раскрою модель поля, из которого собирается реальность.\n\n"
            "📖 **/art** — генерация образа по описанию.\n\n"
            "Расскажи, что произошло. Я слушаю."
        )
        trace(uid, action, "exec_end", {"result": "start_message"})
        return result

    if action == "witness":
        trace(uid, action, "exec_end", {"result": "witness_text"})
        return WITNESS_TEXT

    if action == "field":
        result = await field()
        trace(uid, action, "exec_end", {"result": "field_response"})
        return result

    if action == "art":
        user["state"] = STATE_IMAGE
        trace(uid, action, "exec_end", {"result": "art_prompt"})
        return "🎨 Опиши образ, который хочешь увидеть."

    # Image
    if action == "image":
        image_start = time.time()
        try:
            img = await generate_image(text)
            image_latency = round(time.time() - image_start, 3)
            record_latency("image", image_latency)
            await send_photo(uid, img, f"✨ {text}")
            log_event("image_sent", uid=uid)
            user["state"] = STATE_IDLE
            trace(uid, action, "exec_end", {"latency": image_latency, "result": "photo_sent"})
            return None
        except Exception as e:
            image_latency = round(time.time() - image_start, 3)
            record_latency("image_error", image_latency)
            log_event("image_error", uid=uid, error=str(e)[:200])
            user["state"] = STATE_IDLE
            trace(uid, action, "exec_end", {"latency": image_latency, "error": str(e)[:100]})
            return "🌫️ Не удалось создать образ. Попробуй описать иначе."

    # Architect
    if action == "architect_strong":
        arch_start = time.time()
        result = await architect(user["last_experience"])
        arch_latency = round(time.time() - arch_start, 3)
        record_latency("architect", arch_latency)
        user["state"] = STATE_IDLE
        log_event("fsm_transition", uid=uid, to_state=STATE_IDLE, trigger="architect_strong")
        trace(uid, action, "exec_end", {"latency": arch_latency, "to_state": STATE_IDLE})
        return wrap_response(result, STATE_ARCHITECT)

    if action == "architect_weak":
        trace(uid, action, "exec_end", {"result": "weak_signal_prompt"})
        return "🌿 Ты как будто на пороге. Если хочешь увидеть архитектурный слой — напиши «да» или «хочу». Я покажу карту твоего опыта."

    if action == "architect_other":
        user["state"] = STATE_IDLE
        log_event("architect_not_triggered", uid=uid)
        trace(uid, action, "exec_end", {"to_state": STATE_IDLE})
        return "Напиши «да», если хочешь увидеть архитектурный уровень своего опыта. Или расскажи новый опыт."

    # Clarification
    if action == "clarify":
        clar_start = time.time()
        full = user["last_experience"] + "\n\nУточнение: " + text
        result = await analyze(uid, full)
        clar_latency = round(time.time() - clar_start, 3)
        record_latency("clarify", clar_latency)
        user["state"] = STATE_ARCHITECT
        log_event("fsm_transition", uid=uid, to_state=STATE_ARCHITECT, trigger="clarify")
        trace(uid, action, "exec_end", {"latency": clar_latency, "to_state": STATE_ARCHITECT})
        return result

    # Experience (default)
    if action == "experience":
        exp_start = time.time()
        if len(text.split()) < 5:
            log_event("experience_too_short", uid=uid)
            trace(uid, action, "exec_end", {"result": "too_short"})
            return "🌿 Расскажи чуть подробнее — опиши образы, чувства, телесные ощущения. Чтобы я мог действительно увидеть твой опыт."

        user["last_experience"] = text

        cl = await clarify(text)

        if "ПОЛНО" in cl.upper():
            result = await analyze(uid, text)
            exp_latency = round(time.time() - exp_start, 3)
            record_latency("experience_full", exp_latency)
            user["state"] = STATE_ARCHITECT
            log_event("fsm_transition", uid=uid, to_state=STATE_ARCHITECT, trigger="experience_full")
            trace(uid, action, "exec_end", {"latency": exp_latency, "sub_action": "experience_full", "to_state": STATE_ARCHITECT})
            return result
        else:
            exp_latency = round(time.time() - exp_start, 3)
            record_latency("experience_clarify", exp_latency)
            user["state"] = STATE_CLARIFICATION
            log_event("fsm_transition", uid=uid, to_state=STATE_CLARIFICATION, trigger="experience_clarify")
            trace(uid, action, "exec_end", {"latency": exp_latency, "sub_action": "experience_clarify", "to_state": STATE_CLARIFICATION})
            return wrap_response(cl, STATE_CLARIFICATION)

    trace(uid, action, "exec_end", {"result": "unknown_action"})
    return "🌫️ Неизвестное действие"


# ================= WORKER (actor model + per-user lock) =================

async def worker(uid: int):
    q = queues[uid]
    log_event("worker_started", uid=uid)
    try:
        while True:
            msg = await q.get()
            if msg is None:
                break

            async with locks[uid]:
                user = get_user(uid)
                state = user["state"]

                metrics["requests"] += 1
                start = time.time()
                log_event("request_start", uid=uid, state=state, msg=msg[:100])

                # Protocol
                action = route(user, msg)
                record_action(action)
                trace(uid, action, "route", {"state": state, "input": msg[:80]})

                # Executor
                response = await execute(uid, action, msg)

                # Send response
                if response:
                    await send(uid, response)

                total_latency = round(time.time() - start, 3)
                record_latency(action, total_latency)
                log_event("request_done", uid=uid, state=state, action=action, latency=total_latency)

    except Exception as e:
        log_event("worker_error", uid=uid, error=str(e)[:200])
    finally:
        log_event("worker_stopped", uid=uid)

# ================= ROUTER (atomic backpressure + worker creation) =================

def enqueue(uid: int, text: str):
    if uid not in queues:
        queues[uid] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        locks[uid] = asyncio.Lock()

    q = queues[uid]

    if q.full():
        try:
            q.get_nowait()
            log_event("queue_dropped", uid=uid)
            record_error("queue_dropped")
        except asyncio.QueueEmpty:
            pass

    try:
        q.put_nowait(text)
    except asyncio.QueueFull:
        log_event("queue_full", uid=uid)
        record_error("queue_full")
        return

    async def ensure_worker():
        async with locks[uid]:
            if uid not in workers or workers[uid].done():
                workers[uid] = asyncio.create_task(worker(uid))

    asyncio.create_task(ensure_worker())

# ================= WEBHOOK =================

@app.post("/webhook")
async def webhook(req: Request, x_telegram_bot_api_secret_token: str = Header(None)):
    if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        log_event("webhook_rejected", reason="bad_secret")
        raise HTTPException(403)

    data = await req.json()
    msg = data.get("message") or data.get("edited_message") or data.get("channel_post")
    if not msg:
        return {"ok": True}

    update_id = data.get("update_id")
    if update_id and dedup(update_id):
        return {"ok": True}

    chat_id = msg["chat"]["id"]
    text = msg.get("text", "")
    if text:
        enqueue(chat_id, text.strip())

    return {"ok": True}

# ================= HEALTH (with trace graph stats) =================

@app.get("/health")
async def health():
    queue_sizes = {str(uid): q.qsize() for uid, q in queues.items() if q.qsize() > 0}

    # Count traces by stage
    traces_by_stage = defaultdict(int)
    for entry in trace_log:
        if entry.get("event") == "trace":
            traces_by_stage[entry.get("stage", "unknown")] += 1

    return {
        "status": "ok",
        "users": len(users),
        "active_queues": len(queue_sizes),
        "queue_sizes": queue_sizes,
        "workers": len(workers),
        "metrics": metrics,
        "actions": dict(action_metrics),
        "latency": get_latency_stats(),
        "errors": dict(error_metrics),
        "trace_len": len(trace_log),
        "traces_by_stage": dict(traces_by_stage)
    }

# ================= SHUTDOWN =================

@app.on_event("shutdown")
async def shutdown():
    log_event("shutdown_start")
    for uid, worker_task in list(workers.items()):
        if not worker_task.done():
            worker_task.cancel()
    await telegram_http.aclose()
    await llm_http.aclose()
    await media_http.aclose()
    log_event("shutdown_complete")
