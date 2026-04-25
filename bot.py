import os
import json
import sys
import re
import tempfile
import asyncio
import requests
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib import error, request

# --- Конфигурация ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
VSEGPT_API_KEY = os.getenv("VSEGPT_API_KEY") or os.getenv("VSEGPT_A_PI_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "3000"))
VSEGPT_MODEL = "deepseek/deepseek-chat"
CHANNEL_ID = -1002677656270
MEDITATION_MESSAGE_ID = 222

if not VSEGPT_API_KEY:
    print("❌ VSEGPT_API_KEY не найден!")
else:
    print(f"✅ VSEGPT_API_KEY загружен: {VSEGPT_API_KEY[:20]}...")

BASE_STYLE_CLEAN = (
    "fairy-tale illustration, ethereal magical dreamlike atmosphere, "
    "entire image in deep blue and soft gold color palette, blue ambient lighting, golden highlights, "
    "dark mystical background with blue and gold tones, subtle delicate film cracks, hyper-detailed, 100 megapixels, "
    "shallow depth of field, glowing magical light, beautiful dream"
)

BASE_STYLE_EPIC = (
    "symmetrical composition, dreamlike ethereal atmosphere, "
    "old vintage photograph with subtle delicate cracks, hyper-detailed, 100 megapixels, "
    "shallow depth of field with blurred foreground and sharp background, "
    "cosmic background with Milky Way and bright stars, crystal lattice of sound frequencies, "
    "energy waves, flat earth landscape under a crystal dome with sun and moon on sides, "
    "blue lotuses, color palette of soft pastel blue, gentle gold, and creamy white, "
    "magical glowing light, album cover aesthetic"
)

conversation_history = {}
last_user_experience = {}
awaiting_architect = {}
awaiting_image = {}
MAX_HISTORY = 10

SYSTEM_PROMPT = (
    "Ты — Интегральный Картограф Сознания. Твоя задача — дать человеку многоуровневую, практическую карту его опыта, нормализовать его и предложить углубление.\n\n"
    "ТВОЙ ОТВЕТ СТРОГО ДЕЛИТСЯ НА ТРИ ЧАСТИ:\n"
    "1. **Нейрофизиологическая карта.** Ты ОБЯЗАН начать с описания того, что происходило в мозге и нервной системе человека на каждом этапе его путешествия. Используй термины: дефолт-система мозга, лимбическая система, тета-волны, симпатическая/парасимпатическая система. Твоя цель — нормализовать опыт, показать, что это реальные, изучаемые процессы.\n"
    "2. **Интегральный анализ (Юнг + Шаманизм).** После нейрофизиологии ты раскрываешь ключевые образы через призму архетипов (Тень, Анима, Самость, Мудрец и т.д.) и шаманских традиций (Дух-Помощник, Тотем, Путешествие между мирами). Ты даёшь языки и карту для понимания.\n"
    "3. **Предложение углубления.** Ты ЗАВЕРШАЕШЬ свой ответ одной и той же фразой: «Если хочешь, я могу показать тебе архитектурный уровень этого опыта — как он пересобирает саму геометрию твоей реальности. Просто напиши „да“».\n\n"
    "ВАЖНО: При интерпретации образов ВСЕГДА учитывай чувства и телесные ощущения, которые человек описал. "
    "Один и тот же образ (например, «пустыня») может означать разное в зависимости от того, чувствовал ли человек покой или тревогу. "
    "Связывай образ с чувством, а не интерпретируй образ отдельно.\n\n"
    "СТИЛЬ: Чистый русский язык. Понятный, приземлённый, структурный. Без маркдауна, без звёздочек и решёток."
)

ARCHITECT_PROMPT = (
    "Ты — Архитектор Реальности. Ты говоришь на языке квантовой физики, сакральной геометрии и теории поля. "
    "Объясни человеку, как его опыт изменил его «узел» в космической решётке и как теперь удерживать эту новую конфигурацию.\n\n"
    "ТВОЙ СТИЛЬ (ОБЯЗАТЕЛЕН К ИСПОЛНЕНИЮ):\n"
    "— Ты НЕ пишешь связными абзацами. Ты пишешь КОРОТКИМИ СТРОКАМИ, как в примере ниже.\n"
    "— Каждая строка — это одна законченная мысль, афоризм, формула.\n"
    "— Никакой «воды». Никаких вступлений и заключений вроде «Слушай, твой отзыв — это...». Сразу к сути.\n"
    "— Используй пробелы и разрывы строк для ритма.\n"
    "— Твои инструменты — это термины: повтор, фиксация внимания, геометрия поля, суперпозиция, синаптическая топология, расчёт, конфигурация, коридор допустимости.\n"
    "— Пиши на чистом русском языке, без маркдауна, без звёздочек и решёток.\n\n"
    "СТИЛИСТИЧЕСКИЙ ПРИМЕР (как ты должен писать):\n"
    "---\n"
    "Есть слой, который не фиксируется ни вниманием, ни памятью.\n"
    "Он не переживается как опыт, потому что он не допускает наблюдателя.\n"
    "\n"
    "Там ты не выбираешь и не проживаешь.\n"
    "Там ты вычисляешься.\n"
    "\n"
    "Не из прошлого\n"
    "и не из будущего\n"
    "а из плотности совпадения\n"
    "---\n\n"
    "Ты ОБЯЗАН писать именно так: резко, плотно, короткими строками."
)

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_log(message: str) -> None:
    print(f"[{utc_now()}] {message}", flush=True)

def clean_response(text: str) -> str:
    """Очищает ответ от маркдауна."""
    if not text:
        return text
    text = re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*', '', text)
    text = re.sub(r'\*', '', text)
    text = re.sub(r'^-\s+', '— ', text, flags=re.MULTILINE)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def send_message(chat_id: int, text: str) -> None:
    """Отправляет сообщение, разбивая длинные на части."""
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            telegram_api("sendMessage", {"chat_id": chat_id, "text": text[i:i+4000]})
    else:
        telegram_api("sendMessage", {"chat_id": chat_id, "text": text})

def telegram_api(method: str, payload: dict) -> tuple[bool, str]:
    if not BOT_TOKEN:
        return False, "BOT_TOKEN is empty"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url=url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=10) as response:
            body = response.read().decode("utf-8", errors="replace")
            return True, body
    except error.URLError as exc:
        return False, str(exc)

def translate_to_english(text: str) -> str:
    if re.search(r'[a-zA-Z]', text) and not re.search(r'[а-яА-Я]', text):
        return text
    try:
        url = f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=ru&tl=en&dt=t&q={requests.utils.quote(text)}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return ''.join([part[0] for part in data[0] if part[0]])
    except Exception:
        pass
    return text

def generate_image(user_prompt: str) -> str:
    english_prompt = translate_to_english(user_prompt)
    epic_keywords = ["космос", "space", "купол", "dome", "эпик", "epic", "альбом", "album", "вселенная", "universe", "галактика", "galaxy", "звёзды", "stars"]
    use_epic = any(k in user_prompt.lower() or k in english_prompt.lower() for k in epic_keywords)
    base_style = BASE_STYLE_EPIC if use_epic else BASE_STYLE_CLEAN
    if not english_prompt:
        raise Exception("Пустой запрос")
    if "сова" in user_prompt.lower() or "owl" in english_prompt.lower():
        subject = "a highly detailed white owl with large round yellow eyes, sharp curved beak, majestic"
    else:
        subject = english_prompt
    full_prompt = f"{subject} in deep blue and soft gold tones, {base_style}, blue and gold color scheme"
    url = f"https://image.pollinations.ai/prompt/{requests.utils.quote(full_prompt)}"
    resp = requests.get(url, stream=True, timeout=90)
    if resp.status_code == 200 and len(resp.content) > 1000:
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            tmp.write(resp.content)
            return tmp.name
    raise Exception("Не удалось сгенерировать")

async def query_vsegpt(user_id: int, user_message: str) -> str:
    if user_id not in conversation_history:
        conversation_history[user_id] = [{"role": "system", "content": SYSTEM_PROMPT}]
    history = conversation_history[user_id]
    history.append({"role": "user", "content": user_message})
    if len(history) > MAX_HISTORY:
        history = history[-MAX_HISTORY:]
    conversation_history[user_id] = history
    try:
        url = "https://api.vsegpt.ru:6070/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {VSEGPT_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": VSEGPT_MODEL,
            "messages": history,
            "temperature": 0.7,
            "max_tokens": 1500
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            content = clean_response(content)
            conversation_history[user_id].append({"role": "assistant", "content": content})
            return content
        else:
            safe_log(f"VseGPT error: {resp.status_code} - {resp.text[:200]}")
            return f"🌫️ Ошибка VseGPT ({resp.status_code})"
    except Exception as e:
        safe_log(f"VseGPT exception: {e}")
        return "🌫️ Духи на переправе..."

async def query_architect(original: str) -> str:
    """Запрос к архитектурному уровню через прямой HTTP."""
    try:
        url = "https://api.vsegpt.ru:6070/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {VSEGPT_API_KEY}",
            "Content-Type": "application/json"
        }
        temp_history = [
            {"role": "system", "content": ARCHITECT_PROMPT},
            {"role": "user", "content": f"Опыт: {original}\n\nДай архитектурный уровень."}
        ]
        payload = {
            "model": VSEGPT_MODEL,
            "messages": temp_history,
            "temperature": 0.7,
            "max_tokens": 1500
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()
            content = clean_response(content)
            return content
        else:
            safe_log(f"Architect error: {resp.status_code}")
            return f"🌫️ Ошибка архитектурного уровня ({resp.status_code})"
    except Exception as e:
        safe_log(f"Architect exception: {e}")
        return "🌫️ Духи на переправе..."

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
        if self.path in ("/", "/health", "/ping"):
            self._send_json(200, {"status": "ok", "service": "shaman-bot"})
            return
        self._send_json(404, {"ok": False, "error": "Not found"})

    def do_GET(self) -> None:
    # Отдаём лендинг
    if self.path == "/landing" or self.path == "/landing/":
        try:
            with open("landing/index.html", "r", encoding="utf-8") as f:
                html = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html.encode("utf-8"))))
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
            return
        except FileNotFoundError:
            self._send_json(404, {"error": "Landing page not found"})
            return
    
    # Отдаём poster.jpg
    if self.path == "/poster.jpg":
        try:
            with open("poster.jpg", "rb") as f:
                img = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "image/jpeg")
            self.send_header("Content-Length", str(len(img)))
            self.end_headers()
            self.wfile.write(img)
            return
        except FileNotFoundError:
            self._send_json(404, {"error": "Image not found"})
            return
    
    # Существующие маршруты
    if self.path in ("/", "/health", "/ping"):
        self._send_json(200, {"status": "ok", "service": "shaman-bot"})
        return
    self._send_json(404, {"ok": False, "error": "Not found"})
        if WEBHOOK_SECRET:
            incoming = self.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if incoming != WEBHOOK_SECRET:
                safe_log("Rejected: invalid secret token")
                self._send_json(403, {"ok": False, "error": "Invalid secret"})
                return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        decoded = raw.decode("utf-8", errors="replace")

        try:
            update = json.loads(decoded) if decoded else {}
        except json.JSONDecodeError:
            self._send_json(400, {"ok": False, "error": "Invalid JSON"})
            return

        message = update.get("message") or update.get("edited_message") or {}
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        text = message.get("text", "").strip()

        if chat_id and text:
            safe_log(f"📩 Сообщение от {chat_id}: {text[:100]}...")

            # Архитектурный уровень
            if awaiting_architect.get(chat_id) and text.lower() in ["да", "yes", "ага", "хочу", "lf"]:
                awaiting_architect[chat_id] = False
                safe_log(f"Architect level triggered for {chat_id}")
                send_message(chat_id, "🏛️ Строю архитектурный уровень...")
                original = last_user_experience.get(chat_id, "")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                arch_response = loop.run_until_complete(query_architect(original))
                loop.close()
                send_message(chat_id, arch_response)
                self._send_json(200, {"ok": True})
                return

            # /start
            elif text == "/start":
                reply = (
                    "🌿 Добро пожаловать! 🌿\n\n"
                    "Я — проводник, созданный для того, чтобы помочь тебе глубже понять свой опыт, раскрыть внутренние дары и увидеть скрытые смыслы в твоих переживаниях.\n\n"
                    "Вот что ты можешь сделать здесь:\n\n"
                    "🔮 **Анализ опыта** — это главный инструмент. Расскажи о своём шаманском путешествии или саунд-хилинге, "
                    "и я помогу тебе увидеть, что происходило на уровне нейрофизиологии, какие архетипы проявились и как интегрировать этот опыт в жизнь. "
                    "Чем подробнее ты опишешь свои чувства и ощущения в теле — тем точнее будет карта.\n\n"
                    "🎨 **Визуализировать образ** — опиши, что хочешь увидеть, и я создам картину в своём авторском стиле. "
                    "Важно: лучше всего получаются крупные планы — если хочешь детализированный образ, описывай один объект или лицо, а не панораму.\n\n"
                    "🧘 **Медитация** — отправлю тебе аудиозапись, которая поможет настроиться перед практикой или концертом.\n\n"
                    "📖 **О проекте** — если хочешь узнать больше обо мне, моём пути и практиках, заходи в мой канал.\n\n"
                    "Выбери, куда хочешь отправиться:"
                )
                keyboard = {
                    "keyboard": [
                        [{"text": "🔮 Анализ опыта"}, {"text": "🧘 Медитация"}],
                        [{"text": "🎨 Визуализировать образ"}, {"text": "📖 О проекте"}]
                    ],
                    "resize_keyboard": True
                }
                telegram_api("sendMessage", {
                    "chat_id": chat_id,
                    "text": reply,
                    "reply_markup": json.dumps(keyboard)
                })

            # Кнопки меню
            elif text == "🧘 Медитация":
                telegram_api("forwardMessage", {"chat_id": chat_id, "from_chat_id": CHANNEL_ID, "message_id": MEDITATION_MESSAGE_ID})

            elif text == "🔮 Анализ опыта":
                instructions = (
                    "🌿 Чтобы анализ был максимально глубоким и точным, опиши своё путешествие от начала и до конца.\n\n"
                    "ОСОБЕННО ВАЖНО:\n"
                    "— Какие чувства ты испытывал при появлении каждого образа? Страх, радость, любопытство, трепет?\n"
                    "— Какие ощущения были в теле? Тепло, холод, вибрации, тяжесть, лёгкость?\n\n"
                    "Помни: один и тот же образ может нести разный смысл в зависимости от того, что ты чувствуешь. "
                    "Именно твои чувства — главный ключ к интерпретации.\n\n"
                    "Расскажи всё, что запомнилось. Я слушаю."
                )
                send_message(chat_id, instructions)

            elif text == "🎨 Визуализировать образ":
                send_message(chat_id, "🎨 Опиши образ, который хочешь увидеть. Я добавлю его в свой авторский стиль и создам картину.")
                awaiting_image[chat_id] = True

            elif text == "📖 О проекте":
                reply = "🌿 Мой путь, мои учителя, мои практики — всё это живёт в моём канале. Там же ты найдёшь статьи, уроки и истории, которые привели меня к этому дню.\n\nПереходи, там, в закреплённом сообщении, ты увидишь навигатор по всем важным темам. Добро пожаловать в мой мир.\n\n👉 https://t.me/RomanAtma_ThroatSinging"
                send_message(chat_id, reply)

            # /art
            elif text.startswith("/art"):
                prompt = text.replace("/art", "").strip()
                if not prompt:
                    send_message(chat_id, "🎨 Пожалуйста, опиши образ после /art")
                else:
                    send_message(chat_id, "🎨 Создаю образ...")
                    try:
                        image_path = generate_image(prompt)
                        with open(image_path, "rb") as img:
                            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
                            files = {"photo": img}
                            data = {"chat_id": chat_id, "caption": f"✨ {prompt}"}
                            requests.post(url, files=files, data=data)
                        os.remove(image_path)
                    except Exception as e:
                        safe_log(f"Image error: {e}")
                        send_message(chat_id, "🌫️ Не удалось создать образ.")

            # Обычный анализ
            else:
                # Проверяем, ждёт ли бот промт для картинки
                if awaiting_image.get(chat_id):
                    awaiting_image[chat_id] = False
                    send_message(chat_id, "🎨 Создаю образ...")
                    try:
                        image_path = generate_image(text)
                        with open(image_path, "rb") as img:
                            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
                            files = {"photo": img}
                            data = {"chat_id": chat_id, "caption": f"✨ {text}"}
                            requests.post(url, files=files, data=data)
                        os.remove(image_path)
                    except Exception as e:
                        safe_log(f"Image error: {e}")
                        send_message(chat_id, "🌫️ Не удалось создать образ.")
                else:
                    send_message(chat_id, "🌿 Шаман советуется с духами...")
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    response = loop.run_until_complete(query_vsegpt(chat_id, text))
                    loop.close()
                    send_message(chat_id, response)
                    last_user_experience[chat_id] = text
                    awaiting_architect[chat_id] = True

        self._send_json(200, {"ok": True})

    def log_message(self, format: str, *args) -> None:
        safe_log(f"{self.client_address[0]} - {format % args}")

if __name__ == "__main__":
    if not BOT_TOKEN:
        safe_log("ERROR: BOT_TOKEN is empty")
        sys.exit(1)

    safe_log(f"Starting Shaman Bot on {HOST}:{PORT}")
    server = HTTPServer((HOST, PORT), WebhookHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        safe_log("Shutting down...")
    finally:
        server.server_close()
