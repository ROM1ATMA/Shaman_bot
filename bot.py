import os
import json
import sys
import re
import tempfile
import asyncio
import openai
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

if VSEGPT_API_KEY:
    openai.api_base = "https://api.vsegpt.ru:6070/v1"
    openai.api_key = VSEGPT_API_KEY

conversation_history = {}
last_user_experience = {}
awaiting_architect = {}
MAX_HISTORY = 10

SYSTEM_PROMPT = (
    "Ты — Интегральный Картограф Сознания. Твоя задача — дать человеку многоуровневую, практическую карту его опыта, нормализовать его и предложить углубление.\n\n"
    "ТВОЙ ОТВЕТ СТРОГО ДЕЛИТСЯ НА ТРИ ЧАСТИ:\n"
    "1. **Нейрофизиологическая карта.** Ты ОБЯЗАН начать с описания того, что происходило в мозге и нервной системе человека на каждом этапе его путешествия. Используй термины: дефолт-система мозга, лимбическая система, тета-волны, симпатическая/парасимпатическая система. Твоя цель — нормализовать опыт, показать, что это реальные, изучаемые процессы.\n"
    "2. **Интегральный анализ (Юнг + Шаманизм).** После нейрофизиологии ты раскрываешь ключевые образы через призму архетипов (Тень, Анима, Самость, Мудрец и т.д.) и шаманских традиций (Дух-Помощник, Тотем, Путешествие между мирами). Ты даёшь языки и карту для понимания.\n"
    "3. **Предложение углубления.** Ты ЗАВЕРШАЕШЬ свой ответ одной и той же фразой: «Если хочешь, я могу показать тебе архитектурный уровень этого опыта — как он пересобирает саму геометрию твоей реальности. Просто напиши „да“».\n\n"
    "СТИЛЬ: Чистый русский язык. Понятный, приземлённый, структурный. Без маркдауна."
)

ARCHITECT_PROMPT = (
    "Ты — Архитектор Реальности. Ты говоришь на языке квантовой физики, сакральной геометрии и теории поля. "
    "Объясни человеку, как его опыт изменил его «узел» в космической решётке и как теперь удерживать эту новую конфигурацию. "
    "Используй термины: повтор, фиксация внимания, геометрия поля, суперпозиция, синаптическая топология. "
    "Стиль: точный, ёмкий, без воды."
)

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_log(message: str) -> None:
    print(f"[{utc_now()}] {message}", flush=True)

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
        response = await openai.ChatCompletion.acreate(model=VSEGPT_MODEL, messages=history, temperature=0.7, max_tokens=1500)
        content = response["choices"][0]["message"]["content"].strip()
        conversation_history[user_id].append({"role": "assistant", "content": content})
        return content
    except Exception as e:
        safe_log(f"VseGPT error: {e}")
        return "🌫️ Духи на переправе..."

class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/6.0"

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

    def do_POST(self) -> None:
        if self.path != "/webhook":
            self._send_json(404, {"ok": False, "error": "Not found"})
            return

        if WEBHOOK_SECRET:
            incoming = self.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if incoming != WEBHOOK_SECRET:
                safe_log("Rejected: invalid secret token")
                self._send_json(403, {"ok": False, "error": "Invalid secret"})
                return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        decoded = raw.decode("utf-8", errors="replace")
        safe_log(f"Webhook received: {decoded[:200]}...")

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
            # Проверка на архитектурный уровень
            if awaiting_architect.get(chat_id) and text.lower() in ["да", "yes", "ага", "хочу", "lf"]:
                awaiting_architect[chat_id] = False
                safe_log(f"Architect level triggered for {chat_id}")
                telegram_api("sendMessage", {"chat_id": chat_id, "text": "🏛️ Строю архитектурный уровень..."})
                
                original = last_user_experience.get(chat_id, text)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    temp_history = [
                        {"role": "system", "content": ARCHITECT_PROMPT},
                        {"role": "user", "content": f"Опыт: {original}\n\nДай архитектурный уровень."}
                    ]
                    response = loop.run_until_complete(
                        openai.ChatCompletion.acreate(model=VSEGPT_MODEL, messages=temp_history, temperature=0.7, max_tokens=1500)
                    )
                    arch_response = response["choices"][0]["message"]["content"].strip()
                except Exception as e:
                    arch_response = f"🌫️ Ошибка архитектурного уровня: {e}"
                loop.close()
                telegram_api("sendMessage", {"chat_id": chat_id, "text": arch_response})
                self._send_json(200, {"ok": True})
                return

            # Команда /start с кнопочным меню
            elif text == "/start":
                reply = "🌿 Приветствую, путник! 🌿\n\nЯ — проводник в мир духов. Расскажи о своём опыте саунд-хилинга или шаманского путешествия, и я помогу тебе увидеть его глубину и интегрировать полученные дары.\n\nВыбери, куда хочешь отправиться:"
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

            # Обработка кнопок меню
            elif text == "🧘 Медитация":
                telegram_api("forwardMessage", {"chat_id": chat_id, "from_chat_id": CHANNEL_ID, "message_id": MEDITATION_MESSAGE_ID})

            elif text == "🔮 Анализ опыта":
                telegram_api("sendMessage", {"chat_id": chat_id, "text": "🌿 Поведай мне о своём путешествии. Опиши образы, ощущения, эмоции — всё, что запомнилось. Я помогу тебе увидеть глубину этого опыта."})

            elif text == "🎨 Визуализировать образ":
                telegram_api("sendMessage", {"chat_id": chat_id, "text": "🎨 Опиши образ, который хочешь увидеть. Я добавлю его в свой авторский стиль и создам картину."})

            elif text == "📖 О проекте":
                reply = "🌿 Мой путь, мои учителя, мои практики — всё это живёт в моём канале. Там же ты найдёшь статьи, уроки и истории, которые привели меня к этому дню.\n\nПереходи, там, в закреплённом сообщении, ты увидишь навигатор по всем важным темам. Добро пожаловать в мой мир.\n\n👉 https://t.me/RomanAtma_ThroatSinging"
                telegram_api("sendMessage", {"chat_id": chat_id, "text": reply})

            # Команда /art
            elif text.startswith("/art"):
                prompt = text.replace("/art", "").strip()
                if not prompt:
                    telegram_api("sendMessage", {"chat_id": chat_id, "text": "🎨 Пожалуйста, опиши образ после /art"})
                else:
                    telegram_api("sendMessage", {"chat_id": chat_id, "text": "🎨 Создаю образ..."})
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
                        telegram_api("sendMessage", {"chat_id": chat_id, "text": "🌫️ Не удалось создать образ."})

            # Обычный анализ
            else:
                telegram_api("sendMessage", {"chat_id": chat_id, "text": "🌿 Шаман советуется с духами..."})
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                response = loop.run_until_complete(query_vsegpt(chat_id, text))
                loop.close()
                telegram_api("sendMessage", {"chat_id": chat_id, "text": response})
                
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
