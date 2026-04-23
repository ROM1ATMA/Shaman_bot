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
from urllib import error, parse, request

# --- Конфигурация ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
VSEGPT_API_KEY = os.getenv("VSEGPT_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "3000"))
VSEGPT_MODEL = "deepseek/deepseek-chat"

# --- НАСТРОЙКИ ДЛЯ МЕДИТАЦИИ ---
CHANNEL_ID = -1002677656270
MEDITATION_MESSAGE_ID = 222

# Стили для генерации
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

# Настройка OpenAI
if VSEGPT_API_KEY:
    openai.api_base = "https://api.vsegpt.ru:6070/v1"
    openai.api_key = VSEGPT_API_KEY

# Системный промт (сокращён для краткости, вставь свой полный)
SYSTEM_PROMPT = "Ты — Интегральный Картограф Сознания..."

# Хранилище истории
conversation_history = {}
last_user_experience = {}
MAX_HISTORY = 10

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def log(message: str) -> None:
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
        log(f"VseGPT error: {e}")
        return "🌫️ Духи на переправе..."

class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ShamanBot/3.0"

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

        # Проверка секретного токена
        if WEBHOOK_SECRET:
            incoming = self.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if incoming != WEBHOOK_SECRET:
                log("Rejected: invalid secret token")
                self._send_json(403, {"ok": False, "error": "Invalid secret"})
                return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        decoded = raw.decode("utf-8", errors="replace")
        log(f"Webhook received: {decoded[:200]}...")

        try:
            update = json.loads(decoded) if decoded else {}
        except json.JSONDecodeError:
            self._send_json(400, {"ok": False, "error": "Invalid JSON"})
            return

        message = update.get("message") or update.get("edited_message") or {}
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        text = message.get("text", "")

        if chat_id and text:
            # Обработка сообщения
            if text.startswith("/start"):
                reply = "🌿 Приветствую, путник! 🌿\n\nЯ — проводник в мир духов. Расскажи о своём опыте..."
                telegram_api("sendMessage", {"chat_id": chat_id, "text": reply})
            elif text.startswith("/meditation"):
                telegram_api("forwardMessage", {"chat_id": chat_id, "from_chat_id": CHANNEL_ID, "message_id": MEDITATION_MESSAGE_ID})
            else:
                # Отправляем на анализ
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                response = loop.run_until_complete(query_vsegpt(chat_id, text))
                loop.close()
                telegram_api("sendMessage", {"chat_id": chat_id, "text": response})

        self._send_json(200, {"ok": True})

    def log_message(self, format: str, *args) -> None:
        log(f"{self.client_address[0]} - {format % args}")

def set_webhook(public_url: str) -> int:
    payload = {"url": f"{public_url.rstrip('/')}/webhook"}
    if WEBHOOK_SECRET:
        payload["secret_token"] = WEBHOOK_SECRET
    ok, resp = telegram_api("setWebhook", payload)
    log(f"setWebhook: {resp}")
    return 0 if ok else 1

if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--set-webhook":
        sys.exit(set_webhook(sys.argv[2].strip()))

    if not BOT_TOKEN:
        log("ERROR: BOT_TOKEN is empty")
        sys.exit(1)

    log(f"Starting Shaman Bot on {HOST}:{PORT}")
    server = HTTPServer((HOST, PORT), WebhookHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("Shutting down...")
    finally:
        server.server_close()
