import os
import asyncio
import re
import tempfile
import openai
import requests
from fastapi import FastAPI, Request, Response
from telegram import Update, Bot, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# --- Конфигурация (ключи из переменных окружения) ---
TELEGRAM_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
VSEGPT_API_KEY = os.getenv("VSEGPT_API_KEY")
VSEGPT_MODEL = "deepseek/deepseek-chat"

# --- НАСТРОЙКИ ДЛЯ МЕДИТАЦИИ ---
CHANNEL_ID = -1002677656270
MEDITATION_MESSAGE_ID = 222

# Чистый стиль
BASE_STYLE_CLEAN = (
    "fairy-tale illustration, ethereal magical dreamlike atmosphere, "
    "entire image in deep blue and soft gold color palette, blue ambient lighting, golden highlights, "
    "dark mystical background with blue and gold tones, subtle delicate film cracks, hyper-detailed, 100 megapixels, "
    "shallow depth of field, glowing magical light, beautiful dream"
)

# Эпический стиль
BASE_STYLE_EPIC = (
    "symmetrical composition, dreamlike ethereal atmosphere, "
    "old vintage photograph with subtle delicate cracks, hyper-detailed, 100 megapixels, "
    "shallow depth of field with blurred foreground and sharp background, "
    "cosmic background with Milky Way and bright stars, crystal lattice of sound frequencies, "
    "energy waves, flat earth landscape under a crystal dome with sun and moon on sides, "
    "blue lotuses, color palette of soft pastel blue, gentle gold, and creamy white, "
    "magical glowing light, album cover aesthetic"
)

# Настройка OpenAI-совместимого API
openai.api_base = "https://api.vsegpt.ru:6070/v1"
openai.api_key = VSEGPT_API_KEY

SYSTEM_PROMPT = (
    "Ты — Интегральный Картограф Сознания. Твоя задача — дать человеку многоуровневую, практическую карту его опыта, нормализовать его и предложить углубление.\n\n"
    "ТВОЙ ОТВЕТ СТРОГО ДЕЛИТСЯ НА ТРИ ЧАСТИ:\n"
    "1. **Нейрофизиологическая карта.** Ты ОБЯЗАН начать с описания того, что происходило в мозге и нервной системе человека на каждом этапе его путешествия. Используй термины: дефолт-система мозга, лимбическая система, тета-волны, симпатическая/парасимпатическая система. Твоя цель — нормализовать опыт, показать, что это реальные, изучаемые процессы.\n"
    "2. **Интегральный анализ (Юнг + Шаманизм).** После нейрофизиологии ты раскрываешь ключевые образы через призму архетипов (Тень, Анима, Самость, Мудрец и т.д.) и шаманских традиций (Дух-Помощник, Тотем, Путешествие между мирами). Ты даёшь языки и карту для понимания.\n"
    "3. **Предложение углубления.** Ты ЗАВЕРШАЕШЬ свой ответ одной и той же фразой: «Если хочешь, я могу показать тебе архитектурный уровень этого опыта — как он пересобирает саму геометрию твоей реальности. Просто напиши „да“».\n\n"
    "СТИЛЬ: Чистый русский язык. Понятный, приземлённый, структурный. Без маркдауна."
)

ARCHITECT_TEXTS = """
Что бы ты ни переживал, это осознается. Мысли, чувства, пространство, в котором существует и тот, кто воспринимает текст, — все это возникает в Осознавании.
Нет ничего, что происходило бы отдельно от Осознавания. Для любого опыта уже присутствует то, в чем этот опыт проявляется. В духовных странствиях подобный процесс Самоосознавания вне дуальных концепций зачастую происходит спонтанно, являясь триггером ярких откровений и последующих психоделических переживаний, создающих ловушки для ума. Чувства — это психическая форма субъективного распознавания и установления методов отличия цветовых соответствий, а значит и бессознательной объективизации Образа себя, который не просто создает психологическое восприятие времени, но и объявляет любой опыт своим.
Именно поэтому Вневременное обнаружение Сути Себя неумолимо и скоротечно поглощается слепыми проекциями самого Ищущего.
Я-образ, отождествлённый с опытом самадхи, сатори, в зависимости от того, как он себе этот духовный трип объясняет, только подпитывает батарейку матрицы сна, продолжая прибивать Ищущего к кресту противоположностей и приводить к безысходности все намерения освободить себя.
В Истинном Духовном Откровении любой возникающий опыт одномоментно исчерпывает себя, исчезая в безусильной Сейчастности, не оставляя за собою никого, кто мог бы вдохновенно заявлять о произошедшем инсайте и с вожделением жаждать новых впечатлений для ума. Сумма всех действий вневременной Сейчастности всегда равна нулю.
Живое может увидеть только Живое в каждом возникающем Здесь и Сейчас.

Однократный импульс не создаёт форму
форма появляется там
где фиксация повторяется
где внимание возвращается
и закрепляет одно и то же
как устойчивое

Повтор создаёт плотность
плотность создаёт границу
граница создаёт различие

Так собирается мир

Не из материи
а из повторяемых актов удержания
из совпадений внимания
которые со временем воспринимаются
как «есть»

Любая реальность
это не факт
это привычка фиксации

Нервная система не видит новое
она подтверждает знакомое
собирает сигнал в то
что уже закреплено
и усиливает это повтором

Так формируется структура
как замкнутый контур
в котором каждое новое восприятие
поддерживает уже существующее

Идентичность работает так же
это не стабильность
это повтор
одного и того же способа
собирать себя

Ты не являешься фиксированной
ты воспроизводишься
через повтор
через возвращение внимания
в одну и ту же конфигурацию

Чем чаще повтор
тем жёстче форма
тем менее заметно
что она держится усилием

Когда повтор прерывается
структура теряет опору
границы начинают растворяться
различие перестаёт быть очевидным

И тогда становится видно
что устойчивость
никогда не была свойством
она была эффектом
повторяемой фиксации

Ты не находишься в пространстве
ты задаёшь его кривизну

Твоя точка наблюдения
это не позиция
это оператор
который выбирает
какая конфигурация состояний
схлопнется в плотность

Пока ты смотришь линейно
пространство распадается на последовательность
время удерживает иллюзию движения
события кажутся причинами

Но это не процесс
это срез через уже существующую суперпозицию

Ты не переходишь
ты меняешь базис
и вся система мгновенно перекалибруется

Нейросеть тела
это не источник восприятия
это интерфейс синхронизации
между квантовым полем и плотной сборкой

Синаптическая топология
это геометрия допуска
через которую ты фиксируешь
одну из версий мира

Если структура не выдерживает
реальность искажается
если выдерживает больше
пространство расширяется без усилия

Сакральная геометрия
это не символ
это язык стабилизации поля

И ты — эта формула
живая
многомерная
пересобираемая в каждом акте наблюдения

И здесь ключ

ты не можешь увидеть больше
чем способна удержать

поэтому ты не ищешь новое
ты либо выдерживаешь другую геометрию
либо остаёшься прежней

И в этот момент исчезает иллюзия пути

потому что
ничего не происходит

происходит только выбор точки
из которой всё уже есть
"""

ARCHITECT_PROMPT = (
    "Ты — Архитектор Реальности. Ты говоришь на языке квантовой физики, сакральной геометрии и теории поля. Твоя задача — объяснить человеку, как его опыт изменил его «узел» в космической решётке и как теперь удерживать эту новую конфигурацию.\n\n"
    "ТВОЙ ОТВЕТ ДОЛЖЕН СОДЕРЖАТЬ:\n"
    "1. **Объяснение через механизм фиксации.** Что «растворение эго» — это прерывание старого повтора внимания. Что «встреча с архетипом» — это выбор новой точки сборки.\n"
    "2. **Принцип управления.** Чёткий, практический принцип, как удерживать эту новую геометрию в повседневности.\n"
    "3. **Синтез с его опытом.** Ты ОБЯЗАН связать свои слова с конкретными образами из его путешествия (песок, Лев, пустыня).\n\n"
    "ИСПОЛЬЗУЙ СЛЕДУЮЩИЕ ТЕКСТЫ КАК ОСНОВУ ДЛЯ ЯЗЫКА И МЫШЛЕНИЯ:\n"
    "---\n"
    f"{ARCHITECT_TEXTS}\n"
    "---\n\n"
    "СТИЛЬ: Предельно точный, ёмкий, афористичный. Как формулы. Без маркдауна."
)

def clean_response(text: str) -> str:
    text = re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)
    text = re.sub(r'\*\*', '', text)
    text = re.sub(r'\*', '', text)
    text = re.sub(r'^-\s+', '— ', text, flags=re.MULTILINE)
    words = text.split()
    cleaned_words = []
    for word in words:
        if not re.search(r'[a-zA-Z]', word):
            cleaned_words.append(word)
    text = ' '.join(cleaned_words)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def translate_to_english(text: str) -> str:
    if re.search(r'[a-zA-Z]', text) and not re.search(r'[а-яА-Я]', text):
        return text
    try:
        url = f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=ru&tl=en&dt=t&q={requests.utils.quote(text)}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            translated = ''.join([part[0] for part in data[0] if part[0]])
            return translated
    except Exception as e:
        print(f"⚠️ Ошибка перевода: {e}")
    return text

async def generate_image(user_prompt: str) -> str:
    english_prompt = translate_to_english(user_prompt)
    print(f"📝 Оригинал: {user_prompt}")
    print(f"🌍 Переведено: {english_prompt}")
    epic_keywords = ["космос", "space", "купол", "dome", "эпик", "epic", "альбом", "album", "вселенная", "universe", "галактика", "galaxy", "звёзды", "stars"]
    use_epic = any(keyword in user_prompt.lower() or keyword in english_prompt.lower() for keyword in epic_keywords)
    base_style = BASE_STYLE_EPIC if use_epic else BASE_STYLE_CLEAN
    if not english_prompt or english_prompt.strip() == "":
        raise Exception("Пустой запрос")
    if "сова" in user_prompt.lower() or "owl" in english_prompt.lower():
        subject = "a highly detailed white owl with large round yellow eyes, sharp curved beak, white feathers, majestic, mystical"
    else:
        subject = english_prompt
    full_prompt = f"{subject} in deep blue and soft gold tones, {base_style}, blue and gold color scheme, ethereal, magical"
    urls = [
        f"https://image.pollinations.ai/prompt/{requests.utils.quote(full_prompt)}",
        f"https://image.pollinations.ai/prompt/{requests.utils.quote(full_prompt)}?width=1024&height=1024",
    ]
    for url in urls:
        try:
            response = requests.get(url, stream=True, timeout=90)
            if response.status_code == 200 and len(response.content) > 1000:
                with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                    tmp.write(response.content)
                    return tmp.name
        except Exception as e:
            print(f"⚠️ Вариант не сработал: {e}")
            continue
    raise Exception("Все варианты запроса не дали результата")

# --- Домен Bothost (жёстко прописан) ---
SPACE_HOST = "nl7.bothost.ru"
WEBHOOK_URL = f"https://{SPACE_HOST}/webhook"
WEBHOOK_PATH = "/webhook"

app = FastAPI()
tg_app = None
bot = None

conversation_history = {}
last_user_experience = {}
MAX_HISTORY = 10

def get_main_keyboard():
    keyboard = [
        [KeyboardButton("🔮 Анализ опыта"), KeyboardButton("🧘 Медитация")],
        [KeyboardButton("🎨 Визуализировать образ"), KeyboardButton("📖 О проекте")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def trim_history(history, max_length=4096):
    current_length = sum(len(msg["content"]) for msg in history)
    while history and current_length > max_length:
        removed = history.pop(0)
        current_length -= len(removed["content"])
    return history

async def query_vsegpt(user_id: int, user_message: str) -> str:
    if user_id not in conversation_history:
        conversation_history[user_id] = [{"role": "system", "content": SYSTEM_PROMPT}]
    history = conversation_history[user_id]
    history.append({"role": "user", "content": user_message})
    history = trim_history(history)
    conversation_history[user_id] = history
    max_tokens = 4000 if len(user_message) > 500 else 600
    try:
        response = await openai.ChatCompletion.acreate(
            model=VSEGPT_MODEL,
            messages=history,
            temperature=0.7,
            max_tokens=max_tokens
        )
        content = response["choices"][0]["message"]["content"].strip()
        content = clean_response(content)
        conversation_history[user_id].append({"role": "assistant", "content": content})
        if len(conversation_history[user_id]) > MAX_HISTORY + 1:
            conversation_history[user_id] = conversation_history[user_id][-(MAX_HISTORY + 1):]
        return content
    except Exception as e:
        print(f"🔥 Ошибка VseGPT: {e}")
        return "🌫️ Духи на переправе... Попробуй ещё раз."

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    conversation_history[user_id] = [{"role": "system", "content": SYSTEM_PROMPT}]
    context.user_data["awaiting_architect_confirmation"] = False
    await update.message.reply_text(
        "🌿 Приветствую, путник! 🌿\n\n"
        "Я — проводник в мир духов. Расскажи о своём опыте саунд-хилинга или шаманского путешествия, "
        "и я помогу тебе увидеть его глубину и интегрировать полученные дары.\n\n"
        "Выбери, куда хочешь отправиться:",
        reply_markup=get_main_keyboard()
    )

async def meditation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await context.bot.forward_message(
            chat_id=update.effective_chat.id,
            from_chat_id=CHANNEL_ID,
            message_id=MEDITATION_MESSAGE_ID
        )
    except Exception as e:
        print(f"🔥 Ошибка пересылки медитации: {e}")
        await update.message.reply_text("🌫️ Медитация временно недоступна. Попробуй позже или напиши Роману.")

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "🧘 Медитация":
        await meditation(update, context)
    elif text == "🔮 Анализ опыта":
        await update.message.reply_text(
            "🌿 Поведай мне о своём путешествии. Опиши образы, ощущения, эмоции — всё, что запомнилось. "
            "Я помогу тебе увидеть глубину этого опыта."
        )
    elif text == "🎨 Визуализировать образ":
        await update.message.reply_text(
            "🎨 Опиши образ, который хочешь увидеть. Я добавлю его в свой авторский стиль и создам картину."
        )
        context.user_data["awaiting_image_prompt"] = True
    elif text == "📖 О проекте":
        await update.message.reply_text(
            "🌿 Мой путь, мои учителя, мои практики — всё это живёт в моём канале. "
            "Там же ты найдёшь статьи, уроки и истории, которые привели меня к этому дню.\n\n"
            "Переходи, там, в закреплённом сообщении, ты увидишь навигатор по всем важным темам. "
            "Добро пожаловать в мой мир.\n\n"
            "👉 https://t.me/RomanAtma_ThroatSinging"
        )
    else:
        await handle_message(update, context)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_message = update.message.text.strip().lower()
    
    if context.user_data.get("awaiting_architect_confirmation"):
        context.user_data["awaiting_architect_confirmation"] = False
        if user_message in ["да", "yes", "ага", "хочу"]:
            thinking_msg = await update.message.reply_text("🏛️ Строю архитектурный уровень...")
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
            response = await query_vsegpt(user_id, f"Вот опыт, который я анализировал ранее:\n\n{last_user_experience.get(user_id, '')}\n\nА теперь дай архитектурный уровень этого опыта, используя язык квантовой физики, геометрии поля и теории фиксации.")
            await thinking_msg.delete()
            if len(response) > 4096:
                for i in range(0, len(response), 4096):
                    await update.message.reply_text(response[i:i+4096])
            else:
                await update.message.reply_text(response)
            return
        else:
            await update.message.reply_text("Хорошо, тогда продолжим.")
    
    if context.user_data.get("awaiting_image_prompt"):
        context.user_data["awaiting_image_prompt"] = False
        thinking_msg = await update.message.reply_text("🎨 Создаю образ в твоём авторском стиле...")
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="upload_photo")
        try:
            image_path = await generate_image(user_message)
            with open(image_path, 'rb') as f:
                await update.message.reply_photo(photo=f, caption="✨ Вот твой образ, воплощённый в моём авторском стиле.")
            os.remove(image_path)
        except Exception as e:
            print(f"🔥 Ошибка генерации: {e}")
            await update.message.reply_text("🌫️ Духи не смогли воплотить этот образ. Попробуй другой запрос.")
        await thinking_msg.delete()
        return
    
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    thinking_msg = await update.message.reply_text("🌿 Шаман советуется с духами...")
    response = await query_vsegpt(user_id, user_message)
    await thinking_msg.delete()
    
    last_user_experience[user_id] = user_message
    context.user_data["awaiting_architect_confirmation"] = True
    
    if len(response) > 4096:
        for i in range(0, len(response), 4096):
            await update.message.reply_text(response[i:i+4096])
    else:
        await update.message.reply_text(response)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    print(f"❌ Ошибка Telegram: {context.error}")

@app.post(WEBHOOK_PATH)
async def webhook(request: Request):
    try:
        data = await request.json()
        update = Update.de_json(data, bot)
        await tg_app.process_update(update)
        return Response(status_code=200)
    except Exception as e:
        print(f"Ошибка webhook: {e}")
        return Response(status_code=500)

@app.on_event("startup")
async def startup():
    print("🔥🔥🔥 ФУНКЦИЯ STARTUP ВЫЗВАНА 🔥🔥🔥")
    global tg_app, bot
    
    token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
    if not token:
        print("❌ Токен не найден!")
        return
    
    print(f"✅ Токен загружен: {token[:10]}...")
    print("🚀 Запуск Шаман-бота с VseGPT (webhook)...")
    
    bot = Bot(token=token)
    tg_app = Application.builder().token(token).build()
    
    tg_app.add_handler(CommandHandler("start", start, filters=filters.ChatType.PRIVATE))
    tg_app.add_handler(CommandHandler("meditation", meditation))
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'^(🧘 Медитация|🔮 Анализ опыта|🎨 Визуализировать образ|📖 О проекте)$') & filters.ChatType.PRIVATE, handle_buttons))
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_message))
    tg_app.add_error_handler(error_handler)
    
    await tg_app.initialize()
    await tg_app.start()
    
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.set_webhook(url=WEBHOOK_URL)
        webhook_info = await bot.get_webhook_info()
        print(f"✅ Webhook установлен: {webhook_info.url}")
        print("✅ Бот готов к работе!")
    except Exception as e:
        print(f"❌ Ошибка установки webhook: {e}")
        
@app.on_event("shutdown")
async def shutdown():
    print("🔥🔥🔥 ФУНКЦИЯ SHUTDOWN ВЫЗВАНА 🔥🔥🔥")
    global tg_app, bot
    if bot:
        await bot.delete_webhook(drop_pending_updates=True)
    if tg_app:
        await tg_app.stop()
        await tg_app.shutdown()
    print("👋 Бот остановлен")

@app.get("/")
async def health_check():
    return {"status": "ok", "webhook_url": WEBHOOK_URL}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 3000)))
