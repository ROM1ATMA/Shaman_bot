import requests
import time

# ==========================================
# НАСТРОЙКИ (уже вставлены твои данные)
# ==========================================
TELEGRAM_TOKEN = "6836196194:AAEXLkRTNbn49Y5MV2efXNgpodf7oMMwue8"
HF_API_KEY = "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"
# ==========================================

def get_ai_response(question):
    """Отправляет вопрос в Hugging Face и возвращает ответ"""
    url = "https://api-inference.huggingface.co/models/microsoft/DialoGPT-medium"
    headers = {"Authorization": f"Bearer {HF_API_KEY}"}
    payload = {
        "inputs": f"Ты мудрый шаман. Отвечай по-русски коротко. Вопрос: {question}",
        "parameters": {"max_length": 150, "temperature": 0.7}
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        result = response.json()
        
        if "error" in result:
            return "Модель загружается... Повтори через 10 секунд."
        
        answer = result[0].get('generated_text', '')
        answer = answer.replace(question, "").strip()
        
        if not answer:
            return "Ммм... Дай мне собраться с мыслями."
        
        return answer
        
    except requests.exceptions.Timeout:
        return "Сервер думает слишком долго. Попробуй спросить что-то попроще."
    except Exception as e:
        return f"Ошибка: {str(e)}"

# ==========================================
# ЗАПУСК БОТА
# ==========================================
last_update_id = 0
print("Бот запущен и ждёт сообщений...")

while True:
    try:
        # Получаем новые сообщения
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        response = requests.get(url, params={
            "offset": last_update_id + 1,
            "timeout": 30
        })
        updates = response.json()
        
        if updates.get('ok') and updates.get('result'):
            for update in updates['result']:
                last_update_id = update['update_id']
                
                # Проверяем, что есть сообщение с текстом
                if 'message' in update and 'text' in update['message']:
                    chat_id = update['message']['chat']['id']
                    user_text = update['message']['text']
                    
                    # Обрабатываем команду /start
                    if user_text == '/start':
                        reply = "Привет! Я шаманский помощник. Задай свой вопрос о шаманских путешествиях, горловом пении или Sound Healing."
                    else:
                        reply = get_ai_response(user_text)
                    
                    # Отправляем ответ
                    send_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                    requests.post(send_url, json={"chat_id": chat_id, "text": reply})
        
        time.sleep(1)
        
    except Exception as e:
        print(f"Ошибка в основном цикле: {e}")
        time.sleep(5)
