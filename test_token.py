import asyncio
import aiohttp
import os
from dotenv import load_dotenv

load_dotenv()

YOOMONEY_TOKEN = os.getenv("YOOMONEY_TOKEN")
YOOMONEY_WALLET = os.getenv("YOOMONEY_WALLET")

async def test_token():
    print(f"🔍 Тестируем токен...")
    print(f"Кошелек: {YOOMONEY_WALLET}")
    print(f"Токен: {YOOMONEY_TOKEN[:50]}...\n")
    
    # Тест 1: Проверка баланса
    url = "https://yoomoney.ru/api/account-info"
    headers = {"Authorization": f"Bearer {YOOMONEY_TOKEN}"}
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers) as resp:
            print(f"📊 Тест баланса:")
            print(f"Статус: {resp.status}")
            if resp.status == 200:
                data = await resp.json()
                print(f"✅ УСПЕХ! Баланс: {data.get('balance')} ₽")
                print(f"Кошелек: {data.get('account')}")
                return True
            else:
                data = await resp.json()
                print(f"❌ Ошибка: {data}")
                return False

asyncio.run(test_token())
