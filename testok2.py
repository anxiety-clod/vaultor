import asyncio
import aiohttp
import os
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

async def test_token():
    # Получаем токен из окружения
    token = os.getenv("YOOMONEY_TOKEN")
    wallet = os.getenv("YOOMONEY_WALLET")
    
    print("="*60)
    print("🔍 ТЕСТ ТОКЕНА YOOMONEY")
    print("="*60)
    print(f"📌 Кошелек: {wallet}")
    print(f"📌 Токен: {token[:30]}..." if token else "❌ Токен не найден!")
    print("-"*60)
    
    if not token:
        print("❌ ОШИБКА: Токен не найден в .env файле!")
        return
    
    # ТЕСТ 1: Проверка баланса
    print("\n📊 ТЕСТ 1: Получение информации о кошельке")
    url1 = "https://yoomoney.ru/api/account-info"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url1, headers=headers) as resp:
                print(f"📡 Статус: {resp.status}")
                
                if resp.status == 200:
                    data = await resp.json()
                    print(f"✅ УСПЕХ!")
                    print(f"   Баланс: {data.get('balance')} ₽")
                    print(f"   Кошелек: {data.get('account')}")
                    
                    # Проверяем совпадение кошельков
                    if data.get('account') == wallet:
                        print(f"   ✅ Кошельки совпадают")
                    else:
                        print(f"   ⚠️ Кошельки НЕ совпадают!")
                else:
                    text = await resp.text()
                    print(f"❌ ОШИБКА {resp.status}: {text}")
                    
                    if resp.status == 401:
                        print("\n💡 ТОКЕН НЕ РАБОТАЕТ! Нужен новый.")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
    
    # ТЕСТ 2: Проверка истории операций
    print("\n📊 ТЕСТ 2: Получение истории операций")
    url2 = "https://yoomoney.ru/api/operation-history"
    params = {"records": 5}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url2, headers=headers, data=params) as resp:
                print(f"📡 Статус: {resp.status}")
                
                if resp.status == 200:
                    data = await resp.json()
                    ops = data.get('operations', [])
                    print(f"✅ УСПЕХ! Найдено операций: {len(ops)}")
                    
                    if ops:
                        print("\n📋 Последние операции:")
                        for i, op in enumerate(ops[:3], 1):
                            print(f"   {i}. {op.get('amount')} ₽ - {op.get('status')} - {op.get('label', 'без метки')}")
                else:
                    text = await resp.text()
                    print(f"❌ ОШИБКА {resp.status}: {text}")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
    
    print("\n" + "="*60)
    print("✅ ТЕСТ ЗАВЕРШЕН")
    print("="*60)

if name == "__main__":
    asyncio.run(test_token())
