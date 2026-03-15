import asyncio
import aiohttp
import os
from dotenv import load_dotenv

load_dotenv()

YOOMONEY_TOKEN = os.getenv("YOOMONEY_TOKEN")
YOOMONEY_WALLET = os.getenv("YOOMONEY_WALLET")

async def test_yoomoney():
    """Тестирует подключение к ЮMoney API"""
    
    print("=" * 50)
    print("🔍 ТЕСТ ПОДКЛЮЧЕНИЯ К YOOMONEY")
    print("=" * 50)
    
    print(f"\n📌 Кошелек: {YOOMONEY_WALLET}")
    print(f"📌 Токен: {YOOMONEY_TOKEN[:20]}...{YOOMONEY_TOKEN[-10:] if YOOMONEY_TOKEN else 'None'}")
    
    if not YOOMONEY_TOKEN or not YOOMONEY_WALLET:
        print("\n❌ ОШИБКА: Не установлены переменные в .env файле!")
        print("   Проверь файл .env")
        return
    
    print("\n🔄 Проверяем токен через API...")
    
    # Тест 1: Проверка баланса
    url = "https://yoomoney.ru/api/account-info"
    headers = {"Authorization": f"Bearer {YOOMONEY_TOKEN}"}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as resp:
                print(f"\n📊 Тест баланса:")
                print(f"   Статус: {resp.status}")
                
                if resp.status == 200:
                    data = await resp.json()
                    print(f"   ✅ УСПЕХ!")
                    print(f"   💰 Баланс: {data.get('balance')} ₽")
                    print(f"   🆔 Кошелек API: {data.get('account')}")
                    
                    # Проверяем совпадение кошельков
                    if data.get('account') == YOOMONEY_WALLET:
                        print(f"   ✅ Кошельки совпадают")
                    else:
                        print(f"   ⚠️ Кошельки не совпадают!")
                        print(f"      В .env: {YOOMONEY_WALLET}")
                        print(f"      В API:  {data.get('account')}")
                    
                elif resp.status == 401:
                    data = await resp.json()
                    print(f"   ❌ ОШИБКА 401: Неверный токен")
                    print(f"   Детали: {data}")
                else:
                    print(f"   ❌ Ошибка: {resp.status}")
                    text = await resp.text()
                    print(f"   Ответ: {text[:200]}")
                    
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
    
    # Тест 2: Проверка истории (опционально)
    print("\n🔄 Проверяем историю операций...")
    
    url = "https://yoomoney.ru/api/operation-history"
    params = {"records": 3}  # последние 3 операции
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=params) as resp:
                print(f"   Статус: {resp.status}")
                
                if resp.status == 200:
                    data = await resp.json()
                    operations = data.get('operations', [])
                    print(f"   ✅ УСПЕХ! Найдено операций: {len(operations)}")
                    
                    if operations:
                        print("\n   📋 Последние операции:")
                        for i, op in enumerate(operations[:3], 1):
                            print(f"   {i}. {op.get('amount')} ₽ - {op.get('status')} - {op.get('datetime', '')[:10]}")
                else:
                    print(f"   ❌ Ошибка: {resp.status}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    
    print("\n" + "=" * 50)
    print("✅ Тест завершен")

if name == "__main__":
    asyncio.run(test_yoomoney())
