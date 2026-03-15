import aiohttp
import asyncio

async def get_token():
    client_id = "0B671909ED17D7F62BB10C2D7749E957D64628A801D832D7E86747900562E377"
    client_secret = ""  # Оставь пустым, если нет
    code = input("21C92FC7FD4B25A9CD902DA520953F3720DD979979FCE4AD12ECA417E9FC5132775992B4EC6E4DAD53C5B2AB34AD41A23BB130AF8CE5472957D2A0068A4C037BD8B103FC793DD3FB2830DE92C46ED55E2635F102F5E2F4EA7929619011C048CF319192F8EBFD31AC35EA1545D108DA77C12D6A96F06FD9AC60680482FDE15930 ").strip()
    
    url = "https://yoomoney.ru/oauth/token"
    data = {
        "code": code,
        "client_id": client_id,
        "grant_type": "authorization_code",
        "redirect_uri": "https://example.com"
    }
    
    # Если есть client_secret, добавь его
    if client_secret:
        data["client_secret"] = client_secret
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data) as resp:
            result = await resp.json()
            print(f"Статус: {resp.status}")
            print(f"Ответ: {result}")
            
            if "access_token" in result:
                print(f"\n✅ ТВОЙ ТОКЕН:\n{result['access_token']}")
            else:
                print(f"\n❌ Ошибка: {result.get('error_description', 'Неизвестная ошибка')}")

asyncio.run(get_token())
