import asyncio
import aiohttp

async def exchange(code):
    client_id = "0B671909ED17D7F62BB10C2D7749E957D64628A801D832D7E86747900562E377"
    url = "https://yoomoney.ru/oauth/token"
    data = {
        "code": code,
        "client_id": client_id,
        "grant_type": "authorization_code",
        "redirect_uri": "http://localhost:8080"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data) as resp:
            result = await resp.json()
            print("Токен:", result.get("access_token"))

code = input("Вставь code: ")
asyncio.run(exchange(code))
