import os
import random
import asyncio
import asyncpg
import aiohttp
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from dotenv import load_dotenv

# ================= HEALTH CHECK SERVER (в отдельном потоке) =================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass

def run_health_server():
    try:
        server = HTTPServer(('0.0.0.0', 10000), HealthHandler)
        print("✅ Health server started on port 10000")
        server.serve_forever()
    except Exception as e:
        print(f"❌ Health server error: {e}")

# Запускаем health-сервер ДО инициализации бота
health_thread = threading.Thread(target=run_health_server, daemon=True)
health_thread.start()
time.sleep(1)

# ================= LOAD ENV =================
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
PAY_ACCOUNT = os.getenv("PAY_ACCOUNT")
CARD_HOLDER = os.getenv("CARD_HOLDER")
CARD_BANK = os.getenv("CARD_BANK")
DATABASE_URL = os.getenv("DATABASE_URL")

if not PAY_ACCOUNT:
    raise ValueError("PAY_ACCOUNT должен быть установлен в .env файле")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL должен быть установлен в .env файле")

# ================= КЛИЕНТ =================
app = Client(
    "escrow_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN, 
    workers=100,
    max_concurrent_transmissions=50
)

user_states = {}
escrow = {}
app.replenish_requests = {}

# ================= ПОДКЛЮЧЕНИЕ К POSTGRESQL =================
db_pool = None

async def init_db_pool():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=10,
            command_timeout=60
        )
        print("✅ Подключение к PostgreSQL установлено")
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        raise
    return db_pool

async def get_db():
    global db_pool
    if db_pool is None:
        await init_db_pool()
    return db_pool

# ================= СОЗДАНИЕ ТАБЛИЦ =================
async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id BIGINT PRIMARY KEY,
            balance INTEGER DEFAULT 0,
            blocked INTEGER DEFAULT 0,
            total_stars INTEGER DEFAULT 0,
            total_votes INTEGER DEFAULT 0,
            total_deals INTEGER DEFAULT 0,
            total_turnover INTEGER DEFAULT 0,
            registered_date TEXT
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS deals(
            code TEXT PRIMARY KEY,
            seller_id BIGINT,
            buyer_id BIGINT,
            name TEXT,
            description TEXT,
            amount INTEGER,
            status TEXT,
            dispute_reason TEXT DEFAULT ''
        )
        """)
        
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS ratings(
            id SERIAL PRIMARY KEY,
            seller_id BIGINT,
            buyer_id BIGINT,
            deal_code TEXT,
            rating INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(buyer_id, deal_code)
        )
        """)
        
        print("✅ Таблицы созданы/проверены")

# ================= РАБОТА С ПОЛЬЗОВАТЕЛЯМИ =================
async def get_all_users():
    pool = await get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id FROM users")
        return [row['id'] for row in rows]

async def get_balance(uid):
    pool = await get_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT balance FROM users WHERE id=$1", uid)
        if not row:
            await conn.execute("""
                INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) 
                VALUES($1, $2, $3, $4, $5, $6, $7, $8)
            """, uid, 0, 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
            return 0
        return row['balance']

async def change_balance(uid, amount):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date)
            VALUES($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (id) DO UPDATE SET balance = users.balance + $2
        """, uid, amount, 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))

async def set_block(uid, blocked=True):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date)
            VALUES($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (id) DO UPDATE SET blocked = $3
        """, uid, 0, 1 if blocked else 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))

async def is_blocked(uid):
    pool = await get_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT blocked FROM users WHERE id=$1", uid)
        return row['blocked'] == 1 if row else False

# ================= СТАТИСТИКА ПРОДАВЦА =================
async def get_seller_stats(seller_id):
    pool = await get_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT total_stars, total_votes, total_deals, total_turnover, blocked, registered_date 
            FROM users WHERE id=$1
        """, seller_id)
        
        if not row:
            return None
            
        rating_rows = await conn.fetch("""
            SELECT rating, COUNT(*) as count 
            FROM ratings 
            WHERE seller_id=$1 
            GROUP BY rating 
            ORDER BY rating DESC
        """, seller_id)
        
        dist = {5:0, 4:0, 3:0, 2:0, 1:0}
        for r in rating_rows:
            dist[r['rating']] = r['count']
        
        rating = round(row['total_stars'] / row['total_votes'], 1) if row['total_votes'] > 0 else 0
        
        return {
            "rating": rating,
            "total_votes": row['total_votes'],
            "total_deals": row['total_deals'],
            "total_turnover": row['total_turnover'],
            "blocked": row['blocked'],
            "registered_date": row['registered_date'] if row['registered_date'] else "неизвестно",
            "dist": dist
        }

async def add_rating(seller_id, buyer_id, deal_code, rating_value):
    pool = await get_db()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                INSERT INTO ratings (seller_id, buyer_id, deal_code, rating) 
                VALUES($1, $2, $3, $4)
            """, seller_id, buyer_id, deal_code, rating_value)
            
            await conn.execute("""
                UPDATE users 
                SET total_stars = total_stars + $1,
                    total_votes = total_votes + 1
                WHERE id = $2
            """, rating_value, seller_id)

async def update_seller_deal_stats(seller_id, amount):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users 
            SET total_deals = total_deals + 1,
                total_turnover = total_turnover + $1
            WHERE id = $2
        """, amount, seller_id)

# ================= ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ =================
async def get_user_profile(uid):
    pool = await get_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT balance, total_deals, total_turnover, registered_date, blocked
            FROM users WHERE id=$1
        """, uid)
        
        if not row:
            await get_balance(uid)
            return await get_user_profile(uid)
        
        try:
            user = await app.get_users(uid)
            username = f"@{user.username}" if user.username else "нет"
            first_name = user.first_name or ""
            last_name = user.last_name or ""
            full_name = f"{first_name} {last_name}".strip()
        except:
            username = "недоступен"
            full_name = "неизвестно"
        
        seller_deals = await conn.fetchval("""
            SELECT COUNT(*) FROM deals 
            WHERE seller_id=$1 AND status='completed'
        """, uid)
        
        buyer_deals = await conn.fetchval("""
            SELECT COUNT(*) FROM deals 
            WHERE buyer_id=$1 AND status='completed'
        """, uid)
        
        return {
            "user_id": uid,
            "username": username,
            "full_name": full_name,
            "balance": row['balance'],
            "registered_date": row['registered_date'] if row['registered_date'] else "неизвестно",
            "seller_deals": seller_deals,
            "buyer_deals": buyer_deals,
            "total_turnover": row['total_turnover'],
            "blocked": row['blocked'] == 1
        }

# ================= UI =================
main_keyboard = ReplyKeyboardMarkup(
    [[KeyboardButton("📝 Создать сделку")],
     [KeyboardButton("💰 Баланс"), KeyboardButton("💳 Пополнить"), KeyboardButton("👤 Профиль")],
     [KeyboardButton("🔍 Поиск продавца")]],
    resize_keyboard=True
)

main_keyboard_admin = ReplyKeyboardMarkup(
    [[KeyboardButton("📝 Создать сделку")],
     [KeyboardButton("💰 Баланс"), KeyboardButton("💳 Пополнить"), KeyboardButton("👤 Профиль")],
     [KeyboardButton("🔍 Поиск продавца"), KeyboardButton("⚙ Админ")]],
    resize_keyboard=True
)

admin_keyboard = ReplyKeyboardMarkup(
    [[KeyboardButton("🔒 Заблокировать пользователя")],
     [KeyboardButton("🔓 Разблокировать пользователя")],
     [KeyboardButton("➕ Пополнить баланс")],
     [KeyboardButton("📢 Сообщение")],
     [KeyboardButton("⬅ Назад")]],
    resize_keyboard=True
)

# ================= START =================
@app.on_message(filters.command("start"))
async def start(client, message):
    uid = message.from_user.id
    try:
        args = message.text.split()
        if len(args) > 1 and args[1].startswith("deal_"):
            await join_deal(message, args[1][5:])
            return
        
        await get_balance(uid)
        
        kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
        
        welcome_text = (
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🔥 <b>VAULTOR — Безопасный escrow-сервис</b> 🔥\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "▫️ <b>Защита сделок:</b> Средства резервируются на эскроу-счете\n"
            "▫️ <b>Прозрачность:</b> Полный контроль на каждом этапе\n"
            "▫️ <b>Без комиссии:</b> VAULTOR не взимает комиссию за сделки\n"
            "▫️ <b>Рейтинг продавцов:</b> Выбирайте проверенных партнеров\n"
            "▫️ <b>Арбитраж:</b> Споры решаются администратором\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⬇️ <b>Выберите действие:</b>"
        )
        
        await message.reply(welcome_text, parse_mode=ParseMode.HTML, reply_markup=kb)
    except Exception as e:
        print(f"Ошибка в start: {e}")

# ================= ПОИСК ПОЛЬЗОВАТЕЛЯ =================
async def get_user_by_id_or_username(identifier):
    try:
        identifier = identifier.strip()
        if identifier.startswith("@"):
            identifier = identifier[1:]
        if identifier.isdigit():
            return int(identifier)
        user = await app.get_users(identifier)
        return user.id if user else None
    except Exception as e:
        print(f"Ошибка поиска пользователя: {e}")
        return None

# ================= ПРИСОЕДИНЕНИЕ К СДЕЛКЕ =================
async def join_deal(message, code):
    try:
        pool = await get_db()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT seller_id, name, description, amount, status, buyer_id FROM deals WHERE code=$1", code)
            
            if not row:
                await message.reply("❌ Сделка не найдена.")
                return
                
            seller_id, name, desc, amount, status, buyer_id = row
            
            if status != "open":
                await message.reply("❌ Сделка недоступна.")
                return
                
            if buyer_id is not None:
                await message.reply("❌ К этой сделке уже подключился покупатель.")
                return

            buyer = message.from_user
            username = f"@{buyer.username}" if buyer.username else "без username"

            await conn.execute("UPDATE deals SET buyer_id=$1 WHERE code=$2", buyer.id, code)

            await app.send_message(
                seller_id,
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔔 <b>К ВАШЕЙ СДЕЛКЕ ПОДКЛЮЧИЛСЯ ПОКУПАТЕЛЬ!</b> 🔔\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📦 <b>Название:</b> {name}\n"
                f"💰 <b>Сумма:</b> {amount} ₽\n"
                f"🔢 <b>Код:</b> {code}\n\n"
                f"👤 <b>Покупатель:</b> {username}\n"
                f"🆔 <code>{buyer.id}</code>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )

            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Оплатить", callback_data=f"pay_{code}")]
            ])
            
            await message.reply(
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📦 <b>ИНФОРМАЦИЯ О СДЕЛКЕ</b> 📦\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"<b>Название:</b> {name}\n"
                f"<b>Описание:</b> {desc}\n"
                f"<b>Сумма:</b> {amount} ₽\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Нажмите кнопку ниже, чтобы оплатить сделку.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb
            )
    except Exception as e:
        print(f"Ошибка join_deal: {e}")

# ================= ОСНОВНОЙ ОБРАБОТЧИК =================
@app.on_message(filters.text & ~filters.command(["start"]))
async def handle_text(client, message):
    uid = message.from_user.id
    text = message.text.strip()
    
    try:
        if await is_blocked(uid):
            await message.reply("❌ Вы заблокированы и не можете использовать бота.")
            return

        # Обработка спора
        if uid in user_states and user_states[uid].get("step") == "dispute_msg":
            code = user_states[uid].get("deal_code")
            if not code:
                del user_states[uid]
                return
            
            dispute_text = text
            
            pool = await get_db()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT seller_id, buyer_id, name, amount FROM deals WHERE code=$1", code)
                
                if not row:
                    await message.reply("❌ Сделка не найдена.")
                    del user_states[uid]
                    return
                
                await conn.execute("UPDATE deals SET status='dispute', dispute_reason=$1 WHERE code=$2", dispute_text[:200], code)
                seller_id, buyer_id, name, amount = row
            
            disputer = "ПРОДАВЕЦ" if uid == seller_id else "ПОКУПАТЕЛЬ"
            disputer_emoji = "👤" if uid == seller_id else "👥"
            
            try:
                buyer_user = await app.get_users(buyer_id)
                seller_user = await app.get_users(seller_id)
                buyer_username = f"@{buyer_user.username}" if buyer_user.username else "без username"
                seller_username = f"@{seller_user.username}" if seller_user.username else "без username"
            except:
                buyer_username = "без username"
                seller_username = "без username"
            
            kb_dispute = InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 Отдать покупателю", callback_data=f"resolve_{code}_buyer")],
                [InlineKeyboardButton("👥 Отдать продавцу", callback_data=f"resolve_{code}_seller")]
            ])
            
            await app.send_message(
                ADMIN_ID,
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⚖ <b>ОТКРЫТ СПОР</b> ⚖\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{disputer_emoji} <b>Инициатор:</b> {disputer}\n"
                f"👤 <b>Пользователь:</b> @{message.from_user.username or 'нет'}\n"
                f"🆔 <b>ID:</b> <code>{uid}</code>\n\n"
                f"🔢 <b>Код сделки:</b> <code>{code}</code>\n"
                f"📦 <b>Название:</b> {name}\n"
                f"💰 <b>Сумма:</b> {amount} ₽\n\n"
                f"👤 <b>Покупатель:</b> {buyer_username}\n"
                f"🆔 <code>{buyer_id}</code>\n"
                f"👤 <b>Продавец:</b> {seller_username}\n"
                f"🆔 <code>{seller_id}</code>\n\n"
                f"💬 <b>Сообщение от {disputer.lower()}:</b>\n"
                f"<i>{dispute_text}</i>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_dispute
            )
            
            await message.reply(
                "✅ <b>Ваше сообщение отправлено администратору.</b>\n"
                "Ожидайте решения спора.",
                parse_mode=ParseMode.HTML
            )
            
            del user_states[uid]
            return

        # Пополнение
        if text == "💳 Пополнить":
            user_states[uid] = {"step": "card_amount"}
            await message.reply(
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "💳 <b>ПОПОЛНЕНИЕ БАЛАНСА ПО КАРТЕ</b> 💳\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Введите сумму от 1 до 5000 рублей:\n\n"
                "💡 <i>Только цифры</i>\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
            return

        # Баланс
        if text == "💰 Баланс":
            bal = await get_balance(uid)
            kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
            await message.reply(
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 <b>ВАШ БАЛАНС</b> 💰\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💳 <b>Текущий баланс:</b> {bal} ₽\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML,
                reply_markup=kb
            )
            return

        # Профиль
        if text == "👤 Профиль":
            profile = await get_user_profile(uid)
            status = "❌ ЗАБЛОКИРОВАН" if profile["blocked"] else "✅ АКТИВЕН"
            
            profile_text = (
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>ВАШ ПРОФИЛЬ</b> 👤\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🆔 <b>ID:</b> <code>{profile['user_id']}</code>\n"
                f"👤 <b>Имя:</b> {profile['full_name']}\n"
                f"📛 <b>Username:</b> {profile['username']}\n"
                f"📅 <b>Дата:</b> {profile['registered_date']}\n\n"
                f"💰 <b>Баланс:</b> {profile['balance']} ₽\n"
                f"📊 <b>Сделок:</b> {profile['seller_deals']} прод, {profile['buyer_deals']} куп\n"
                f"💫 <b>Оборот:</b> {profile['total_turnover']} ₽\n\n"
                f"🔰 <b>Статус:</b> {status}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            )
            
            kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
            await message.reply(profile_text, parse_mode=ParseMode.HTML, reply_markup=kb)
            return

        # Поиск продавца
        if text == "🔍 Поиск продавца":
            user_states[uid] = {"step": "search_seller"}
            await message.reply("Введите ID или @username продавца:")
            return

        # Создать сделку
        if text.startswith("📝"):
            user_states[uid] = {"step": "name", "data": {}}
            await message.reply("Введите название сделки:")
            return

        # Админ-панель
        if uid == ADMIN_ID and text == "⚙ Админ":
            user_states[uid] = {"step": "admin_menu"}
            await message.reply("🛠 Админ-панель:", reply_markup=admin_keyboard)
            return

        # Обработка поиска
        if uid in user_states and user_states[uid].get("step") == "search_seller":
            seller_identifier = text.strip()
            seller_id = await get_user_by_id_or_username(seller_identifier)
            
            if not seller_id:
                await message.reply("❌ Пользователь не найден.")
                del user_states[uid]
                return
            
            stats = await get_seller_stats(seller_id)
            if not stats:
                await message.reply("❌ Нет данных о продавце.")
                del user_states[uid]
                return
            
            try:
                seller_user = await app.get_users(seller_id)
                seller_username = f"@{seller_user.username}" if seller_user.username else "нет username"
                seller_name = seller_user.first_name or ""
            except:
                seller_username = "нет username"
                seller_name = "неизвестно"
            
            reliability = "🆕 НЕТ ОЦЕНОК"
            if stats["blocked"]:
                reliability = "❌ ЗАБЛОКИРОВАН"
            elif stats["rating"] >= 4.5:
                reliability = "🏆 ОЧЕНЬ ВЫСОКИЙ"
            elif stats["rating"] >= 4.0:
                reliability = "⭐ ВЫСОКИЙ"
            
            profile_text = (
                f"👤 <b>ПРОФИЛЬ ПРОДАВЦА</b>\n\n"
                f"🆔 ID: <code>{seller_id}</code>\n"
                f"📛 Username: {seller_username}\n"
                f"👤 Имя: {seller_name}\n\n"
                f"⭐ Рейтинг: {stats['rating']} ({stats['total_votes']} отзывов)\n"
                f"5⭐: {stats['dist'][5]} | 4⭐: {stats['dist'][4]} | 3⭐: {stats['dist'][3]}\n"
                f"✅ Сделок: {stats['total_deals']}\n"
                f"💰 Оборот: {stats['total_turnover']} ₽\n"
                f"🎯 Надежность: {reliability}"
            )
            
            await message.reply(profile_text, parse_mode=ParseMode.HTML)
            del user_states[uid]
            return

        # Обработка состояний
        if uid in user_states:
            state = user_states[uid]

            # Создание сделки
            if state.get("step") == "name":
                state["data"]["name"] = text
                state["step"] = "desc"
                await message.reply("Введите описание сделки:")
                return
            
            if state.get("step") == "desc":
                state["data"]["desc"] = text
                state["step"] = "amount"
                await message.reply("Введите сумму сделки (числом):")
                return
            
            if state.get("step") == "amount":
                if not text.isdigit():
                    await message.reply("⚠ Введите число.")
                    return
                amount = int(text)
                code = str(random.randint(10000000, 99999999))
                
                pool = await get_db()
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO deals(code, seller_id, buyer_id, name, description, amount, status) VALUES($1,$2,$3,$4,$5,$6,$7)",
                        code, uid, None, state["data"]["name"], state["data"]["desc"], amount, "open"
                    )
                
                bot_username = (await client.get_me()).username
                link = f"https://t.me/{bot_username}?start=deal_{code}"
                
                await message.reply(
                    f"✅ Сделка создана!\nКод: {code}\nСсылка: {link}",
                    disable_web_page_preview=True
                )
                del user_states[uid]
                return

            # Пополнение по карте
            if state.get("step") == "card_amount":
                try:
                    amount = float(text.strip())
                    if amount < 1 or amount > 5000:
                        await message.reply("❌ Сумма от 1 до 5000")
                        return
                    
                    amount = round(amount, 2)
                    request_id = f"replenish_{uid}_{int(time.time())}"
                    
                    user = message.from_user
                    username = f"@{user.username}" if user.username else "нет username"
                    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                    
                    app.replenish_requests[request_id] = {
                        "user_id": uid,
                        "username": username,
                        "full_name": full_name,
                        "amount": amount,
                        "timestamp": time.time(),
                        "confirmed": False
                    }
                    
                    notify_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Я перевел деньги", callback_data=f"notify_replenish_{request_id}")]
                    ])
                    
                    await message.reply(
                        f"💳 <b>РЕКВИЗИТЫ</b>\n\n"
                        f"Карта: <code>{PAY_ACCOUNT}</code>\n"
                        f"Получатель: {CARD_HOLDER}\n"
                        f"Банк: {CARD_BANK}\n"
                        f"Сумма: {amount} ₽\n\n"
                        f"После перевода нажмите кнопку ниже.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=notify_kb
                    )
                    del user_states[uid]
                    
                except ValueError:
                    await message.reply("❌ Введите число")
                return

            # Админка
            if state.get("step") == "admin_menu":
                if text == "⬅ Назад":
                    user_states[uid] = {}
                    await message.reply("Главное меню", reply_markup=main_keyboard_admin)
                    return
                if text == "🔒 Заблокировать пользователя":
                    state["step"] = "block"
                    await message.reply("Введите ID или @username:")
                    return
                if text == "🔓 Разблокировать пользователя":
                    state["step"] = "unblock"
                    await message.reply("Введите ID или @username:")
                    return
                if text == "➕ Пополнить баланс":
                    state["step"] = "add_balance"
                    await message.reply("Введите ID и сумму через пробел:")
                    return
                if text == "📢 Сообщение":
                    state["step"] = "broadcast"
                    await message.reply("Введите текст рассылки:")
                    return

            if state.get("step") == "block":
                target = await get_user_by_id_or_username(text)
                if target:
                    await set_block(target, True)
                    await message.reply(f"✅ Пользователь заблокирован")
                else:
                    await message.reply("❌ Не найден")
                state["step"] = "admin_menu"
                return

            if state.get("step") == "unblock":
                target = await get_user_by_id_or_username(text)
                if target:
                    await set_block(target, False)
                    await message.reply(f"✅ Пользователь разблокирован")
                else:
                    await message.reply("❌ Не найден")
                state["step"] = "admin_menu"
                return

            if state.get("step") == "add_balance":
                parts = text.split()
                if len(parts) != 2 or not parts[1].isdigit():
                    await message.reply("⚠ Формат: @username 100")
                    return
                target = await get_user_by_id_or_username(parts[0])
                amount = int(parts[1])
                if target:
                    await change_balance(target, amount)
                    await message.reply(f"✅ Баланс пополнен")
                else:
                    await message.reply("❌ Не найден")
                state["step"] = "admin_menu"
                return

            if state.get("step") == "broadcast":
                if text == "/cancel":
                    del user_states[uid]
                    await message.reply("❌ Отменено", reply_markup=admin_keyboard)
                    return
                
                state["broadcast_text"] = text
                users = await get_all_users()
                
                kb_confirm = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_broadcast")],
                    [InlineKeyboardButton("❌ Отменить", callback_data="cancel_broadcast")]
                ])
                
                await message.reply(
                    f"Отправить {len(users)} пользователям?\n\n{text}",
                    reply_markup=kb_confirm
                )
                return

    except Exception as e:
        print(f"Ошибка: {e}")

# ================= ОБРАБОТЧИК КОМАНД =================
@app.on_callback_query()
async def callbacks(client, call):
    uid = call.from_user.id
    data = call.data
    
    try:
        # Уведомление о переводе
        if data.startswith("notify_replenish_"):
            request_id = data.replace("notify_replenish_", "")
            
            if request_id not in app.replenish_requests:
                await call.answer("❌ Заявка устарела", show_alert=True)
                return
            
            request = app.replenish_requests[request_id]
            if request.get("confirmed"):
                await call.answer("✅ Уже обработано", show_alert=True)
                return
            
            await call.answer("✅ Уведомление отправлено", show_alert=False)
            
            await call.message.edit_text(
                f"💳 Заявка на {request['amount']} ₽ отправлена админу"
            )
            
            admin_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_replenish_{request_id}")],
                [InlineKeyboardButton("🔒 Заблокировать", callback_data=f"block_user_{request['user_id']}")]
            ])
            
            await app.send_message(
                ADMIN_ID,
                f"💳 Заявка\n"
                f"Пользователь: {request['full_name']} (@{request['username']})\n"
                f"ID: {request['user_id']}\n"
                f"Сумма: {request['amount']} ₽",
                reply_markup=admin_kb
            )
            return

        # Подтверждение пополнения
        if data.startswith("confirm_replenish_"):
            if uid != ADMIN_ID:
                await call.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            request_id = data.replace("confirm_replenish_", "")
            
            if request_id not in app.replenish_requests:
                await call.answer("❌ Заявка не найдена", show_alert=True)
                return
            
            request = app.replenish_requests[request_id]
            if request.get("confirmed"):
                await call.answer("✅ Уже подтверждено", show_alert=True)
                return
            
            await change_balance(request["user_id"], request["amount"])
            request["confirmed"] = True
            
            await call.message.edit_text(
                f"✅ Пополнение {request['amount']} ₽ подтверждено"
            )
            
            try:
                await app.send_message(
                    request["user_id"],
                    f"✅ Пополнение на {request['amount']} ₽ подтверждено!"
                )
            except:
                pass
            
            await call.answer("✅ Готово", show_alert=True)
            return

        # Блокировка
        if data.startswith("block_user_"):
            if uid != ADMIN_ID:
                await call.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            user_id = int(data.replace("block_user_", ""))
            await set_block(user_id, True)
            
            await call.message.edit_text(f"🔒 Пользователь {user_id} заблокирован")
            await call.answer("✅ Заблокирован", show_alert=True)
            return

        # Рассылка
        if data == "confirm_broadcast":
            if uid != ADMIN_ID:
                await call.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            if uid not in user_states:
                return
            
            text = user_states[uid].get("broadcast_text", "")
            users = await get_all_users()
            
            await call.message.edit_text(f"📢 Рассылка {len(users)} пользователям...")
            
            sent = 0
            for user_id in users:
                try:
                    await app.send_message(user_id, text, parse_mode=ParseMode.HTML)
                    sent += 1
                    await asyncio.sleep(0.05)
                except:
                    pass
            
            await app.send_message(uid, f"✅ Отправлено: {sent}")
            del user_states[uid]
            return
        
        if data == "cancel_broadcast":
            del user_states[uid]
            await call.message.edit_text("❌ Отменено")
            return

        # Оценка
        if data.startswith("rate_"):
            parts = data.split("_")
            code = parts[1]
            rating = int(parts[2])
            
            pool = await get_db()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT seller_id, buyer_id FROM deals WHERE code=$1", code)
                
                if not row:
                    await call.answer("❌ Сделка не найдена", show_alert=True)
                    return
                    
                if row['buyer_id'] != uid:
                    await call.answer("❌ Не ваш отзыв", show_alert=True)
                    return
                
                try:
                    await add_rating(row['seller_id'], uid, code, rating)
                    await call.message.edit_text(f"✅ Спасибо за оценку {rating}⭐")
                except Exception:
                    await call.answer("❌ Уже оценили", show_alert=True)
            return

        # Оплата
        if data.startswith("pay_"):
            code = data.split("_")[1]
            pool = await get_db()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT buyer_id, seller_id, name, amount, status FROM deals WHERE code=$1", code)
                
                if not row:
                    await call.answer("❌ Сделка не найдена", show_alert=True)
                    return
                    
                if row['status'] != "open":
                    await call.answer("❌ Сделка закрыта", show_alert=True)
                    return
                    
                bal = await get_balance(uid)
                if bal < row['amount']:
                    await call.answer("❌ Недостаточно средств", show_alert=True)
                    return
                
                if row['buyer_id'] is not None and row['buyer_id'] != uid:
                    await call.answer("❌ Не ваша сделка", show_alert=True)
                    return
                    
                await conn.execute("UPDATE deals SET buyer_id=$1 WHERE code=$2", uid, code)
                
                await change_balance(uid, -row['amount'])
                escrow[code] = {"buyer_id": uid, "seller_id": row['seller_id'], "amount": row['amount']}
                
                kb_deal = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{code}")],
                    [InlineKeyboardButton("⚖ Спор", callback_data=f"dispute_{code}")]
                ])
                
                await app.send_message(uid, f"✅ Оплачено {row['amount']} ₽", reply_markup=kb_deal)
                await app.send_message(row['seller_id'], f"🔔 Сделка {code} оплачена")
                
                await call.answer("✅ Оплата прошла", show_alert=False)
            return

        # Подтверждение сделки
        if data.startswith("confirm_"):
            code = data.split("_")[1]
            if code not in escrow:
                await call.answer("❌ Сделка не найдена", show_alert=True)
                return
            
            deal = escrow.pop(code)
            await change_balance(deal["seller_id"], deal["amount"])
            await update_seller_deal_stats(deal["seller_id"], deal["amount"])
            
            pool = await get_db()
            async with pool.acquire() as conn:
                await conn.execute("UPDATE deals SET status='completed' WHERE code=$1", code)
            
            rating_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("1⭐", f"rate_{code}_1"), InlineKeyboardButton("2⭐", f"rate_{code}_2"),
                 InlineKeyboardButton("3⭐", f"rate_{code}_3"), InlineKeyboardButton("4⭐", f"rate_{code}_4"),
                 InlineKeyboardButton("5⭐", f"rate_{code}_5")]
            ])
            
            await app.send_message(deal["buyer_id"], "✅ Сделка завершена! Оцените продавца:", reply_markup=rating_kb)
            await app.send_message(deal["seller_id"], f"💰 Получено {deal['amount']} ₽")
            
            await call.answer("✅ Готово", show_alert=False)
            return

        # Спор
        if data.startswith("dispute_"):
            code = data.split("_")[1]
            user_states[uid] = {"step": "dispute_msg", "deal_code": code}
            await call.message.reply("Опишите проблему:")
            await call.answer()
            return

        # Решение спора
        if data.startswith("resolve_"):
            parts = data.split("_")
            code = parts[1]
            choice = parts[2]
            
            pool = await get_db()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT buyer_id, seller_id, amount, status FROM deals WHERE code=$1", code)
                
                if not row or row['status'] != "dispute":
                    await call.answer("❌ Спор не найден", show_alert=True)
                    return
                
                if code in escrow:
                    amount = escrow.pop(code)["amount"]
                else:
                    amount = row['amount']
                
                if choice == "buyer":
                    await change_balance(row['buyer_id'], amount)
                    await app.send_message(row['buyer_id'], f"💰 Спор решен в вашу пользу")
                    await app.send_message(row['seller_id'], f"❌ Спор проигран")
                else:
                    await change_balance(row['seller_id'], amount)
                    await update_seller_deal_stats(row['seller_id'], amount)
                    await app.send_message(row['seller_id'], f"💰 Спор решен в вашу пользу")
                    await app.send_message(row['buyer_id'], f"❌ Спор проигран")
                
                await conn.execute("UPDATE deals SET status='closed' WHERE code=$1", code)
            
            await call.message.edit_text(f"✅ Спор решен в пользу {choice}")
            await call.answer()
            return

        await call.answer()
        
    except Exception as e:
        print(f"Ошибка: {e}")

# ================= ОЧИСТКА =================
async def cleanup_old_payments():
    current_time = time.time()
    expired = [rid for rid, req in app.replenish_requests.items() 
               if current_time - req.get("timestamp", 0) > 86400]
    for rid in expired:
        del app.replenish_requests[rid]
    return len(expired)

# ================= ЗАПУСК =================
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(init_db_pool())
        loop.run_until_complete(init_db())
        
        print("="*60)
        print("🚀 БОТ VAULTOR ЗАПУЩЕН")
        print("="*60)
        print("✅ PostgreSQL подключен")
        print("✅ Health server on port 10000")
        print("✅ Карты РФ: активны")
        print("="*60)
        
        app.run()
    except KeyboardInterrupt:
        print("\n❌ Бот остановлен")
    except Exception as e:
        print(f"❌ Ошибка запуска: {e}")
