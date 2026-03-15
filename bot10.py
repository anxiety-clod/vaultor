import os
import random
import asyncio
import aiosqlite
import aiohttp
import time
import signal
import sys
from datetime import datetime
from urllib.parse import urlencode
from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from dotenv import load_dotenv

# ================= LOAD ENV =================
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
PAY_ACCOUNT = os.getenv("PAY_ACCOUNT")  # Номер карты для пополнения
CARD_HOLDER = os.getenv("CARD_HOLDER")  # Имя владельца карты (опционально)
CARD_BANK = os.getenv("CARD_BANK")      # Название банка (опционально)

if not PAY_ACCOUNT:
    raise ValueError("PAY_ACCOUNT должен быть установлен в .env файле")

# ================= ОПТИМИЗИРОВАННЫЙ КЛИЕНТ =================
app = Client(
    "escrow_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN, 
    workers=100,
    max_concurrent_transmissions=50
)

DB = "bot.db"
user_states = {}
escrow = {}

# Хранилище для заявок на пополнение
app.replenish_requests = {}

# ===== SECURITY HELPERS =====
def ensure_deal_active(deal):
    if deal.get("status") == "closed":
        raise Exception("Deal already closed")

def close_deal(deal):
    deal["status"] = "closed"

# ================= ОПТИМИЗАЦИЯ БАЗЫ ДАННЫХ =================
db_connection = None

async def get_db():
    """Одно соединение с БД для всего бота"""
    global db_connection
    if db_connection is None:
        db_connection = await aiosqlite.connect(DB)
        await db_connection.execute("PRAGMA journal_mode=WAL")
        await db_connection.execute("PRAGMA synchronous=NORMAL")
        await db_connection.execute("PRAGMA cache_size=-20000")
        await db_connection.execute("PRAGMA temp_store=MEMORY")
    return db_connection

async def close_db_connection():
    """Закрывает соединение с базой данных"""
    global db_connection
    if db_connection:
        await db_connection.close()
        db_connection = None
        print("✅ Соединение с БД закрыто")

# ================= DATABASE =================
async def init_db():
    db = await get_db()
    
    # Создаем таблицу users если её нет
    await db.execute("""
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY,
        balance INTEGER DEFAULT 0,
        blocked INTEGER DEFAULT 0
    )
    """)
    
    # Проверяем и добавляем новые колонки в таблицу users
    cursor = await db.execute("PRAGMA table_info(users)")
    columns = await cursor.fetchall()
    column_names = [column[1] for column in columns]
    
    # Добавляем недостающие колонки
    if 'total_stars' not in column_names:
        await db.execute("ALTER TABLE users ADD COLUMN total_stars INTEGER DEFAULT 0")
        print("Колонка total_stars добавлена")
    
    if 'total_votes' not in column_names:
        await db.execute("ALTER TABLE users ADD COLUMN total_votes INTEGER DEFAULT 0")
        print("Колонка total_votes добавлена")
    
    if 'total_deals' not in column_names:
        await db.execute("ALTER TABLE users ADD COLUMN total_deals INTEGER DEFAULT 0")
        print("Колонка total_deals добавлена")
    
    if 'total_turnover' not in column_names:
        await db.execute("ALTER TABLE users ADD COLUMN total_turnover INTEGER DEFAULT 0")
        print("Колонка total_turnover добавлена")
    
    if 'registered_date' not in column_names:
        await db.execute("ALTER TABLE users ADD COLUMN registered_date TEXT")
        print("Колонка registered_date добавлена")
    
    # Создаем таблицу deals
    await db.execute("""
    CREATE TABLE IF NOT EXISTS deals(
        code TEXT PRIMARY KEY,
        seller_id INTEGER,
        buyer_id INTEGER,
        name TEXT,
        description TEXT,
        amount INTEGER,
        status TEXT
    )
    """)
    
    # Проверяем и добавляем колонку dispute_reason в таблицу deals
    cursor = await db.execute("PRAGMA table_info(deals)")
    columns = await cursor.fetchall()
    column_names = [column[1] for column in columns]
    
    if 'dispute_reason' not in column_names:
        await db.execute("ALTER TABLE deals ADD COLUMN dispute_reason TEXT DEFAULT ''")
        print("Колонка dispute_reason добавлена в таблицу deals")
    
    # Создаем таблицу ratings
    await db.execute("""
    CREATE TABLE IF NOT EXISTS ratings(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        seller_id INTEGER,
        buyer_id INTEGER,
        deal_code TEXT,
        rating INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(buyer_id, deal_code)
    )
    """)
    
    await db.commit()
    print("База данных успешно инициализирована")

async def get_all_users():
    """Получает список всех пользователей бота"""
    db = await get_db()
    cur = await db.execute("SELECT id FROM users")
    rows = await cur.fetchall()
    return [row[0] for row in rows]

async def get_balance(uid):
    db = await get_db()
    cur = await db.execute("SELECT balance FROM users WHERE id=?", (uid,))
    row = await cur.fetchone()
    
    if not row:
        await db.execute(
            "INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (uid, 0, 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
        )
        await db.commit()
        return 0
    return row[0]

async def change_balance(uid, amount):
    db = await get_db()
    cur = await db.execute("SELECT id FROM users WHERE id=?", (uid,))
    row = await cur.fetchone()
    
    if not row:
        await db.execute(
            "INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (uid, amount if amount > 0 else 0, 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
        )
    else:
        await db.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amount, uid))
    await db.commit()

async def set_block(uid, blocked=True):
    db = await get_db()
    cur = await db.execute("SELECT id FROM users WHERE id=?", (uid,))
    row = await cur.fetchone()
    
    if not row:
        await db.execute(
            "INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (uid, 0, 1 if blocked else 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
        )
    else:
        await db.execute("UPDATE users SET blocked=? WHERE id=?", (1 if blocked else 0, uid))
    await db.commit()

async def is_blocked(uid):
    db = await get_db()
    cur = await db.execute("SELECT blocked FROM users WHERE id=?", (uid,))
    row = await cur.fetchone()
    if not row:
        return False
    return row[0] == 1

async def get_seller_stats(seller_id):
    db = await get_db()
    cur = await db.execute("SELECT id FROM users WHERE id=?", (seller_id,))
    user_exists = await cur.fetchone()
    
    if not user_exists:
        await db.execute(
            "INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (seller_id, 0, 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
        )
        await db.commit()
    
    cur = await db.execute("""
        SELECT total_stars, total_votes, total_deals, total_turnover, blocked, registered_date 
        FROM users WHERE id=?
    """, (seller_id,))
    row = await cur.fetchone()
    
    if not row:
        return None
        
    total_stars, total_votes, total_deals, total_turnover, blocked, registered_date = row
    
    cur = await db.execute("""
        SELECT rating, COUNT(*) as count 
        FROM ratings 
        WHERE seller_id=? 
        GROUP BY rating 
        ORDER BY rating DESC
    """, (seller_id,))
    rating_dist = await cur.fetchall()
    
    dist = {5:0, 4:0, 3:0, 2:0, 1:0}
    for r, c in rating_dist:
        dist[r] = c
    
    rating = round(total_stars / total_votes, 1) if total_votes > 0 else 0
    
    return {
        "rating": rating,
        "total_votes": total_votes,
        "total_deals": total_deals,
        "total_turnover": total_turnover,
        "blocked": blocked,
        "registered_date": registered_date if registered_date else "неизвестно",
        "dist": dist
    }

async def add_rating(seller_id, buyer_id, deal_code, rating_value):
    db = await get_db()
    await db.execute("""
        INSERT INTO ratings (seller_id, buyer_id, deal_code, rating) 
        VALUES (?, ?, ?, ?)
    """, (seller_id, buyer_id, deal_code, rating_value))
    
    await db.execute("""
        UPDATE users 
        SET total_stars = total_stars + ?,
            total_votes = total_votes + 1
        WHERE id = ?
    """, (rating_value, seller_id))
    
    await db.commit()

async def update_seller_deal_stats(seller_id, amount):
    db = await get_db()
    await db.execute("""
        UPDATE users 
        SET total_deals = total_deals + 1,
            total_turnover = total_turnover + ?
        WHERE id = ?
    """, (amount, seller_id))
    await db.commit()

# ================= ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ =================
async def get_user_profile(uid):
    """Получает полный профиль пользователя"""
    db = await get_db()
    cur = await db.execute("""
        SELECT balance, total_deals, total_turnover, registered_date, blocked
        FROM users WHERE id=?
    """, (uid,))
    user_data = await cur.fetchone()
    
    if not user_data:
        await get_balance(uid)
        return await get_user_profile(uid)
    
    balance, total_deals, total_turnover, registered_date, blocked = user_data
    
    try:
        user = await app.get_users(uid)
        username = f"@{user.username}" if user.username else "нет"
        first_name = user.first_name or ""
        last_name = user.last_name or ""
        full_name = f"{first_name} {last_name}".strip()
    except:
        username = "недоступен"
        full_name = "неизвестно"
    
    cur = await db.execute("""
        SELECT COUNT(*) FROM deals 
        WHERE seller_id=? AND status='completed'
    """, (uid,))
    seller_deals = (await cur.fetchone())[0]
    
    cur = await db.execute("""
        SELECT COUNT(*) FROM deals 
        WHERE buyer_id=? AND status='completed'
    """, (uid,))
    buyer_deals = (await cur.fetchone())[0]
    
    return {
        "user_id": uid,
        "username": username,
        "full_name": full_name,
        "balance": balance,
        "registered_date": registered_date if registered_date else "неизвестно",
        "seller_deals": seller_deals,
        "buyer_deals": buyer_deals,
        "total_turnover": total_turnover,
        "blocked": blocked == 1
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
        print("Ошибка в start:", e)

# ================= TEXT HANDLER =================
@app.on_message(filters.text & ~filters.command(["start"]))
async def handle_text(client, message):
    uid = message.from_user.id
    text = message.text.strip()
    
    try:
        if await is_blocked(uid):
            await message.reply("❌ Вы заблокированы и не можете использовать бота.")
            return

        # ======== ОБРАБОТКА СООБЩЕНИЙ ДЛЯ СПОРА ========
        if uid in user_states and user_states[uid].get("step") == "dispute_msg":
            code = user_states[uid].get("deal_code")
            if not code:
                del user_states[uid]
                return
            
            dispute_text = text
            
            db = await get_db()
            cur = await db.execute("SELECT seller_id, buyer_id, name, amount FROM deals WHERE code=?", (code,))
            deal = await cur.fetchone()
            if deal:
                await db.execute("UPDATE deals SET status='dispute', dispute_reason=? WHERE code=?", (dispute_text[:200], code))
                await db.commit()
            
            if not deal:
                await message.reply("❌ Сделка не найдена.")
                del user_states[uid]
                return
            
            seller_id, buyer_id, name, amount = deal
            
            if uid == seller_id:
                disputer = "ПРОДАВЕЦ"
                disputer_emoji = "👤"
            else:
                disputer = "ПОКУПАТЕЛЬ"
                disputer_emoji = "👥"
            
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
                "Ожидайте решения спора. Администратор свяжется с вами при необходимости.",
                parse_mode=ParseMode.HTML
            )
            
            del user_states[uid]
            return

        # ======== НОВАЯ СИСТЕМА ПОПОЛНЕНИЯ ПО КАРТЕ ========
        if text == "💳 Пополнить":
            user_states[uid] = {"step": "card_amount"}
            await message.reply(
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "💳 <b>ПОПОЛНЕНИЕ БАЛАНСА ПО КАРТЕ</b> 💳\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Введите сумму пополнения от <b>1</b> до <b>5000</b> рублей:\n\n"
                "💡 <i>Только цифры, без пробелов и букв</i>\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
            return

        # ======== Баланс ========
        if text == "💰 Баланс":
            bal = await get_balance(uid)
            kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
            await message.reply(
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 <b>ВАШ БАЛАНС</b> 💰\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💳 <b>Текущий баланс:</b> {bal} ₽\n\n"
                f"🔒 Средства замораживаются при оплате сделки.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML,
                reply_markup=kb
            )
            return

        # ======== ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ ========
        if text == "👤 Профиль":
            profile = await get_user_profile(uid)
            
            if profile["blocked"]:
                status = "❌ ЗАБЛОКИРОВАН"
            else:
                status = "✅ АКТИВЕН"
            
            profile_text = (
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>ВАШ ПРОФИЛЬ</b> 👤\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🆔 <b>ID:</b> <code>{profile['user_id']}</code>\n"
                f"👤 <b>Имя:</b> {profile['full_name']}\n"
                f"📛 <b>Username:</b> {profile['username']}\n"
                f"📅 <b>Дата регистрации:</b> {profile['registered_date']}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 <b>Баланс:</b> {profile['balance']} ₽\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📊 <b>СТАТИСТИКА СДЕЛОК</b>\n"
                f"┣ ✅ <b>Как продавец:</b> {profile['seller_deals']}\n"
                f"┣ ✅ <b>Как покупатель:</b> {profile['buyer_deals']}\n"
                f"┗ 💰 <b>Общий оборот:</b> {profile['total_turnover']} ₽\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔰 <b>Статус:</b> {status}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            )
            
            kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
            await message.reply(profile_text, parse_mode=ParseMode.HTML, reply_markup=kb)
            return

        # ======== Поиск продавца ========
        if text == "🔍 Поиск продавца":
            user_states[uid] = {"step": "search_seller"}
            await message.reply(
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "🔍 <b>ПОИСК ПРОДАВЦА</b> 🔍\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Введите ID или @username интересующего вас продавца:\n\n"
                "📌 <b>Примеры:</b>\n"
                "   • @ivan_shop\n"
                "   • 123456789\n\n"
                "💡 <i>Вы получите полную статистику продавца, рейтинг и отзывы</i>\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
            return

        # ======== Создать сделку ========
        if text.startswith("📝"):
            user_states[uid] = {"step": "name", "data": {}}
            await message.reply(
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "📝 <b>СОЗДАНИЕ СДЕЛКИ</b> 📝\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "✨ Введите название сделки\n"
                "📌 <i>Например: Продажа iPhone 13</i>\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
            return

        # ======== Админ-панель ========
        if uid == ADMIN_ID and text == "⚙ Админ":
            user_states[uid] = {"step": "admin_menu"}
            await message.reply("🛠 Админ-панель:", reply_markup=admin_keyboard)
            return

        # ======== Обработка поиска продавца ========
        if uid in user_states and user_states[uid].get("step") == "search_seller":
            seller_identifier = text.strip()
            
            seller_id = await get_user_by_id_or_username(seller_identifier)
            
            if not seller_id:
                await message.reply(
                    "❌ <b>ОШИБКА ПОИСКА</b>\n\n"
                    "Пользователь не найден.\n"
                    "Проверьте правильность ввода ID или @username.",
                    parse_mode=ParseMode.HTML
                )
                del user_states[uid]
                return
            
            stats = await get_seller_stats(seller_id)
            
            try:
                seller_user = await app.get_users(seller_id)
                seller_username = f"@{seller_user.username}" if seller_user.username else "нет username"
                seller_name = seller_user.first_name or ""
                if seller_user.last_name:
                    seller_name += f" {seller_user.last_name}"
            except:
                seller_username = "нет username"
                seller_name = "неизвестно"
            
            if stats["blocked"]:
                reliability = "❌ ЗАБЛОКИРОВАН"
            elif stats["rating"] >= 4.5:
                reliability = "🏆 ОЧЕНЬ ВЫСОКИЙ"
            elif stats["rating"] >= 4.0:
                reliability = "⭐ ВЫСОКИЙ"
            elif stats["rating"] >= 3.5:
                reliability = "📊 СРЕДНИЙ"
            elif stats["rating"] > 0:
                reliability = "⚠ НИЖЕ СРЕДНЕГО"
            else:
                reliability = "🆕 НЕТ ОЦЕНОК"
            
            profile_text = (
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>ПРОФИЛЬ ПРОДАВЦА</b> 👤\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🆔 <b>ID:</b> <code>{seller_id}</code>\n"
                f"📛 <b>Username:</b> {seller_username}\n"
                f"👤 <b>Имя:</b> {seller_name}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"⭐ <b>РЕЙТИНГ</b> ⭐\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🏆 <b>Общий рейтинг:</b> {stats['rating']} / 5.0\n"
                f"📊 <b>На основе:</b> {stats['total_votes']} отзывов\n\n"
                f"5⭐ — {stats['dist'][5]} отзывов\n"
                f"4⭐ — {stats['dist'][4]} отзывов\n"
                f"3⭐ — {stats['dist'][3]} отзывов\n"
                f"2⭐ — {stats['dist'][2]} отзывов\n"
                f"1⭐ — {stats['dist'][1]} отзывов\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📦 <b>СТАТИСТИКА СДЕЛОК</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>Завершенных сделок:</b> {stats['total_deals']}\n"
                f"💰 <b>Оборот:</b> {stats['total_turnover']:,} ₽\n"
                f"📅 <b>На платформе:</b> с {stats['registered_date']}\n\n"
                f"🎯 <b>Надежность:</b> {reliability}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            )
            
            if stats["blocked"]:
                profile_text += "\n\n⚠️ <b>Пользователь заблокирован и недоступен для сделок</b>"
            
            kb = None
            if seller_username != "нет username":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 Написать продавцу", url=f"https://t.me/{seller_username.replace('@', '')}")]
                ])
            
            await message.reply(profile_text, parse_mode=ParseMode.HTML, reply_markup=kb)
            del user_states[uid]
            return

        # ======== Работа с состояниями пользователей ========
        if uid in user_states:
            state = user_states[uid]

            # Создание сделки
            if state.get("step") == "name":
                state["data"]["name"] = text
                state["step"] = "desc"
                await message.reply(
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "📝 <b>СОЗДАНИЕ СДЕЛКИ</b> 📝\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "📄 Введите описание сделки:\n\n"
                    "💡 <i>Опишите товар или услугу подробно</i>\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode=ParseMode.HTML
                )
                return
            if state.get("step") == "desc":
                state["data"]["desc"] = text
                state["step"] = "amount"
                await message.reply(
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "📝 <b>СОЗДАНИЕ СДЕЛКИ</b> 📝\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "💰 Введите сумму сделки (числом):\n\n"
                    "💡 <i>Только цифры, без пробелов</i>\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode=ParseMode.HTML
                )
                return
            if state.get("step") == "amount":
                if not text.isdigit():
                    await message.reply("⚠ Введите корректное число.")
                    return
                amount = int(text)
                code = str(random.randint(10000000, 99999999))
                db = await get_db()
                await db.execute(
                    "INSERT INTO deals(code, seller_id, buyer_id, name, description, amount, status) VALUES(?,?,?,?,?,?,?)",
                    (code, uid, None, state["data"]["name"], state["data"]["desc"], amount, "open")
                )
                await db.commit()
                bot_username = (await client.get_me()).username
                link = f"https://t.me/{bot_username}?start=deal_{code}"
                
                success_text = (
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🎉 <b>СДЕЛКА УСПЕШНО СОЗДАНА!</b> 🎉\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    
                    f"📦 <b>ИНФОРМАЦИЯ О СДЕЛКЕ:</b>\n"
                    f"▫️ <b>Название:</b> {state['data']['name']}\n"
                    f"▫️ <b>Описание:</b> {state['data']['desc']}\n"
                    f"▫️ <b>Сумма:</b> 💰 {amount:,} ₽\n"
                    f"▫️ <b>Код сделки:</b> <code>{code}</code>\n\n"
                    
                    f"🔗 <b>ССЫЛКА ДЛЯ ПОКУПАТЕЛЯ:</b>\n"
                    f"<code>{link}</code>\n\n"
                    
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📌 <b>ЧТО ДАЛЬШЕ?</b>\n"
                    f"1️⃣ Нажмите кнопку ниже, чтобы отправить ссылку покупателю\n"
                    f"2️⃣ Покупатель перейдет по ссылке и оплатит\n"
                    f"3️⃣ Деньги заморозятся на эскроу 🔒\n"
                    f"4️⃣ Выполните свои обязательства\n"
                    f"5️⃣ Получите оплату после подтверждения ✅\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                )
                
                share_kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📤 Отправить ссылку покупателю", switch_inline_query=f"🛡 VAULTOR | Код сделки: {code}\n💰 Сумма: {amount} ₽\n\nСсылка для подключения: {link}")]
                ])
                
                await message.reply(success_text, parse_mode=ParseMode.HTML, reply_markup=share_kb, disable_web_page_preview=True)
                del user_states[uid]
                return

            # ======== НОВАЯ СИСТЕМА: Ввод суммы для пополнения по карте ========
            if state.get("step") == "card_amount":
                try:
                    amount = float(text.strip())
                    
                    if amount < 1 or amount > 5000:
                        await message.reply(
                            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"❌ <b>ОШИБКА ВВОДА</b> ❌\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                            f"Сумма должна быть от <b>1</b> до <b>5000</b> рублей.\n\n"
                            f"Пожалуйста, введите корректную сумму:\n\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                            parse_mode=ParseMode.HTML
                        )
                        return
                    
                    amount = round(amount, 2)
                    
                    # Создаем уникальный ID заявки
                    request_id = f"replenish_{uid}_{int(time.time())}"
                    
                    # Получаем информацию о пользователе
                    user = message.from_user
                    username = f"@{user.username}" if user.username else "нет username"
                    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
                    
                    # Сохраняем заявку
                    app.replenish_requests[request_id] = {
                        "user_id": uid,
                        "username": username,
                        "full_name": full_name,
                        "amount": amount,
                        "timestamp": time.time(),
                        "confirmed": False
                    }
                    
                    # Формируем реквизиты для оплаты
                    pay_display = f"<code>{PAY_ACCOUNT}</code>"
                    card_holder_display = f"<b>{CARD_HOLDER}</b>" if CARD_HOLDER else "не указан"
                    bank_display = f"<b>{CARD_BANK}</b>" if CARD_BANK else "банк не указан"
                    
                    # Кнопка для уведомления о переводе
                    notify_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Я перевел деньги", callback_data=f"notify_replenish_{request_id}")]
                    ])
                    
                    await message.reply(
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"💳 <b>РЕКВИЗИТЫ ДЛЯ ПЕРЕВОДА</b> 💳\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"🏦 <b>Банк:</b> {bank_display}\n"
                        f"💳 <b>Номер карты/счета:</b>\n{pay_display}\n"
                        f"👤 <b>Получатель:</b> {card_holder_display}\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"💰 <b>Сумма пополнения:</b> {amount} ₽\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"📌 <b>ИНСТРУКЦИЯ:</b>\n"
                        f"1️⃣ Переведите указанную сумму <b>{amount} ₽</b> на карту выше\n"
                        f"2️⃣ После перевода нажмите кнопку <b>«✅ Я перевел деньги»</b>\n"
                        f"3️⃣ Ожидайте подтверждения администратором\n\n"
                        f"⚠️ <b>ВНИМАНИЕ!</b>\n"
                        f"• Ложное сообщение о переводе приведет к <b>БЛОКИРОВКЕ</b>\n"
                        f"• Средства зачисляются после проверки администратором\n"
                        f"• Время зачисления: от 5 до 30 минут\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"<i>Нажмите кнопку ниже после перевода</i>",
                        parse_mode=ParseMode.HTML,
                        reply_markup=notify_kb,
                        disable_web_page_preview=True
                    )
                    
                    del user_states[uid]
                    
                except ValueError:
                    await message.reply(
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"❌ <b>ОШИБКА ВВОДА</b> ❌\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"Пожалуйста, введите число (только цифры).\n\n"
                        f"Пример: <code>500</code> или <code>1500.50</code>\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                        parse_mode=ParseMode.HTML
                    )
                return

            # ======== Админ действия ========
            if state.get("step") == "admin_menu":
                if text == "⬅ Назад":
                    user_states[uid] = {}
                    await message.reply("Вы вернулись в главное меню.", reply_markup=main_keyboard_admin)
                    return
                if text == "🔒 Заблокировать пользователя":
                    state["step"] = "block"
                    await message.reply("Введите ID или @username для блокировки:")
                    return
                if text == "🔓 Разблокировать пользователя":
                    state["step"] = "unblock"
                    await message.reply("Введите ID или @username для разблокировки:")
                    return
                if text == "➕ Пополнить баланс":
                    state["step"] = "add_balance"
                    await message.reply("Введите ID или @username и сумму через пробел:")
                    return
                if text == "📢 Сообщение":
                    state["step"] = "broadcast"
                    await message.reply(
                        "📢 <b>РАССЫЛКА СООБЩЕНИЯ</b>\n\n"
                        "Введите текст сообщения, которое будет отправлено ВСЕМ пользователям бота.\n\n"
                        "💡 Поддерживается HTML-разметка.\n"
                        "❌ Для отмены введите /cancel",
                        parse_mode=ParseMode.HTML
                    )
                    return

            # Обработка ввода текста для рассылки
            if state.get("step") == "broadcast":
                if text == "/cancel":
                    del user_states[uid]
                    await message.reply("❌ Рассылка отменена.", reply_markup=admin_keyboard)
                    return
                
                msg_text = text
                state["broadcast_text"] = msg_text
                
                users = await get_all_users()
                user_count = len(users)
                
                kb_confirm = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить отправку", callback_data="confirm_broadcast")],
                    [InlineKeyboardButton("❌ Отменить", callback_data="cancel_broadcast")]
                ])
                
                await message.reply(
                    f"📢 <b>ПОДТВЕРЖДЕНИЕ РАССЫЛКИ</b>\n\n"
                    f"📨 Будет отправлено <b>{user_count}</b> пользователям\n\n"
                    f"📝 <b>Текст сообщения:</b>\n{msg_text}\n\n"
                    f"⚠️ Отправка может занять некоторое время.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_confirm
                )
                return

            # ======== Админ ввод данных ========
            if state.get("step") == "block":
                target = await get_user_by_id_or_username(text)
                if target:
                    await set_block(target, True)
                    await message.reply(f"✅ Пользователь {text} заблокирован.", reply_markup=admin_keyboard)
                else:
                    await message.reply("❌ Пользователь не найден.", reply_markup=admin_keyboard)
                state["step"] = "admin_menu"
                return

            if state.get("step") == "unblock":
                target = await get_user_by_id_or_username(text)
                if target:
                    await set_block(target, False)
                    await message.reply(f"✅ Пользователь {text} разблокирован.", reply_markup=admin_keyboard)
                else:
                    await message.reply("❌ Пользователь не найден.", reply_markup=admin_keyboard)
                state["step"] = "admin_menu"
                return

            if state.get("step") == "add_balance":
                parts = text.split()
                if len(parts) != 2 or not parts[1].isdigit():
                    await message.reply("⚠ Неверный формат. Пример: @username 100", reply_markup=admin_keyboard)
                    return
                target = await get_user_by_id_or_username(parts[0])
                amount = int(parts[1])
                if target:
                    await change_balance(target, amount)
                    try:
                        await app.send_message(
                            target,
                            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"💰 <b>БАЛАНС ПОПОЛНЕН</b> 💰\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                            f"Ваш баланс был пополнен на <b>{amount} ₽</b> администратором.\n\n"
                            f"💳 Теперь вы можете использовать средства для оплаты сделок.\n\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                            parse_mode=ParseMode.HTML
                        )
                    except:
                        pass
                    await message.reply(f"✅ Баланс пользователя {parts[0]} успешно пополнен на {amount} ₽", reply_markup=admin_keyboard)
                else:
                    await message.reply("❌ Пользователь не найден.", reply_markup=admin_keyboard)
                state["step"] = "admin_menu"
                return

    except Exception as e:
        print(f"Ошибка handle_text: {e}")

# ================= CALLBACK HANDLER =================
@app.on_callback_query()
async def callbacks(client, call):
    uid = call.from_user.id
    data = call.data
    try:
        # ===== НОВЫЙ ОБРАБОТЧИК: Уведомление о переводе =====
        if data.startswith("notify_replenish_"):
            request_id = data.replace("notify_replenish_", "")
            
            # Проверяем существование заявки
            if request_id not in app.replenish_requests:
                await call.answer("❌ Заявка не найдена или устарела", show_alert=True)
                return
            
            request = app.replenish_requests[request_id]
            
            # Проверяем, не подтверждена ли уже заявка
            if request.get("confirmed"):
                await call.answer("✅ Эта заявка уже была обработана", show_alert=True)
                return
            
            user_id = request["user_id"]
            amount = request["amount"]
            username = request["username"]
            full_name = request["full_name"]
            
            # Отвечаем на callback
            await call.answer("✅ Уведомление отправлено администратору", show_alert=False)
            
            # Обновляем сообщение (убираем кнопку)
            await call.message.edit_text(
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💳 <b>ЗАЯВКА НА ПОПОЛНЕНИЕ</b> 💳\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💰 <b>Сумма:</b> {amount} ₽\n\n"
                f"✅ <b>Статус:</b> Уведомление отправлено администратору\n"
                f"⏳ <b>Ожидайте подтверждения</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
            
            # Кнопки для админа (ДВЕ КНОПКИ: подтвердить и заблокировать)
            admin_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить пополнение", callback_data=f"confirm_replenish_{request_id}")],
                [InlineKeyboardButton("🔒 Заблокировать пользователя", callback_data=f"block_user_{user_id}")]
            ])
            
            # Отправляем админу уведомление
            await app.send_message(
                ADMIN_ID,
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💳 <b>ЗАЯВКА НА ПОПОЛНЕНИЕ</b> 💳\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 <b>Пользователь:</b> {full_name}\n"
                f"📛 <b>Username:</b> {username}\n"
                f"🆔 <b>ID:</b> <code>{user_id}</code>\n\n"
                f"💰 <b>Сумма:</b> {amount} ₽\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Выберите действие:</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=admin_kb
            )
            
            return

        # ===== НОВЫЙ ОБРАБОТЧИК: Подтверждение пополнения админом =====
        if data.startswith("confirm_replenish_"):
            if uid != ADMIN_ID:
                await call.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            request_id = data.replace("confirm_replenish_", "")
            
            # Проверяем существование заявки
            if request_id not in app.replenish_requests:
                await call.answer("❌ Заявка не найдена или уже обработана", show_alert=True)
                return
            
            request = app.replenish_requests[request_id]
            
            # Проверяем, не подтверждена ли уже
            if request.get("confirmed"):
                await call.answer("✅ Заявка уже подтверждена", show_alert=True)
                return
            
            user_id = request["user_id"]
            amount = request["amount"]
            
            # Зачисляем средства
            await change_balance(user_id, amount)
            
            # Помечаем заявку как подтвержденную
            request["confirmed"] = True
            
            # Отвечаем на callback и убираем кнопки
            await call.message.edit_text(
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>ПОПОЛНЕНИЕ ПОДТВЕРЖДЕНО</b> ✅\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 <b>Пользователь:</b> {request['full_name']}\n"
                f"📛 <b>Username:</b> {request['username']}\n"
                f"🆔 <b>ID:</b> <code>{user_id}</code>\n\n"
                f"💰 <b>Сумма:</b> {amount} ₽\n\n"
                f"✅ <b>Статус:</b> Средства зачислены\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
            
            # Уведомляем пользователя
            try:
                await app.send_message(
                    user_id,
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"✅ <b>ПОПОЛНЕНИЕ ПОДТВЕРЖДЕНО</b> ✅\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"💰 <b>Сумма:</b> {amount} ₽\n"
                    f"💳 <b>Новый баланс:</b> {await get_balance(user_id)} ₽\n\n"
                    f"Средства зачислены и доступны для использования.\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
            
            await call.answer("✅ Средства зачислены пользователю", show_alert=True)
            return

        # ===== НОВЫЙ ОБРАБОТЧИК: Блокировка пользователя админом =====
        if data.startswith("block_user_"):
            if uid != ADMIN_ID:
                await call.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            user_id = int(data.replace("block_user_", ""))
            
            # Блокируем пользователя
            await set_block(user_id, True)
            
            # Отвечаем на callback и убираем кнопки
            await call.message.edit_text(
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔒 <b>ПОЛЬЗОВАТЕЛЬ ЗАБЛОКИРОВАН</b> 🔒\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🆔 <b>ID:</b> <code>{user_id}</code>\n\n"
                f"✅ <b>Статус:</b> Пользователь заблокирован\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
            
            # Уведомляем пользователя о блокировке
            try:
                await app.send_message(
                    user_id,
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔒 <b>ВЫ БЫЛИ ЗАБЛОКИРОВАНЫ</b> 🔒\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Вы заблокированы за нарушение правил сервиса.\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
            
            await call.answer("✅ Пользователь заблокирован", show_alert=True)
            return

        # ===== Подтверждение рассылки =====
        if data == "confirm_broadcast":
            if uid != ADMIN_ID:
                await call.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            if uid not in user_states or user_states[uid].get("step") != "broadcast":
                await call.message.edit("❌ Ошибка: рассылка не найдена")
                await call.answer()
                return
            
            broadcast_text = user_states[uid].get("broadcast_text", "")
            users = await get_all_users()
            
            await call.message.edit(
                f"📢 <b>РАССЫЛКА ЗАПУЩЕНА</b>\n\n"
                f"👥 Всего пользователей: {len(users)}\n"
                f"⏳ Отправка...",
                parse_mode=ParseMode.HTML
            )
            
            await call.answer("✅ Рассылка начата", show_alert=False)
            
            tasks = []
            for user_id in users:
                tasks.append(app.send_message(user_id, broadcast_text, parse_mode=ParseMode.HTML))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            sent = sum(1 for r in results if not isinstance(r, Exception))
            failed = len(results) - sent
            
            await app.send_message(
                uid,
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📢 <b>РАССЫЛКА ЗАВЕРШЕНА</b> 📢\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✅ <b>Успешно доставлено:</b> {sent}\n"
                f"❌ <b>Не удалось доставить:</b> {failed}\n"
                f"📊 <b>Всего пользователей:</b> {len(users)}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
            
            if uid in user_states:
                del user_states[uid]
            
            user_states[uid] = {"step": "admin_menu"}
            await app.send_message(uid, "🛠 Админ-панель", reply_markup=admin_keyboard)
            return
        
        if data == "cancel_broadcast":
            if uid in user_states:
                del user_states[uid]
            await call.message.edit("❌ Рассылка отменена")
            
            user_states[uid] = {"step": "admin_menu"}
            await app.send_message(uid, "🛠 Админ-панель", reply_markup=admin_keyboard)
            await call.answer()
            return

        # ===== Оценка сделки =====
        if data.startswith("rate_"):
            parts = data.split("_")
            code = parts[1]
            rating = int(parts[2])
            
            db = await get_db()
            cur = await db.execute("SELECT seller_id, buyer_id, name FROM deals WHERE code=?", (code,))
            deal = await cur.fetchone()
            
            if not deal:
                await call.answer("❌ Сделка не найдена", show_alert=True)
                return
                
            seller_id, buyer_id, name = deal
            
            if buyer_id != uid:
                await call.answer("❌ Только покупатель может оценить сделку", show_alert=True)
                return
            
            try:
                await add_rating(seller_id, uid, code, rating)
                stats = await get_seller_stats(seller_id)
                
                await call.message.edit(
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"⭐ <b>СПАСИБО ЗА ОЦЕНКУ!</b> ⭐\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Вы поставили продавцу <b>{rating}⭐</b>\n\n"
                    f"📊 <b>Текущий рейтинг продавца:</b> {stats['rating']} ({stats['total_votes']} оценок)\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode=ParseMode.HTML
                )
                await call.answer("✅ Оценка сохранена", show_alert=False)
            except Exception as e:
                if "UNIQUE constraint failed" in str(e):
                    await call.answer("❌ Вы уже оценили эту сделку", show_alert=True)
                else:
                    print(f"Ошибка при сохранении оценки: {e}")
                    await call.answer("❌ Ошибка при сохранении оценки", show_alert=True)
            return

        # ===== Оплата сделки =====
        if data.startswith("pay_"):
            code = data.split("_")[1]
            db = await get_db()
            cur = await db.execute("SELECT buyer_id, seller_id, name, amount, status FROM deals WHERE code=?", (code,))
            deal = await cur.fetchone()
            
            if not deal:
                await call.answer("❌ Сделка не найдена", show_alert=True)
                return
                
            buyer_id, seller_id, name, amount, status = deal
            
            if status != "open":
                await call.answer("❌ Сделка уже оплачена или закрыта", show_alert=True)
                return
                
            bal = await get_balance(uid)
            if bal < amount:
                await call.answer("❌ Недостаточно средств", show_alert=True)
                return
            
            if buyer_id is not None and buyer_id != uid:
                await call.answer("❌ Эта сделка предназначена для другого покупателя", show_alert=True)
                return
                
            await db.execute("UPDATE deals SET buyer_id=? WHERE code=?", (uid, code))
            await db.commit()
            
            await change_balance(uid, -amount)
            escrow[code] = {"buyer_id": uid, "seller_id": seller_id, "amount": amount}

            bot_username = (await app.get_me()).username
            deal_link = f"https://t.me/{bot_username}?start=deal_{code}"

            buyer_user = await app.get_users(uid)
            buyer_username = f"@{buyer_user.username}" if buyer_user.username else "без username"
            seller_user = await app.get_users(seller_id)
            seller_username = f"@{seller_user.username}" if seller_user.username else "без username"

            kb_deal = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить выполнение", callback_data=f"confirm_{code}")],
                [InlineKeyboardButton("⚖ Открыть спор", callback_data=f"dispute_{code}")]
            ])

            await app.send_message(
                uid,
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💳 <b>ОПЛАТА ПРОШЛА УСПЕШНО</b> 💳\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📦 <b>Название:</b> {name}\n"
                f"💰 <b>Сумма:</b> {amount} ₽\n"
                f"🔢 <b>Код сделки:</b> {code}\n"
                f"👤 <b>Продавец:</b> {seller_username}\n\n"
                f"🔒 <b>Деньги заморожены на эскроу.</b>\n\n"
                f"⚠️ <b>Важно:</b> Не подтверждайте сделку до получения товара!\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_deal
            )

            await app.send_message(
                seller_id,
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔔 <b>СДЕЛКА ОПЛАЧЕНА</b> 🔔\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📦 <b>Название:</b> {name}\n"
                f"💰 <b>Сумма:</b> {amount} ₽\n"
                f"🔢 <b>Код сделки:</b> {code}\n"
                f"👤 <b>Покупатель:</b> {buyer_username}\n\n"
                f"💰 <b>Деньги заморожены на эскроу.</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⚖ Открыть спор", callback_data=f"dispute_{code}")]
                ])
            )

            await call.answer("✅ Оплата прошла успешно", show_alert=False)
            return

        # ===== Подтвердить выполнение сделки =====
        if data.startswith("confirm_"):
            code = data.split("_")[1]
            if code not in escrow:
                await call.answer("❌ Сделка не найдена или уже закрыта", show_alert=True)
                return
            deal = escrow.pop(code)
            
            await change_balance(deal["seller_id"], deal["amount"])
            await update_seller_deal_stats(deal["seller_id"], deal["amount"])
            
            db = await get_db()
            cur = await db.execute("SELECT name FROM deals WHERE code=?", (code,))
            row = await cur.fetchone()
            name = row[0] if row else "Сделка"
            await db.execute("UPDATE deals SET status='completed' WHERE code=?", (code,))
            await db.commit()
            
            try:
                buyer_user = await app.get_users(deal["buyer_id"])
                seller_user = await app.get_users(deal["seller_id"])
                buyer_username = f"@{buyer_user.username}" if buyer_user.username else "нет username"
                seller_username = f"@{seller_user.username}" if seller_user.username else "нет username"
            except:
                buyer_username = "нет username"
                seller_username = "нет username"
            
            seller_balance = await get_balance(deal["seller_id"])
            
            rating_kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("1⭐", callback_data=f"rate_{code}_1"),
                    InlineKeyboardButton("2⭐", callback_data=f"rate_{code}_2"),
                    InlineKeyboardButton("3⭐", callback_data=f"rate_{code}_3"),
                    InlineKeyboardButton("4⭐", callback_data=f"rate_{code}_4"),
                    InlineKeyboardButton("5⭐", callback_data=f"rate_{code}_5")
                ]
            ])
            
            buyer_text = (
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>СДЕЛКА УСПЕШНО ЗАВЕРШЕНА!</b> ✅\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📦 <b>Название:</b> {name}\n"
                f"💰 <b>Сумма:</b> {deal['amount']:,} ₽\n"
                f"👤 <b>Продавец:</b> {seller_username}\n\n"
                f"💫 <b>Статус:</b> Деньги переведены продавцу\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"👇 <b>Оцените продавца:</b>"
            )
            
            seller_text = (
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 <b>ПОЛУЧЕНА ОПЛАТА ЗА СДЕЛКУ!</b> 💰\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📦 <b>Название:</b> {name}\n"
                f"💰 <b>Сумма:</b> {deal['amount']:,} ₽\n"
                f"👤 <b>Покупатель:</b> {buyer_username}\n\n"
                f"💚 <b>Средства зачислены на ваш баланс!</b>\n"
                f"💰 <b>Текущий баланс:</b> {seller_balance:,} ₽\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            )
            
            await app.send_message(deal["buyer_id"], buyer_text, parse_mode=ParseMode.HTML, reply_markup=rating_kb)
            await app.send_message(deal["seller_id"], seller_text, parse_mode=ParseMode.HTML)
            
            await call.answer("✅ Сделка подтверждена", show_alert=False)
            return

        # ===== Открыть спор =====
        if data.startswith("dispute_"):
            code = data.split("_")[1]
            user_states[uid] = {"step": "dispute_msg", "deal_code": code}
            await call.message.reply(
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "⚖ <b>ОТКРЫТИЕ СПОРА</b> ⚖\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Опишите проблему в этом чате сообщением.\n\n"
                "💬 <i>Будьте максимально конкретны и честны.</i>\n"
                "Ваше сообщение будет отправлено администратору для решения спора.\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
            await call.answer()
            return

        # ===== Решение спора админом =====
        if data.startswith("resolve_"):
            parts = data.split("_")
            code = parts[1]
            choice = parts[2]
            
            db = await get_db()
            cur = await db.execute("SELECT buyer_id, seller_id, amount, name, status FROM deals WHERE code=?", (code,))
            deal = await cur.fetchone()
            
            if not deal:
                await call.answer("❌ Сделка не найдена", show_alert=True)
                return
                
            buyer_id, seller_id, amount, name, status = deal
            
            if status != "dispute":
                await call.answer("❌ Спор уже решён или не открыт", show_alert=True)
                return
            
            if code in escrow:
                escrow_deal = escrow.pop(code)
                amount = escrow_deal["amount"]
            
            if choice == "buyer":
                await change_balance(buyer_id, amount)
                
                await app.send_message(
                    buyer_id,
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💰 <b>СПОР РЕШЕН В ВАШУ ПОЛЬЗУ!</b> 💰\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📦 <b>Сделка:</b> {name}\n"
                    f"🔢 <b>Код:</b> {code}\n"
                    f"💰 <b>Сумма:</b> {amount} ₽\n\n"
                    f"Средства возвращены на ваш баланс.\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode=ParseMode.HTML
                )
                await app.send_message(
                    seller_id,
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"❌ <b>СПОР РЕШЕН В ПОЛЬЗУ ПОКУПАТЕЛЯ</b> ❌\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📦 <b>Сделка:</b> {name}\n"
                    f"🔢 <b>Код:</b> {code}\n"
                    f"💰 <b>Сумма:</b> {amount} ₽\n\n"
                    f"Деньги возвращены покупателю.\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode=ParseMode.HTML
                )
            else:
                await change_balance(seller_id, amount)
                await update_seller_deal_stats(seller_id, amount)
                
                await app.send_message(
                    seller_id,
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💰 <b>СПОР РЕШЕН В ВАШУ ПОЛЬЗУ!</b> 💰\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📦 <b>Сделка:</b> {name}\n"
                    f"🔢 <b>Код:</b> {code}\n"
                    f"💰 <b>Сумма:</b> {amount} ₽\n\n"
                    f"Средства зачислены на ваш баланс.\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode=ParseMode.HTML
                )
                await app.send_message(
                    buyer_id,
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"❌ <b>СПОР РЕШЕН В ПОЛЬЗУ ПРОДАВЦА</b> ❌\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📦 <b>Сделка:</b> {name}\n"
                    f"🔢 <b>Код:</b> {code}\n"
                    f"💰 <b>Сумма:</b> {amount} ₽\n\n"
                    f"Деньги переведены продавцу.\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    parse_mode=ParseMode.HTML
                )
            
            await db.execute("UPDATE deals SET status='closed' WHERE code=?", (code,))
            await db.commit()
            
            if code in escrow:
                escrow.pop(code)
                
            await call.message.edit(
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>СПОР РЕШЕН</b> ✅\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Решение принято в пользу <b>{'покупателя' if choice == 'buyer' else 'продавца'}</b>.\n"
                f"Уведомления отправлены сторонам.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
            await call.answer("✅ Спор решён", show_alert=False)
            return

        await call.answer()
        
    except Exception as e:
        print(f"Ошибка в callback: {e}")
        try:
            await call.answer("❌ Произошла ошибка", show_alert=True)
        except:
            pass

# ================= JOIN DEAL =================
async def join_deal(message, code):
    try:
        db = await get_db()
        cur = await db.execute("SELECT seller_id, name, description, amount, status, buyer_id FROM deals WHERE code=?", (code,))
        deal = await cur.fetchone()
        if not deal:
            await message.reply("❌ Сделка не найдена.")
            return
        seller_id, name, desc, amount, status, buyer_id = deal
        if status != "open":
            await message.reply("❌ Сделка недоступна.")
            return
            
        if buyer_id is not None:
            await message.reply("❌ К этой сделке уже подключился покупатель.")
            return

        buyer = message.from_user
        username = f"@{buyer.username}" if buyer.username else "без username"

        await db.execute("UPDATE deals SET buyer_id=? WHERE code=?", (buyer.id, code))
        await db.commit()

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

# ================= UTILS =================
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

async def cleanup_old_payments():
    """Очищает старые неиспользованные заявки"""
    current_time = time.time()
    expired = []
    
    for request_id, request in app.replenish_requests.items():
        if current_time - request.get("timestamp", 0) > 86400:  # 24 часа
            expired.append(request_id)
    
    for request_id in expired:
        del app.replenish_requests[request_id]
    
    return len(expired)

def signal_handler(sig, frame):
    """Обработчик сигналов для корректной остановки"""
    print("\n" + "="*60)
    print("🛑 ПОЛУЧЕН СИГНАЛ ОСТАНОВКИ")
    print("="*60)
    
    # Создаем новый цикл событий для закрытия соединения с БД
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Закрываем соединение с БД
        if db_connection:
            loop.run_until_complete(close_db_connection())
        print("✅ Ресурсы освобождены")
    except Exception as e:
        print(f"❌ Ошибка при освобождении ресурсов: {e}")
    finally:
        loop.close()
    
    print("="*60)
    print("👋 БОТ ОСТАНОВЛЕН")
    print("="*60)
    sys.exit(0)

# ================= RUN =================
if __name__ == "__main__":
    # Устанавливаем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("="*60)
    print("🚀 БОТ VAULTOR ЗАПУЩЕН")
    print("="*60)
    print("✅ Система пополнения по карте: АКТИВНА")
    print("✅ Профиль пользователя: АКТИВЕН")
    print("✅ Защита от ложных переводов: АКТИВНА")
    print("✅ Кнопка блокировки: АКТИВНА")
    print("="*60)
    print("🛑 Для остановки бота нажмите Ctrl+C")
    print("="*60)
    
    try:
        # Инициализация БД перед запуском
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(init_db())
        
        # Запуск бота
        app.run()
    except KeyboardInterrupt:
        # Этот блок может не сработать из-за signal_handler, но оставим для надежности
        print("\n" + "="*60)
        print("🛑 БОТ ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ")
        print("="*60)
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        print("="*60)
    finally:
        # Закрываем соединение с БД в случае аварийного завершения
        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(close_db_connection())
        except:
            pass
