import os
import random
import asyncio
import aiosqlite
import aiohttp
import time
from datetime import datetime, timedelta
from urllib.parse import urlencode
from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode
from dotenv import load_dotenv

# ================= НАСТРОЙКА ОКРУЖЕНИЯ =================
load_dotenv()

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
YOOMONEY_WALLET = os.getenv("YOOMONEY_WALLET")
YOOMONEY_TOKEN = os.getenv("YOOMONEY_TOKEN")

if not YOOMONEY_WALLET or not YOOMONEY_TOKEN:
    raise ValueError("YOOMONEY_WALLET и YOOMONEY_TOKEN должны быть установлены в .env файле")

app = Client("escrow_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=32)
DB = "bot.db"
user_states = {}
escrow = {}

# Хранилище для платежей ЮMoney
app.yoomoney_payments = {}

# ===== ЗАЩИТА ОТ ДВОЙНОГО НАЖАТИЯ =====
app.processing = {
    "pay": set(),
    "confirm": set(),
    "check": set(),
    "rate": set(),
    "dispute": set(),
    "resolve": set()
}

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ БЕЗОПАСНОСТИ =====
def ensure_deal_active(deal):
    if deal.get("status") == "closed":
        raise Exception("Deal already closed")

def close_deal(deal):
    deal["status"] = "closed"

# ================= БАЗА ДАННЫХ =================
async def init_db():
    async with aiosqlite.connect(DB) as db:
        # Таблица users
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY,
            balance REAL DEFAULT 0,
            blocked INTEGER DEFAULT 0,
            total_stars INTEGER DEFAULT 0,
            total_votes INTEGER DEFAULT 0,
            total_deals INTEGER DEFAULT 0,
            total_turnover REAL DEFAULT 0,
            registered_date TEXT
        )
        """)
        
        # Проверяем и добавляем колонки
        cursor = await db.execute("PRAGMA table_info(users)")
        columns = await cursor.fetchall()
        column_names = [column[1] for column in columns]
        
        if 'total_stars' not in column_names:
            await db.execute("ALTER TABLE users ADD COLUMN total_stars INTEGER DEFAULT 0")
        if 'total_votes' not in column_names:
            await db.execute("ALTER TABLE users ADD COLUMN total_votes INTEGER DEFAULT 0")
        if 'total_deals' not in column_names:
            await db.execute("ALTER TABLE users ADD COLUMN total_deals INTEGER DEFAULT 0")
        if 'total_turnover' not in column_names:
            await db.execute("ALTER TABLE users ADD COLUMN total_turnover REAL DEFAULT 0")
        if 'registered_date' not in column_names:
            await db.execute("ALTER TABLE users ADD COLUMN registered_date TEXT")
        
        # Таблица deals
        await db.execute("""
        CREATE TABLE IF NOT EXISTS deals(
            code TEXT PRIMARY KEY,
            seller_id INTEGER,
            buyer_id INTEGER,
            name TEXT,
            description TEXT,
            amount REAL,
            status TEXT,
            dispute_reason TEXT DEFAULT ''
        )
        """)
        
        # Таблица ratings
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
        
        # Таблица payments
        await db.execute("""
        CREATE TABLE IF NOT EXISTS payments(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT UNIQUE,
            user_id INTEGER,
            amount REAL,
            timestamp INTEGER,
            checked INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        await db.commit()
        print("✅ База данных инициализирована")

# ================= ФУНКЦИИ ДЛЯ ПЛАТЕЖЕЙ =================
async def save_payment(label, user_id, amount):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT OR REPLACE INTO payments (label, user_id, amount, timestamp) VALUES (?, ?, ?, ?)",
            (label, user_id, amount, int(time.time()))
        )
        await db.commit()

async def mark_payment_checked(label):
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE payments SET checked=1 WHERE label=?", (label,))
        await db.commit()

async def is_payment_checked(label):
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT checked FROM payments WHERE label=?", (label,))
        row = await cur.fetchone()
        return row[0] == 1 if row else False

async def get_user_pending_payments(user_id):
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute(
            "SELECT label, amount FROM payments WHERE user_id=? AND checked=0 ORDER BY timestamp DESC",
            (user_id,)
        )
        return await cur.fetchall()

# ================= ФУНКЦИИ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ =================
async def get_balance(uid):
    async with aiosqlite.connect(DB) as db:
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
    async with aiosqlite.connect(DB) as db:
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

async def is_blocked(uid):
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT blocked FROM users WHERE id=?", (uid,))
        row = await cur.fetchone()
        return row[0] == 1 if row else False

async def set_block(uid, blocked=True):
    async with aiosqlite.connect(DB) as db:
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

async def get_all_users():
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT id FROM users")
        rows = await cur.fetchall()
        return [row[0] for row in rows]

async def get_seller_stats(seller_id):
    async with aiosqlite.connect(DB) as db:
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
    async with aiosqlite.connect(DB) as db:
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
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            UPDATE users 
            SET total_deals = total_deals + 1,
                total_turnover = total_turnover + ?
            WHERE id = ?
        """, (amount, seller_id))
        await db.commit()

# ================= ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ =================
async def get_user_profile(uid):
    async with aiosqlite.connect(DB) as db:
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
            "🔥 <b>VAULTOR — Безопасный escrow-сервис</b>\n\n"
            "▫️ Мгновенные платежи через ЮMoney\n"
            "▫️ 100% зачисление средств\n"
            "▫️ Защита сделок\n"
            "▫️ Рейтинг продавцов\n"
            "▫️ Арбитраж при спорах\n\n"
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
            await message.reply("❌ Вы заблокированы")
            return

        # ======== ОБРАБОТКА СООБЩЕНИЙ ДЛЯ СПОРА ========
        if uid in user_states and user_states[uid].get("step") == "dispute_msg":
            code = user_states[uid].get("deal_code")
            if not code:
                del user_states[uid]
                return
            
            dispute_text = text
            
            async with aiosqlite.connect(DB) as db:
                cur = await db.execute("SELECT seller_id, buyer_id, name, amount FROM deals WHERE code=?", (code,))
                deal = await cur.fetchone()
                if deal:
                    await db.execute("UPDATE deals SET status='dispute', dispute_reason=? WHERE code=?", (dispute_text[:200], code))
                    await db.commit()
            
            if not deal:
                await message.reply("❌ Сделка не найдена")
                del user_states[uid]
                return
            
            seller_id, buyer_id, name, amount = deal
            
            disputer = "ПРОДАВЕЦ" if uid == seller_id else "ПОКУПАТЕЛЬ"
            disputer_emoji = "👤" if uid == seller_id else "👥"
            
            kb_dispute = InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 Отдать покупателю", callback_data=f"resolve_{code}_buyer")],
                [InlineKeyboardButton("👥 Отдать продавцу", callback_data=f"resolve_{code}_seller")]
            ])
            
            await app.send_message(
                ADMIN_ID,
                f"⚖ <b>СПОР</b>\n\n"
                f"{disputer_emoji} {disputer}: @{message.from_user.username or 'нет'}\n"
                f"🔢 Код: {code}\n"
                f"💰 Сумма: {amount} ₽\n"
                f"💬 Причина: {dispute_text}",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_dispute
            )
            
            await message.reply("✅ Сообщение отправлено администратору")
            del user_states[uid]
            return

        # ======== ПОПОЛНЕНИЕ ========
        if text == "💳 Пополнить":
            user_states[uid] = {"step": "yoomoney_amount"}
            await message.reply(
                "💰 <b>ПОПОЛНЕНИЕ БАЛАНСА</b>\n\n"
                "Введите сумму от 50 до 10000 рублей:",
                parse_mode=ParseMode.HTML
            )
            return

        # ======== БАЛАНС ========
        if text == "💰 Баланс":
            bal = await get_balance(uid)
            kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
            await message.reply(
                f"💳 Ваш баланс: {bal:.2f} ₽\n\n"
                "🔒 Средства замораживаются при оплате сделки.",
                reply_markup=kb
            )
            return

        # ======== ПРОФИЛЬ ========
        if text == "👤 Профиль":
            profile = await get_user_profile(uid)
            
            status = "❌ ЗАБЛОКИРОВАН" if profile["blocked"] else "✅ Активен"
            
            profile_text = (
                f"👤 <b>ВАШ ПРОФИЛЬ</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🆔 <b>ID:</b> <code>{profile['user_id']}</code>\n"
                f"👤 <b>Имя:</b> {profile['full_name']}\n"
                f"📛 <b>Username:</b> {profile['username']}\n"
                f"📅 <b>Дата регистрации:</b> {profile['registered_date']}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 <b>Баланс:</b> {profile['balance']:.2f} ₽\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>СТАТИСТИКА СДЕЛОК</b>\n"
                f"┣ ✅ <b>Как продавец:</b> {profile['seller_deals']}\n"
                f"┣ ✅ <b>Как покупатель:</b> {profile['buyer_deals']}\n"
                f"┗ 💰 <b>Общий оборот:</b> {profile['total_turnover']:.2f} ₽\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔰 <b>Статус:</b> {status}"
            )
            
            kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
            await message.reply(profile_text, parse_mode=ParseMode.HTML, reply_markup=kb)
            return

        # ======== ПОИСК ПРОДАВЦА ========
        if text == "🔍 Поиск продавца":
            user_states[uid] = {"step": "search_seller"}
            await message.reply(
                "🔍 <b>ПОИСК ПРОДАВЦА</b>\n\n"
                "Введите ID или @username продавца:",
                parse_mode=ParseMode.HTML
            )
            return

        # ======== СОЗДАТЬ СДЕЛКУ ========
        if text == "📝 Создать сделку":
            user_states[uid] = {"step": "name", "data": {}}
            await message.reply("✨ Введите название сделки:")
            return

        # ======== АДМИН-ПАНЕЛЬ ========
        if uid == ADMIN_ID and text == "⚙ Админ":
            user_states[uid] = {"step": "admin_menu"}
            await message.reply("🛠 Админ-панель:", reply_markup=admin_keyboard)
            return

        # ======== ОБРАБОТКА ПОИСКА ПРОДАВЦА ========
        if uid in user_states and user_states[uid].get("step") == "search_seller":
            seller_identifier = text.strip()
            
            seller_id = await get_user_by_id_or_username(seller_identifier)
            
            if not seller_id:
                await message.reply(
                    "❌ <b>ОШИБКА</b>\n\nПользователь не найден.",
                    parse_mode=ParseMode.HTML
                )
                del user_states[uid]
                return
            
            stats = await get_seller_stats(seller_id)
            
            try:
                seller_user = await app.get_users(seller_id)
                seller_username = f"@{seller_user.username}" if seller_user.username else "нет"
                seller_name = f"{seller_user.first_name or ''} {seller_user.last_name or ''}".strip()
            except:
                seller_username = "нет"
                seller_name = "неизвестно"
            
            if stats["blocked"]:
                reliability = "❌ ЗАБЛОКИРОВАН"
            elif stats["rating"] >= 4.5:
                reliability = "🏆 Очень высокий"
            elif stats["rating"] >= 4.0:
                reliability = "⭐ Высокий"
            elif stats["rating"] >= 3.5:
                reliability = "📊 Средний"
            elif stats["rating"] > 0:
                reliability = "⚠ Ниже среднего"
            else:
                reliability = "🆕 Нет оценок"
            
            profile_text = (
                f"👤 <b>ПРОФИЛЬ ПРОДАВЦА</b>\n\n"
                f"🆔 <b>ID:</b> <code>{seller_id}</code>\n"
                f"📛 <b>Username:</b> {seller_username}\n"
                f"👤 <b>Имя:</b> {seller_name}\n\n"
                f"⭐ <b>РЕЙТИНГ</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🏆 <b>Общий рейтинг:</b> {stats['rating']} / 5.0\n"
                f"📊 <b>На основе:</b> {stats['total_votes']} отзывов\n\n"
                f"5⭐ — {stats['dist'][5]}\n"
                f"4⭐ — {stats['dist'][4]}\n"
                f"3⭐ — {stats['dist'][3]}\n"
                f"2⭐ — {stats['dist'][2]}\n"
                f"1⭐ — {stats['dist'][1]}\n\n"
                f"📦 <b>СТАТИСТИКА</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>Сделок:</b> {stats['total_deals']}\n"
                f"💰 <b>Оборот:</b> {stats['total_turnover']:.2f} ₽\n"
                f"📅 <b>На платформе:</b> с {stats['registered_date']}\n\n"
                f"🎯 <b>Надежность:</b> {reliability}"
            )
            
            if stats["blocked"]:
                profile_text += "\n\n⚠️ <b>Пользователь заблокирован</b>"
            
            kb = None
            if seller_username != "нет":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔗 Написать продавцу", url=f"https://t.me/{seller_username.replace('@', '')}")]
                ])
            
            await message.reply(profile_text, parse_mode=ParseMode.HTML, reply_markup=kb)
            del user_states[uid]
            return

        # ======== РАБОТА С СОСТОЯНИЯМИ ========
        if uid in user_states:
            state = user_states[uid]

            # СОЗДАНИЕ СДЕЛКИ
            if state.get("step") == "name":
                state["data"]["name"] = text
                state["step"] = "desc"
                await message.reply("📄 Введите описание сделки:")
                return
            if state.get("step") == "desc":
                state["data"]["desc"] = text
                state["step"] = "amount"
                await message.reply("💰 Введите сумму сделки (числом):")
                return
            if state.get("step") == "amount":
                if not text.replace(".", "").isdigit():
                    await message.reply("⚠ Введите корректное число")
                    return
                amount = float(text)
                code = str(random.randint(10000000, 99999999))
                async with aiosqlite.connect(DB) as db:
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
                    f"📦 <b>Название:</b> {state['data']['name']}\n"
                    f"📄 <b>Описание:</b> {state['data']['desc']}\n"
                    f"💰 <b>Сумма:</b> {amount:.2f} ₽\n"
                    f"🔢 <b>Код сделки:</b> <code>{code}</code>\n\n"
                    f"🔗 <b>ССЫЛКА ДЛЯ ПОКУПАТЕЛЯ:</b>\n"
                    f"<code>{link}</code>\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                )
                
                share_kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📤 Отправить ссылку", switch_inline_query=f"Сделка {code}\nСумма: {amount:.2f} ₽\nСсылка: {link}")]
                ])
                
                await message.reply(success_text, parse_mode=ParseMode.HTML, reply_markup=share_kb, disable_web_page_preview=True)
                del user_states[uid]
                return

            # ======== ПОПОЛНЕНИЕ ЧЕРЕЗ ЮMONEY ========
            if state.get("step") == "yoomoney_amount":
                try:
                    amount = float(text.strip())
                    
                    if amount < 50 or amount > 10000:
                        await message.reply(
                            f"❌ <b>Неверная сумма</b>\n\n"
                            f"Сумма должна быть от <b>50</b> до <b>10000</b> рублей.",
                            parse_mode=ParseMode.HTML
                        )
                        return
                    
                    amount = round(amount, 2)
                    
                    old_payments = await get_user_pending_payments(uid)
                    if old_payments:
                        print(f"🗑 Найдено {len(old_payments)} старых платежей пользователя {uid}")
                    
                    label = f"user_{uid}_{int(time.time())}"
                    
                    await save_payment(label, uid, amount)
                    
                    params = {
                        "receiver": YOOMONEY_WALLET,
                        "quickpay-form": "shop",
                        "targets": "Пополнение VAULTOR",
                        "paymentType": "SB",
                        "sum": amount,
                        "label": label,
                        "successURL": f"https://t.me/{(await app.get_me()).username}"
                    }
                    
                    payment_url = "https://yoomoney.ru/quickpay/confirm.xml?" + urlencode(params)
                    
                    app.yoomoney_payments[label] = {
                        "user_id": uid,
                        "amount": amount,
                        "timestamp": time.time(),
                        "checked": False
                    }
                    
                    check_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Я оплатил", callback_data=f"check_payment_{label}")]
                    ])
                    
                    await message.reply(
                        f"💰 <b>СЧЕТ НА ОПЛАТУ</b> 💰\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n"
                        f"💵 <b>Сумма:</b> {amount} ₽\n"
                        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"🔗 <b>Ссылка для оплаты:</b>\n"
                        f"<code>{payment_url}</code>\n\n"
                        f"📌 <b>Инструкция:</b>\n"
                        f"1️⃣ Перейдите по ссылке выше\n"
                        f"2️⃣ Выберите способ оплаты:\n"
                        f"   • Карта (Сбер, Тинькофф) — комиссия 2%\n"
                        f"   • ЮMoney кошелёк — комиссия 0%\n"
                        f"   • СБП — комиссия 0%\n"
                        f"3️⃣ Подтвердите платеж\n"
                        f"4️⃣ Нажмите кнопку <b>«✅ Я оплатил»</b>\n\n"
                        f"⚠️ <b>ВАЖНО!</b>\n"
                        f"• Используйте ТОЛЬКО ЭТУ ссылку\n"
                        f"• Если создадите новую - старая может не сработать\n"
                        f"• Деньги зачисляются автоматически в течение 1-2 минут\n"
                        f"• На баланс поступит ровно <b>{amount} ₽</b>\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━━",
                        parse_mode=ParseMode.HTML,
                        reply_markup=check_kb,
                        disable_web_page_preview=True
                    )
                    
                    del user_states[uid]
                    
                except ValueError:
                    await message.reply(
                        "❌ <b>Ошибка ввода</b>\n\n"
                        "Пожалуйста, введите число.",
                        parse_mode=ParseMode.HTML
                    )
                return

            # ======== АДМИН ДЕЙСТВИЯ ========
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
                    await message.reply(
                        "📢 <b>РАССЫЛКА</b>\n\n"
                        "Введите текст сообщения:",
                        parse_mode=ParseMode.HTML
                    )
                    return

            if state.get("step") == "broadcast":
                if text == "/cancel":
                    del user_states[uid]
                    await message.reply("❌ Рассылка отменена", reply_markup=admin_keyboard)
                    return
                
                msg_text = text
                state["broadcast_text"] = msg_text
                
                users = await get_all_users()
                user_count = len(users)
                
                kb_confirm = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_broadcast")],
                    [InlineKeyboardButton("❌ Отменить", callback_data="cancel_broadcast")]
                ])
                
                await message.reply(
                    f"📢 <b>ПОДТВЕРЖДЕНИЕ</b>\n\n"
                    f"📨 Будет отправлено <b>{user_count}</b> пользователям\n\n"
                    f"📝 <b>Текст:</b>\n{msg_text}\n\n"
                    f"⚠️ Отправка может занять некоторое время.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_confirm
                )
                return

            if state.get("step") == "block":
                target = await get_user_by_id_or_username(text)
                if target:
                    await set_block(target, True)
                    await message.reply(f"✅ Пользователь {text} заблокирован", reply_markup=admin_keyboard)
                else:
                    await message.reply("❌ Пользователь не найден", reply_markup=admin_keyboard)
                state["step"] = "admin_menu"
                return

            if state.get("step") == "unblock":
                target = await get_user_by_id_or_username(text)
                if target:
                    await set_block(target, False)
                    await message.reply(f"✅ Пользователь {text} разблокирован", reply_markup=admin_keyboard)
                else:
                    await message.reply("❌ Пользователь не найден", reply_markup=admin_keyboard)
                state["step"] = "admin_menu"
                return

            if state.get("step") == "add_balance":
                parts = text.split()
                if len(parts) == 2 and parts[1].replace(".", "").isdigit():
                    target = await get_user_by_id_or_username(parts[0])
                    amount = float(parts[1])
                    if target:
                        await change_balance(target, amount)
                        try:
                            await app.send_message(
                                target,
                                f"💰 Ваш баланс пополнен на {amount:.2f} ₽"
                            )
                        except:
                            pass
                        await message.reply(f"✅ Баланс {parts[0]} пополнен на {amount:.2f} ₽", reply_markup=admin_keyboard)
                    else:
                        await message.reply("❌ Пользователь не найден", reply_markup=admin_keyboard)
                else:
                    await message.reply("⚠ Формат: ID сумма", reply_markup=admin_keyboard)
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
        # ===== ПРОВЕРКА ПЛАТЕЖЕЙ ЮMONEY =====
        if data.startswith("check_payment_"):
            # Отвечаем на callback сразу
            await call.answer("🔄 Проверяю...", show_alert=False)
            
            label = data.replace("check_payment_", "")
            
            # Защита от двойного нажатия
            if label in app.processing["check"]:
                await call.message.reply("⏳ Проверка уже выполняется...")
                return
            app.processing["check"].add(label)
            
            try:
                print(f"\n🔍 Проверка платежа: {label}")
                
                if label not in app.yoomoney_payments:
                    async with aiosqlite.connect(DB) as db:
                        cur = await db.execute("SELECT user_id, amount, checked FROM payments WHERE label=?", (label,))
                        payment = await cur.fetchone()
                    
                    if not payment:
                        await call.message.edit_text("❌ Платеж не найден")
                        return
                    
                    user_id, expected_amount, checked = payment
                    if checked == 1:
                        await call.message.edit_text("✅ Платеж уже зачислен")
                        return
                    
                    payment = {
                        "user_id": user_id,
                        "amount": expected_amount,
                        "checked": False
                    }
                else:
                    payment = app.yoomoney_payments[label]
                    user_id = payment['user_id']
                    expected_amount = payment['amount']
                
                if payment.get("checked"):
                    await call.message.edit_text("✅ Платеж уже зачислен")
                    return
                
                status_msg = await call.message.reply("🔄 Проверка платежа...")
                
                found = False
                payment_info = ""
                
                for attempt in range(5):
                    if attempt > 0:
                        await status_msg.edit_text(f"🔄 Попытка {attempt+1}/5... (ждем 5 сек)")
                        await asyncio.sleep(5)
                    
                    try:
                        url = "https://yoomoney.ru/api/operation-history"
                        headers = {"Authorization": f"Bearer {YOOMONEY_TOKEN}"}
                        params = {"records": 50}
                        
                        async with aiohttp.ClientSession() as session:
                            async with session.post(url, headers=headers, data=params) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    operations = data.get('operations', [])
                                    
                                    for op in operations:
                                        op_status = op.get('status')
                                        op_amount = float(op.get('amount', 0))
                                        op_label = op.get('label', '')
                                        
                                        if op_label == label and op_status == "success":
                                            found = True
                                            payment_info = f"метка: {op_label}, сумма: {op_amount:.2f}₽"
                                            break
                                        
                                        if op_status == "success" and abs(op_amount - expected_amount) < 0.05:
                                            found = True
                                            payment_info = f"сумма: {op_amount:.2f}₽"
                                            break
                                    
                                    if found:
                                        break
                    except Exception as e:
                        print(f"Ошибка API: {e}")
                
                if found:
                    await change_balance(user_id, expected_amount)
                    
                    if label in app.yoomoney_payments:
                        app.yoomoney_payments[label]["checked"] = True
                    
                    await mark_payment_checked(label)
                    
                    new_balance = await get_balance(user_id)
                    
                    await status_msg.edit_text(
                        f"✅ <b>ПЛАТЕЖ ПОДТВЕРЖДЕН!</b>\n\n"
                        f"💰 Сумма: {expected_amount} ₽\n"
                        f"💳 Новый баланс: {new_balance} ₽\n\n"
                        f"🔍 Найдено по: {payment_info}",
                        parse_mode=ParseMode.HTML
                    )
                    
                    await call.answer("✅ Платеж успешно зачислен!", show_alert=True)
                else:
                    await status_msg.edit_text(
                        f"❌ <b>ПЛАТЕЖ НЕ НАЙДЕН</b>\n\n"
                        f"💰 Ожидаемая сумма: {expected_amount} ₽\n\n"
                        f"📌 Подождите 2-3 минуты и попробуйте снова.",
                        parse_mode=ParseMode.HTML
                    )
                    
                    await call.answer("❌ Платеж не найден", show_alert=True)
                
            finally:
                app.processing["check"].discard(label)
            
            return

        # ===== ОПЛАТА СДЕЛКИ (С ЗАЩИТОЙ ОТ ДВОЙНОГО НАЖАТИЯ) =====
        if data.startswith("pay_"):
            code = data.split("_")[1]
            
            await call.answer("⏳ Обрабатываю оплату...", show_alert=False)
            
            # Защита от двойного нажатия
            if code in app.processing["pay"]:
                await call.message.reply("⏳ Оплата уже обрабатывается, подождите...")
                return
            app.processing["pay"].add(code)
            
            try:
                async with aiosqlite.connect(DB) as db:
                    cur = await db.execute("SELECT buyer_id, seller_id, name, amount, status FROM deals WHERE code=?", (code,))
                    deal = await cur.fetchone()
                
                if not deal:
                    await call.message.reply("❌ Сделка не найдена")
                    return
                    
                buyer_id, seller_id, name, amount, status = deal
                
                if status != "open":
                    await call.message.reply("❌ Сделка уже оплачена или закрыта")
                    return
                    
                if buyer_id is not None:
                    await call.message.reply("❌ У этой сделки уже есть покупатель")
                    return
                
                bal = await get_balance(uid)
                if bal < amount:
                    await call.message.reply("❌ Недостаточно средств")
                    return
                
                # Транзакция с повторной проверкой
                async with aiosqlite.connect(DB) as db:
                    cur = await db.execute("SELECT status, buyer_id FROM deals WHERE code=?", (code,))
                    check = await cur.fetchone()
                    if check[0] != "open" or check[1] is not None:
                        await call.message.reply("❌ Сделку уже оплатили")
                        return
                    
                    await db.execute("UPDATE deals SET buyer_id=?, status='paid' WHERE code=?", (uid, code))
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
                    f"💳 Вы успешно оплатили сделку!\n\n"
                    f"📦 Название: {name}\n"
                    f"💰 Сумма: {amount} ₽\n"
                    f"🔢 Код сделки: {code}\n"
                    f"🔗 Ссылка: {deal_link}\n"
                    f"👤 Продавец: {seller_username} | 🆔 {seller_id}\n\n"
                    "🔒 Деньги заморожены на эскроу.",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_deal
                )

                await app.send_message(
                    seller_id,
                    f"🔔 Покупатель оплатил вашу сделку!\n\n"
                    f"📦 Название: {name}\n"
                    f"💰 Сумма: {amount} ₽\n"
                    f"🔢 Код сделки: {code}\n"
                    f"👤 Покупатель: {buyer_username} | 🆔 {uid}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⚖ Открыть спор", callback_data=f"dispute_{code}")]
                    ])
                )
                
            except Exception as e:
                print(f"❌ Ошибка при оплате: {e}")
                await call.message.reply("❌ Произошла ошибка при оплате")
            finally:
                app.processing["pay"].discard(code)
            
            return

        # ===== ПОДТВЕРДИТЬ ВЫПОЛНЕНИЕ СДЕЛКИ =====
        if data.startswith("confirm_"):
            code = data.split("_")[1]
            
            await call.answer("⏳ Подтверждаю сделку...", show_alert=False)
            
            # Защита от двойного нажатия
            if code in app.processing["confirm"]:
                await call.message.reply("⏳ Подтверждение уже обрабатывается...")
                return
            app.processing["confirm"].add(code)
            
            try:
                if code not in escrow:
                    await call.message.reply("❌ Сделка не найдена или уже закрыта")
                    return
                
                deal = escrow.pop(code)
                
                async with aiosqlite.connect(DB) as db:
                    cur = await db.execute("SELECT status FROM deals WHERE code=?", (code,))
                    row = await cur.fetchone()
                    if row and row[0] == 'completed':
                        await call.message.reply("✅ Сделка уже подтверждена")
                        return
                
                await change_balance(deal["seller_id"], deal["amount"])
                await update_seller_deal_stats(deal["seller_id"], deal["amount"])
                
                async with aiosqlite.connect(DB) as db:
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
                    f"✅ <b>СДЕЛКА УСПЕШНО ЗАВЕРШЕНА!</b> ✅\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📦 <b>Детали сделки:</b>\n"
                    f"┣ 🔢 Код: <code>{code}</code>\n"
                    f"┣ 📝 Название: {name}\n"
                    f"┣ 💰 Сумма: {deal['amount']:.2f} ₽\n"
                    f"┗ 👤 Продавец: {seller_username}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💫 <b>Статус:</b> ✓ Деньги переведены продавцу\n\n"
                    f"👇 <b>Оцените продавца:</b>"
                )
                
                seller_text = (
                    f"💰 <b>ПОЛУЧЕНА ОПЛАТА ЗА СДЕЛКУ!</b> 💰\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📦 <b>Информация о сделке:</b>\n"
                    f"┣ 🔢 Код: <code>{code}</code>\n"
                    f"┣ 📝 Название: {name}\n"
                    f"┣ 💰 Сумма: {deal['amount']:.2f} ₽\n"
                    f"┗ 👤 Покупатель: {buyer_username}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💚 <b>Средства зачислены на ваш баланс!</b>\n"
                    f"💰 Текущий баланс: {seller_balance:.2f} ₽"
                )
                
                await app.send_message(deal["buyer_id"], buyer_text, parse_mode=ParseMode.HTML, reply_markup=rating_kb)
                await app.send_message(deal["seller_id"], seller_text, parse_mode=ParseMode.HTML)
                
            except Exception as e:
                print(f"❌ Ошибка при подтверждении: {e}")
                await call.message.reply("❌ Произошла ошибка при подтверждении")
            finally:
                app.processing["confirm"].discard(code)
            
            return

        # ===== ОЦЕНКА СДЕЛКИ =====
        if data.startswith("rate_"):
            parts = data.split("_")
            code = parts[1]
            rating = int(parts[2])
            key = f"{code}_{rating}"
            
            await call.answer("⭐ Сохраняю оценку...", show_alert=False)
            
            # Защита от двойного нажатия
            if key in app.processing["rate"]:
                return
            app.processing["rate"].add(key)
            
            try:
                async with aiosqlite.connect(DB) as db:
                    cur = await db.execute("SELECT seller_id, buyer_id, name FROM deals WHERE code=?", (code,))
                    deal = await cur.fetchone()
                
                if not deal:
                    await call.answer("❌ Сделка не найдена", show_alert=True)
                    return
                    
                seller_id, buyer_id, name = deal
                
                if buyer_id != uid:
                    await call.answer("❌ Только покупатель может оценить", show_alert=True)
                    return
                
                await add_rating(seller_id, uid, code, rating)
                stats = await get_seller_stats(seller_id)
                
                await call.message.edit(
                    f"⭐ <b>СПАСИБО ЗА ОЦЕНКУ!</b>\n\n"
                    f"Вы поставили {rating}⭐\n\n"
                    f"📊 Рейтинг продавца: {stats['rating']} ({stats['total_votes']} оценок)",
                    parse_mode=ParseMode.HTML
                )
                await call.answer("✅ Оценка сохранена", show_alert=False)
                
            except Exception as e:
                if "UNIQUE constraint failed" in str(e):
                    await call.answer("❌ Вы уже оценили эту сделку", show_alert=True)
                else:
                    print(f"Ошибка: {e}")
                    await call.answer("❌ Ошибка", show_alert=True)
            finally:
                app.processing["rate"].discard(key)
            
            return

        # ===== ОТКРЫТЬ СПОР =====
        if data.startswith("dispute_"):
            code = data.split("_")[1]
            
            # Защита от двойного нажатия
            if code in app.processing["dispute"]:
                return
            app.processing["dispute"].add(code)
            
            try:
                user_states[uid] = {"step": "dispute_msg", "deal_code": code}
                await call.message.reply(
                    "⚖ <b>СПОР</b>\n\nОпишите проблему:",
                    parse_mode=ParseMode.HTML
                )
                await call.answer()
            finally:
                app.processing["dispute"].discard(code)
            
            return

        # ===== РЕШЕНИЕ СПОРА АДМИНОМ =====
        if data.startswith("resolve_"):
            if uid != ADMIN_ID:
                await call.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            parts = data.split("_")
            code = parts[1]
            choice = parts[2]
            key = f"{code}_{choice}"
            
            await call.answer("⚖ Обрабатываю решение...", show_alert=False)
            
            # Защита от двойного нажатия
            if key in app.processing["resolve"]:
                return
            app.processing["resolve"].add(key)
            
            try:
                async with aiosqlite.connect(DB) as db:
                    cur = await db.execute("SELECT buyer_id, seller_id, amount, name, status FROM deals WHERE code=?", (code,))
                    deal = await cur.fetchone()
                
                if not deal:
                    await call.answer("❌ Сделка не найдена", show_alert=True)
                    return
                    
                buyer_id, seller_id, amount, name, status = deal
                
                if status != "dispute":
                    await call.answer("❌ Спор уже решён", show_alert=True)
                    return
                
                if code in escrow:
                    amount = escrow.pop(code)["amount"]
                
                if choice == "buyer":
                    await change_balance(buyer_id, amount)
                    await app.send_message(buyer_id, f"💰 Спор решен в вашу пользу! +{amount:.2f} ₽")
                    await app.send_message(seller_id, f"❌ Спор проигран")
                else:
                    await change_balance(seller_id, amount)
                    await update_seller_deal_stats(seller_id, amount)
                    await app.send_message(seller_id, f"💰 Спор решен в вашу пользу! +{amount:.2f} ₽")
                    await app.send_message(buyer_id, f"❌ Спор проигран")
                
                async with aiosqlite.connect(DB) as db:
                    await db.execute("UPDATE deals SET status='closed' WHERE code=?", (code,))
                    await db.commit()
                
                await call.message.edit_text("✅ Спор решен")
                await call.answer("✅ Готово", show_alert=False)
                
            except Exception as e:
                print(f"Ошибка при решении спора: {e}")
                await call.answer("❌ Ошибка", show_alert=True)
            finally:
                app.processing["resolve"].discard(key)
            
            return

        # ===== ПОДТВЕРЖДЕНИЕ РАССЫЛКИ =====
        if data == "confirm_broadcast":
            if uid != ADMIN_ID:
                await call.answer("❌ Доступ запрещен", show_alert=True)
                return
            
            if uid not in user_states or user_states[uid].get("step") != "broadcast":
                await call.message.edit("❌ Ошибка: рассылка не найдена")
                return
            
            broadcast_text = user_states[uid].get("broadcast_text", "")
            users = await get_all_users()
            
            await call.message.edit(
                f"📢 <b>РАССЫЛКА НАЧАТА</b>\n\n"
                f"Всего пользователей: {len(users)}\n"
                f"Отправка... 0/{len(users)}",
                parse_mode=ParseMode.HTML
            )
            
            sent = 0
            failed = 0
            
            for i, user_id in enumerate(users):
                try:
                    await app.send_message(user_id, broadcast_text, parse_mode=ParseMode.HTML)
                    sent += 1
                except Exception as e:
                    failed += 1
                    print(f"Ошибка отправки пользователю {user_id}: {e}")
                
                if (i + 1) % 10 == 0:
                    await call.message.edit(
                        f"📢 <b>РАССЫЛКА НАЧАТА</b>\n\n"
                        f"Всего пользователей: {len(users)}\n"
                        f"Отправка... {i + 1}/{len(users)}",
                        parse_mode=ParseMode.HTML
                    )
            
            await call.message.edit(
                f"📢 <b>РАССЫЛКА ЗАВЕРШЕНА</b>\n\n"
                f"✅ Успешно: {sent}\n"
                f"❌ Ошибок: {failed}",
                parse_mode=ParseMode.HTML
            )
            
            if uid in user_states:
                del user_states[uid]
            
            user_states[uid] = {"step": "admin_menu"}
            await app.send_message(uid, "🛠 Админ-панель", reply_markup=admin_keyboard)
            
            await call.answer()
            return
        
        if data == "cancel_broadcast":
            if uid in user_states:
                del user_states[uid]
            await call.message.edit("❌ Рассылка отменена")
            
            user_states[uid] = {"step": "admin_menu"}
            await app.send_message(uid, "🛠 Админ-панель", reply_markup=admin_keyboard)
            await call.answer()
            return

    except Exception as e:
        print(f"❌ Ошибка в callback: {e}")
        try:
            await call.answer("❌ Произошла ошибка", show_alert=True)
        except:
            pass

# ================= JOIN DEAL =================
async def join_deal(message, code):
    try:
        async with aiosqlite.connect(DB) as db:
            cur = await db.execute("SELECT seller_id, name, description, amount, status, buyer_id FROM deals WHERE code=?", (code,))
            deal = await cur.fetchone()
        if not deal:
            await message.reply("❌ Сделка не найдена")
            return
        seller_id, name, desc, amount, status, buyer_id = deal
        if status != "open":
            await message.reply("❌ Сделка недоступна")
            return
            
        if buyer_id is not None:
            await message.reply("❌ Уже куплено")
            return

        buyer = message.from_user
        username = f"@{buyer.username}" if buyer.username else "без username"

        async with aiosqlite.connect(DB) as db:
            await db.execute("UPDATE deals SET buyer_id=? WHERE code=?", (buyer.id, code))
            await db.commit()

        await app.send_message(
            seller_id,
            f"🔔 <b>Покупатель найден!</b>\n\n"
            f"📦 {name}\n"
            f"💰 {amount:.2f} ₽\n"
            f"👤 {username}\n"
            f"🆔 <code>{buyer.id}</code>",
            parse_mode=ParseMode.HTML
        )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Оплатить", callback_data=f"pay_{code}")]
        ])
        
        await message.reply(
            f"📦 {name}\n📄 {desc}\n💰 {amount:.2f} ₽\n\nНажмите кнопку для оплаты",
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
    current_time = time.time()
    expired = []
    
    for label, payment in app.yoomoney_payments.items():
        if current_time - payment.get("timestamp", 0) > 3600:
            expired.append(label)
    
    for label in expired:
        del app.yoomoney_payments[label]
    
    return len(expired)

# ================= RUN =================
if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(init_db())
    print("="*60)
    print("🚀 Бот VAULTOR запущен")
    print("="*60)
    print("✅ ЮMoney платежи: 100% ГАРАНТИЯ ЗАЧИСЛЕНИЯ")
    print("✅ Защита от двойного нажатия: АКТИВНА")
    print("="*60)
    
    app.run()
