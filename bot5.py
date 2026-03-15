import os
import random
import asyncio
import aiosqlite
import aiohttp
import time
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

# ===== SECURITY HELPERS =====
def ensure_deal_active(deal):
    if deal.get("status") == "closed":
        raise Exception("Deal already closed")

def close_deal(deal):
    deal["status"] = "closed"

# ================= DATABASE =================
async def init_db():
    async with aiosqlite.connect(DB) as db:
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
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT id FROM users")
        rows = await cur.fetchall()
        return [row[0] for row in rows]

async def get_balance(uid):
    async with aiosqlite.connect(DB) as db:
        # Проверяем, существует ли пользователь
        cur = await db.execute("SELECT balance FROM users WHERE id=?", (uid,))
        row = await cur.fetchone()
        
        if not row:
            # Если пользователя нет, создаем его с текущей датой
            await db.execute(
                "INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (uid, 0, 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
            )
            await db.commit()
            return 0
        return row[0]

async def change_balance(uid, amount):
    async with aiosqlite.connect(DB) as db:
        # Проверяем, существует ли пользователь
        cur = await db.execute("SELECT id FROM users WHERE id=?", (uid,))
        row = await cur.fetchone()
        
        if not row:
            # Если пользователя нет, создаем его
            await db.execute(
                "INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (uid, amount if amount > 0 else 0, 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
            )
        else:
            # Обновляем баланс существующего пользователя
            await db.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amount, uid))
        await db.commit()

async def set_block(uid, blocked=True):
    async with aiosqlite.connect(DB) as db:
        # Проверяем, существует ли пользователь
        cur = await db.execute("SELECT id FROM users WHERE id=?", (uid,))
        row = await cur.fetchone()
        
        if not row:
            # Если пользователя нет, создаем его
            await db.execute(
                "INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (uid, 0, 1 if blocked else 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
            )
        else:
            # Обновляем статус блокировки
            await db.execute("UPDATE users SET blocked=? WHERE id=?", (1 if blocked else 0, uid))
        await db.commit()

async def is_blocked(uid):
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT blocked FROM users WHERE id=?", (uid,))
        row = await cur.fetchone()
        if not row:
            return False
        return row[0] == 1

async def get_seller_stats(seller_id):
    async with aiosqlite.connect(DB) as db:
        # Сначала проверяем существование пользователя
        cur = await db.execute("SELECT id FROM users WHERE id=?", (seller_id,))
        user_exists = await cur.fetchone()
        
        if not user_exists:
            # Если пользователя нет, создаем его
            await db.execute(
                "INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (seller_id, 0, 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
            )
            await db.commit()
        
        # Получаем статистику
        cur = await db.execute("""
            SELECT total_stars, total_votes, total_deals, total_turnover, blocked, registered_date 
            FROM users WHERE id=?
        """, (seller_id,))
        row = await cur.fetchone()
        
        if not row:
            return None
            
        total_stars, total_votes, total_deals, total_turnover, blocked, registered_date = row
        
        # Получаем распределение оценок
        cur = await db.execute("""
            SELECT rating, COUNT(*) as count 
            FROM ratings 
            WHERE seller_id=? 
            GROUP BY rating 
            ORDER BY rating DESC
        """, (seller_id,))
        rating_dist = await cur.fetchall()
        
        # Создаем словарь с распределением
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
        # Добавляем оценку
        await db.execute("""
            INSERT INTO ratings (seller_id, buyer_id, deal_code, rating) 
            VALUES (?, ?, ?, ?)
        """, (seller_id, buyer_id, deal_code, rating_value))
        
        # Обновляем статистику продавца
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
    """Получает полный профиль пользователя"""
    async with aiosqlite.connect(DB) as db:
        # Получаем основную информацию о пользователе
        cur = await db.execute("""
            SELECT balance, total_deals, total_turnover, registered_date, blocked
            FROM users WHERE id=?
        """, (uid,))
        user_data = await cur.fetchone()
        
        if not user_data:
            # Если пользователя нет, создаем
            await get_balance(uid)
            return await get_user_profile(uid)
        
        balance, total_deals, total_turnover, registered_date, blocked = user_data
        
        # Получаем информацию из Telegram
        try:
            user = await app.get_users(uid)
            username = f"@{user.username}" if user.username else "нет"
            first_name = user.first_name or ""
            last_name = user.last_name or ""
            full_name = f"{first_name} {last_name}".strip()
        except:
            username = "недоступен"
            full_name = "неизвестно"
        
        # Получаем количество сделок, где пользователь был продавцом
        cur = await db.execute("""
            SELECT COUNT(*) FROM deals 
            WHERE seller_id=? AND status='completed'
        """, (uid,))
        seller_deals = (await cur.fetchone())[0]
        
        # Получаем количество сделок, где пользователь был покупателем
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
        
        # Инициализируем пользователя в базе данных
        await get_balance(uid)
        
        kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
        
        # Новый красочный текст приветствия
        welcome_text = (
            "🔥 <b>Добро пожаловать в VAULTOR — современный escrow-сервис нового уровня.</b>\n\n"
            "Здесь каждая сделка проходит под надёжной защитой: средства резервируются и переводятся только после выполнения обязательств 🛡🤝\n"
            "Полная прозрачность процесса и контроль на каждом этапе обеспечивают безопасность для обеих сторон.\n\n"
            "💎 <b>И главное — VAULTOR не взимает комиссию за сделки.</b>\n"
            "Вы получаете защиту и гарантию расчётов без дополнительных расходов.\n\n"
            "🚀 Запустите сделку и убедитесь, что безопасные финансовые операции могут быть простыми и выгодными.\n\n"
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
            
            # Получаем информацию о сделке
            async with aiosqlite.connect(DB) as db:
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
            
            # Определяем, кто открыл спор
            if uid == seller_id:
                disputer = "ПРОДАВЕЦ"
                disputer_emoji = "👤"
            else:
                disputer = "ПОКУПАТЕЛЬ"
                disputer_emoji = "👥"
            
            # Получаем информацию о пользователях
            try:
                buyer_user = await app.get_users(buyer_id)
                seller_user = await app.get_users(seller_id)
                buyer_username = f"@{buyer_user.username}" if buyer_user.username else "без username"
                seller_username = f"@{seller_user.username}" if seller_user.username else "без username"
            except:
                buyer_username = "без username"
                seller_username = "без username"
            
            # Кнопки для админа
            kb_dispute = InlineKeyboardMarkup([
                [InlineKeyboardButton("👤 Отдать покупателю", callback_data=f"resolve_{code}_buyer")],
                [InlineKeyboardButton("👥 Отдать продавцу", callback_data=f"resolve_{code}_seller")]
            ])
            
            # Отправляем админу с уточнением отправителя
            await app.send_message(
                ADMIN_ID,
                f"⚖ <b>ОТКРЫТ СПОР</b>\n\n"
                f"{disputer_emoji} <b>Инициатор:</b> {disputer}\n"
                f"👤 Пользователь: @{message.from_user.username or 'нет username'} | 🆔 <code>{uid}</code>\n\n"
                f"🔢 Код сделки: <code>{code}</code>\n"
                f"📦 Название: {name}\n"
                f"💰 Сумма: {amount} ₽\n\n"
                f"👤 Покупатель: {buyer_username} | 🆔 <code>{buyer_id}</code>\n"
                f"👤 Продавец: {seller_username} | 🆔 <code>{seller_id}</code>\n\n"
                f"💬 <b>Сообщение от {disputer.lower()}:</b>\n{dispute_text}",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_dispute
            )
            
            # Уведомляем пользователя
            await message.reply(
                "✅ Ваше сообщение отправлено администратору. Ожидайте решения спора.\n"
                "Администратор свяжется с вами при необходимости."
            )
            
            # Очищаем состояние
            del user_states[uid]
            return

        # ======== Пополнение через ЮMoney ========
        if text == "💳 Пополнить":
            user_states[uid] = {"step": "yoomoney_amount"}
            await message.reply(
                "💰 <b>ПОПОЛНЕНИЕ БАЛАНСА через ЮMoney</b> 💰\n\n"
                "Введите сумму пополнения (от 50 до 10000 рублей):\n\n"
                "💡 <i>Только цифры, без пробелов и букв</i>",
                parse_mode=ParseMode.HTML
            )
            return

        # ======== Баланс ========
        if text == "💰 Баланс":
            bal = await get_balance(uid)
            kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
            await message.reply(
                f"💳 Ваш баланс: {bal} ₽\n\n"
                "🔒 Средства замораживаются при оплате сделки.",
                reply_markup=kb
            )
            return

        # ======== ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ ========
        if text == "👤 Профиль":
            profile = await get_user_profile(uid)
            
            # Определяем статус
            if profile["blocked"]:
                status = "❌ ЗАБЛОКИРОВАН"
            else:
                status = "✅ Активен"
            
            # Формируем сообщение
            profile_text = (
                f"👤 <b>ВАШ ПРОФИЛЬ</b>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🆔 <b>ID:</b> <code>{profile['user_id']}</code>\n"
                f"👤 <b>Имя:</b> {profile['full_name']}\n"
                f"📛 <b>Username:</b> {profile['username']}\n"
                f"📅 <b>Дата регистрации:</b> {profile['registered_date']}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 <b>Баланс:</b> {profile['balance']} ₽\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <b>СТАТИСТИКА СДЕЛОК</b>\n"
                f"┣ ✅ <b>Как продавец:</b> {profile['seller_deals']}\n"
                f"┣ ✅ <b>Как покупатель:</b> {profile['buyer_deals']}\n"
                f"┗ 💰 <b>Общий оборот:</b> {profile['total_turnover']} ₽\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🔰 <b>Статус:</b> {status}"
            )
            
            kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
            await message.reply(profile_text, parse_mode=ParseMode.HTML, reply_markup=kb)
            return

        # ======== Поиск продавца ========
        if text == "🔍 Поиск продавца":
            user_states[uid] = {"step": "search_seller"}
            await message.reply(
                "🔍 <b>ПОИСК ПРОДАВЦА НА ПЛАТФОРМЕ</b>\n\n"
                "Введите ID или @username интересующего вас продавца\n\n"
                "📌 <b>Примеры:</b>\n"
                "   • @ivan_shop\n"
                "   • 123456789\n\n"
                "🌟 <b>Что вы получите:</b>\n"
                "   • 📊 Полную статистику продавца\n"
                "   • ⭐ Рейтинг и отзывы\n"
                "   • 💰 Оборот и количество сделок\n"
                "   • 🏆 Уровень надежности\n\n"
                "💡 <i>Введите данные для поиска...</i>",
                parse_mode=ParseMode.HTML
            )
            return

        # ======== Создать сделку ========
        if text.startswith("📝"):
            user_states[uid] = {"step": "name", "data": {}}
            await message.reply("✨ Введите название сделки (например: Продажа товара).")
            return

        # ======== Админ-панель ========
        if uid == ADMIN_ID and text == "⚙ Админ":
            user_states[uid] = {"step": "admin_menu"}
            await message.reply("🛠 Админ-панель:", reply_markup=admin_keyboard)
            return

        # ======== Обработка поиска продавца ========
        if uid in user_states and user_states[uid].get("step") == "search_seller":
            seller_identifier = text.strip()
            
            # Получаем ID продавца
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
            
            # Получаем статистику продавца
            stats = await get_seller_stats(seller_id)
            
            # Получаем информацию о пользователе из Telegram
            try:
                seller_user = await app.get_users(seller_id)
                seller_username = f"@{seller_user.username}" if seller_user.username else "нет username"
                seller_name = seller_user.first_name or ""
                if seller_user.last_name:
                    seller_name += f" {seller_user.last_name}"
            except:
                seller_username = "нет username"
                seller_name = "неизвестно"
            
            # Определяем уровень надежности
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
            
            # Формируем сообщение с профилем продавца
            profile_text = (
                f"👤 <b>ПРОФИЛЬ ПРОДАВЦА</b>\n\n"
                f"🆔 <b>ID:</b> <code>{seller_id}</code>\n"
                f"📛 <b>Username:</b> {seller_username}\n"
                f"👤 <b>Имя:</b> {seller_name}\n\n"
                f"⭐ <b>РЕЙТИНГ</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🏆 <b>Общий рейтинг:</b> {stats['rating']} / 5.0\n"
                f"📊 <b>На основе:</b> {stats['total_votes']} отзывов\n\n"
                f"⭐ <b>Распределение оценок:</b>\n"
                f"5⭐ — {stats['dist'][5]} отзывов ({round(stats['dist'][5]/max(stats['total_votes'],1)*100)}%)\n"
                f"4⭐ — {stats['dist'][4]} отзывов ({round(stats['dist'][4]/max(stats['total_votes'],1)*100)}%)\n"
                f"3⭐ — {stats['dist'][3]} отзывов ({round(stats['dist'][3]/max(stats['total_votes'],1)*100)}%)\n"
                f"2⭐ — {stats['dist'][2]} отзывов ({round(stats['dist'][2]/max(stats['total_votes'],1)*100)}%)\n"
                f"1⭐ — {stats['dist'][1]} отзывов ({round(stats['dist'][1]/max(stats['total_votes'],1)*100)}%)\n\n"
                f"📦 <b>СТАТИСТИКА СДЕЛОК</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ <b>Завершенных сделок:</b> {stats['total_deals']}\n"
                f"💰 <b>Оборот:</b> {stats['total_turnover']:,} ₽\n"
                f"📅 <b>На платформе:</b> с {stats['registered_date']}\n\n"
                f"🎯 <b>Надежность:</b> {reliability}"
            )
            
            if stats["blocked"]:
                profile_text += "\n\n⚠️ <b>Пользователь заблокирован и недоступен для сделок</b>"
            
            # Кнопка для связи с продавцом (только если есть username)
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
                await message.reply("📄 Введите описание сделки.")
                return
            if state.get("step") == "desc":
                state["data"]["desc"] = text
                state["step"] = "amount"
                await message.reply("💰 Введите сумму сделки (числом).")
                return
            if state.get("step") == "amount":
                if not text.isdigit():
                    await message.reply("⚠ Введите корректное число.")
                    return
                amount = int(text)
                code = str(random.randint(10000000, 99999999))
                async with aiosqlite.connect(DB) as db:
                    await db.execute(
                        "INSERT INTO deals(code, seller_id, buyer_id, name, description, amount, status) VALUES(?,?,?,?,?,?,?)",
                        (code, uid, None, state["data"]["name"], state["data"]["desc"], amount, "open")
                    )
                    await db.commit()
                bot_username = (await client.get_me()).username
                link = f"https://t.me/{bot_username}?start=deal_{code}"
                
                # НОВЫЙ ТЕКСТ С МОНОШИРНОЙ ССЫЛКОЙ И КНОПКОЙ
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
                    f"5️⃣ Получите оплату после подтверждения ✅"
                )
                
                # Инлайн-кнопка для отправки ссылки
                share_kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📤 Отправить ссылку покупателю", switch_inline_query=f"🛡 Escrow сделка\n\n🔑 Код сделки: {code}\n💰 Сумма: {amount} ₽\n\nДля участия откройте бота и введите код сделки.\nСсылка для подключения: {link}")]
                ])
                
                await message.reply(success_text, parse_mode=ParseMode.HTML, reply_markup=share_kb)
                del user_states[uid]
                return

            # ======== Ввод суммы для пополнения через ЮMoney ========
            if state.get("step") == "yoomoney_amount":
                try:
                    amount = float(text.strip())
                    
                    # Проверка минимальной и максимальной суммы
                    if amount < 2  or amount > 10000:
                        await message.reply(
                            f"❌ <b>Неверная сумма</b>\n\n"
                            f"Сумма должна быть от <b>50</b> до <b>10000</b> рублей.\n"
                            f"Введите корректную сумму:",
                            parse_mode=ParseMode.HTML
                        )
                        return
                    
                    # Округляем до 2 знаков
                    amount = round(amount, 2)
                    
                    # Создаем метку для платежа
                    label = f"user_{uid}_{int(time.time())}"
                    
                    # Формируем ссылку на оплату
                    params = {
                        "receiver": YOOMONEY_WALLET,
                        "quickpay-form": "shop",
                        "targets": "Пополнение баланса в VAULTOR",
                        "paymentType": "SB",  # SB - Сбербанк (самый популярный)
                        "sum": amount,
                        "label": label,
                        "successURL": "https://t.me/your_bot"  # Замените на юзернейм вашего бота
                    }
                    
                    payment_url = "https://yoomoney.ru/quickpay/confirm.xml?" + urlencode(params)
                    
                    # Сохраняем информацию о платеже
                    app.yoomoney_payments[label] = {
                        "user_id": uid,
                        "amount": amount,
                        "timestamp": time.time(),
                        "checked": False
                    }
                    
                    # Кнопка для проверки оплаты (без кнопки отмены)
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
                        f"✅ <b>Деньги зачисляются автоматически</b> в течение 1-2 минут\n"
                        f"💰 На баланс поступит ровно <b>{amount} ₽</b>\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━━",
                        parse_mode=ParseMode.HTML,
                        reply_markup=check_kb,
                        disable_web_page_preview=True
                    )
                    
                    # Очищаем состояние
                    del user_states[uid]
                    
                except ValueError:
                    await message.reply(
                        "❌ <b>Ошибка ввода</b>\n\n"
                        "Пожалуйста, введите число (только цифры).\n"
                        "Пример: <code>500</code> или <code>1500.50</code>",
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
                # Обработка кнопки "📢 Сообщение"
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
                
                # Подтверждение отправки
                msg_text = text
                state["broadcast_text"] = msg_text
                
                # Получаем количество пользователей
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
                    # сообщение пользователю о пополнении
                    try:
                        await app.send_message(
                            target,
                            f"💰 Ваш баланс был пополнен на {amount} ₽ администратором.\n"
                            "💳 Теперь вы можете использовать средства для оплаты сделок."
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
        # ===== ИСПРАВЛЕННАЯ ПРОВЕРКА ПЛАТЕЖЕЙ =====
        if data.startswith("check_payment_"):
            label = data.replace("check_payment_", "")
            
            print(f"\n🔍 Проверка платежа: {label}")
            
            # Проверяем существование платежа
            if label not in app.yoomoney_payments:
                await call.answer("❌ Платеж не найден в памяти бота", show_alert=True)
                return
            
            payment = app.yoomoney_payments[label]
            print(f"💰 Ожидание: {payment['amount']} ₽ от user {payment['user_id']}")
            
            # Проверяем, не проверяли ли уже
            if payment.get("checked"):
                await call.answer("✅ Платеж уже был зачислен!", show_alert=True)
                return
            
            await call.answer("🔄 Проверяю...", show_alert=False)
            
            try:
                # Получаем последние 20 операций
                url = "https://yoomoney.ru/api/operation-history"
                headers = {"Authorization": f"Bearer {YOOMONEY_TOKEN}"}
                params = {"records": 20}
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, data=params) as resp:
                        print(f"📊 API статус: {resp.status}")
                        
                        if resp.status == 200:
                            data = await resp.json()
                            operations = data.get('operations', [])
                            print(f"📋 Получено операций: {len(operations)}")
                            
                            # Ищем нашу метку
                            found = False
                            for op in operations:
                                op_label = op.get('label')
                                op_status = op.get('status')
                                op_amount = float(op.get('amount', 0))
                                
                                print(f"  Операция: label={op_label}, status={op_status}, amount={op_amount}")
                                
                                if op_label == label and op_status == "success":
                                    print(f"✅ НАШЛИ! Сумма: {op_amount}")
                                    
                                    # Проверяем сумму (с погрешностью)
                                    if abs(op_amount - payment['amount']) < 0.01:
                                        # Зачисляем
                                        await change_balance(payment['user_id'], payment['amount'])
                                        payment['checked'] = True
                                        
                                        new_balance = await get_balance(payment['user_id'])
                                        
                                        await call.message.edit_text(
                                            f"✅ <b>ПЛАТЕЖ ПОДТВЕРЖДЕН!</b>\n\n"
                                            f"💰 Сумма: {payment['amount']} ₽\n"
                                            f"💳 Новый баланс: {new_balance} ₽",
                                            parse_mode=ParseMode.HTML
                                        )
                                        await call.answer("✅ Успешно зачислено!", show_alert=True)
                                        found = True
                                        break
                                    else:
                                        print(f"⚠️ Сумма не совпадает: ожидалось {payment['amount']}, получено {op_amount}")
                            
                            if not found:
                                # Показываем пользователю последние операции для понимания
                                ops_text = "\n".join([f"  • {o.get('amount')}₽ - {o.get('status')}" for o in operations[:3]])
                                await call.message.edit_text(
                                    f"❌ <b>Платеж не найден</b>\n\n"
                                    f"Метка: <code>{label}</code>\n\n"
                                    f"📋 Последние операции в ЮMoney:\n{ops_text}\n\n"
                                    f"⏳ Попробуйте через минуту или обратитесь к администратору.",
                                    parse_mode=ParseMode.HTML
                                )
                                await call.answer("❌ Платеж не найден", show_alert=True)
                        else:
                            error_text = await resp.text()
                            await call.message.edit_text(
                                f"❌ <b>Ошибка API ЮMoney</b>\n\n"
                                f"Код: {resp.status}\n"
                                f"Попробуйте позже.",
                                parse_mode=ParseMode.HTML
                            )
                            print(f"❌ API ошибка: {resp.status} - {error_text}")
                            
            except Exception as e:
                print(f"❌ Исключение: {e}")
                await call.message.edit_text(
                    f"❌ <b>Ошибка при проверке</b>\n\n"
                    f"{str(e)[:100]}",
                    parse_mode=ParseMode.HTML
                )
            
            return

        # ===== Подтверждение рассылки =====
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
                
                # Обновляем статус каждые 10 сообщений
                if (i + 1) % 10 == 0:
                    await call.message.edit(
                        f"📢 <b>РАССЫЛКА НАЧАТА</b>\n\n"
                        f"Всего пользователей: {len(users)}\n"
                        f"Отправка... {i + 1}/{len(users)}",
                        parse_mode=ParseMode.HTML
                    )
            
            await call.message.edit(
                f"📢 <b>РАССЫЛКА ЗАВЕРШЕНА</b>\n\n"
                f"✅ Успешно отправлено: {sent}\n"
                f"❌ Не удалось отправить: {failed}\n"
                f"📊 Всего пользователей: {len(users)}",
                parse_mode=ParseMode.HTML
            )
            
            # Очищаем состояние
            if uid in user_states:
                del user_states[uid]
            
            # Устанавливаем состояние admin_menu и отправляем клавиатуру
            user_states[uid] = {"step": "admin_menu"}
            
            # Отправляем новое сообщение с админ-клавиатурой
            await app.send_message(
                uid,
                "🛠 Админ-панель",
                reply_markup=admin_keyboard
            )
            
            await call.answer()
            return
        
        if data == "cancel_broadcast":
            if uid in user_states:
                del user_states[uid]
            await call.message.edit("❌ Рассылка отменена")
            
            # Устанавливаем состояние admin_menu и возвращаем админ-клавиатуру
            user_states[uid] = {"step": "admin_menu"}
            await app.send_message(
                uid,
                "🛠 Админ-панель",
                reply_markup=admin_keyboard
            )
            await call.answer()
            return

        # ===== Оценка сделки =====
        if data.startswith("rate_"):
            parts = data.split("_")
            code = parts[1]
            rating = int(parts[2])
            
            async with aiosqlite.connect(DB) as db:
                cur = await db.execute("SELECT seller_id, buyer_id, name FROM deals WHERE code=?", (code,))
                deal = await cur.fetchone()
            
            if not deal:
                await call.answer("❌ Сделка не найдена", show_alert=True)
                return
                
            seller_id, buyer_id, name = deal
            
            # Проверяем, что оценивает именно покупатель
            if buyer_id != uid:
                await call.answer("❌ Только покупатель может оценить сделку", show_alert=True)
                return
            
            try:
                await add_rating(seller_id, uid, code, rating)
                
                # Получаем обновленную статистику
                stats = await get_seller_stats(seller_id)
                
                await call.message.edit(
                    f"⭐ <b>СПАСИБО ЗА ОЦЕНКУ!</b> ⭐\n\n"
                    f"Вы поставили продавцу {rating}⭐\n\n"
                    f"📊 <b>Текущий рейтинг продавца:</b> {stats['rating']} ({stats['total_votes']} оценок)",
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
            async with aiosqlite.connect(DB) as db:
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
            
            # Проверяем, что покупатель тот, кто нажимает кнопку
            if buyer_id is not None and buyer_id != uid:
                await call.answer("❌ Эта сделка предназначена для другого покупателя", show_alert=True)
                return
                
            # Обновляем buyer_id в сделке
            async with aiosqlite.connect(DB) as db:
                await db.execute("UPDATE deals SET buyer_id=? WHERE code=?", (uid, code))
                await db.commit()
            
            # списываем деньги с покупателя
            await change_balance(uid, -amount)
            
            # Сохраняем в escrow для подтверждения
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

            # сообщение покупателю
            await app.send_message(
                uid,
                f"💳 Вы успешно оплатили сделку!\n\n"
                f"📦 Название: {name}\n"
                f"💰 Сумма: {amount} ₽\n"
                f"🔢 Код сделки: {code}\n"
                f"🔗 Ссылка: {deal_link}\n"
                f"👤 Продавец: {seller_username} | 🆔 {seller_id}\n\n"
                "🔒 Деньги заморожены на эскроу.\n"
                "⚠ Не подтверждайте сделку до того, как продавец выполнит свои обязательства.\n"
                "Если возникнут проблемы — откройте спор ниже.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb_deal
            )

            # сообщение продавцу
            await app.send_message(
                seller_id,
                f"🔔 Покупатель оплатил вашу сделку!\n\n"
                f"📦 Название: {name}\n"
                f"💰 Сумма: {amount} ₽\n"
                f"🔢 Код сделки: {code}\n"
                f"🔗 Ссылка: {deal_link}\n"
                f"👤 Покупатель: {buyer_username} | 🆔 {uid}\n\n"
                "⚖ Вы можете открыть спор, если возникнут проблемы, или ждать подтверждения выполнения сделки.",
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
            
            # Переводим деньги продавцу
            await change_balance(deal["seller_id"], deal["amount"])
            
            # Обновляем статистику продавца
            await update_seller_deal_stats(deal["seller_id"], deal["amount"])
            
            # Получаем информацию о сделке из базы
            async with aiosqlite.connect(DB) as db:
                cur = await db.execute("SELECT name FROM deals WHERE code=?", (code,))
                row = await cur.fetchone()
                name = row[0] if row else "Сделка"
                await db.execute("UPDATE deals SET status='completed' WHERE code=?", (code,))
                await db.commit()
            
            # Получаем username'ы
            try:
                buyer_user = await app.get_users(deal["buyer_id"])
                seller_user = await app.get_users(deal["seller_id"])
                buyer_username = f"@{buyer_user.username}" if buyer_user.username else "нет username"
                seller_username = f"@{seller_user.username}" if seller_user.username else "нет username"
            except:
                buyer_username = "нет username"
                seller_username = "нет username"
            
            # Получаем баланс продавца для отображения
            seller_balance = await get_balance(deal["seller_id"])
            
            # Кнопки для оценки
            rating_kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("1⭐", callback_data=f"rate_{code}_1"),
                    InlineKeyboardButton("2⭐", callback_data=f"rate_{code}_2"),
                    InlineKeyboardButton("3⭐", callback_data=f"rate_{code}_3"),
                    InlineKeyboardButton("4⭐", callback_data=f"rate_{code}_4"),
                    InlineKeyboardButton("5⭐", callback_data=f"rate_{code}_5")
                ]
            ])
            
            # Сообщение для покупателя
            buyer_text = (
                f"✅ <b>СДЕЛКА УСПЕШНО ЗАВЕРШЕНА!</b> ✅\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📦 <b>Детали сделки:</b>\n"
                f"┣ 🔢 Код: <code>{code}</code>\n"
                f"┣ 📝 Название: {name}\n"
                f"┣ 💰 Сумма: {deal['amount']:,} ₽\n"
                f"┗ 👤 Продавец: {seller_username}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"💫 <b>Статус:</b> ✓ Деньги переведены продавцу\n\n"
                f"🙏 Спасибо за использование нашего сервиса!\n\n"
                f"👇 <b>Оцените продавца:</b>"
            )
            
            # Сообщение для продавца
            seller_text = (
                f"💰 <b>ПОЛУЧЕНА ОПЛАТА ЗА СДЕЛКУ!</b> 💰\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"📦 <b>Информация о сделке:</b>\n"
                f"┣ 🔢 Код: <code>{code}</code>\n"
                f"┣ 📝 Название: {name}\n"
                f"┣ 💰 Сумма: {deal['amount']:,} ₽\n"
                f"┗ 👤 Покупатель: {buyer_username}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"💚 <b>Средства зачислены на ваш баланс!</b>\n"
                f"💰 Текущий баланс: {seller_balance:,} ₽\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ Сделка успешно завершена!"
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
                "⚖ <b>Открыт спор!</b>\n\n"
                "Опишите проблему в этом чате сообщением. Будьте максимально конкретны и честны.\n"
                "💬 Ваше сообщение будет отправлено администратору для решения спора.",
                parse_mode=ParseMode.HTML
            )
            await call.answer()
            return

        # ===== Решение спора админом =====
        if data.startswith("resolve_"):
            parts = data.split("_")
            code = parts[1]
            choice = parts[2]  # buyer или seller
            
            # Получаем информацию о сделке из базы данных
            async with aiosqlite.connect(DB) as db:
                cur = await db.execute("SELECT buyer_id, seller_id, amount, name, status FROM deals WHERE code=?", (code,))
                deal = await cur.fetchone()
            
            if not deal:
                await call.answer("❌ Сделка не найдена", show_alert=True)
                return
                
            buyer_id, seller_id, amount, name, status = deal
            
            if status != "dispute":
                await call.answer("❌ Спор уже решён или не открыт", show_alert=True)
                return
            
            # Проверяем, есть ли информация в escrow
            if code in escrow:
                # Если есть в escrow, используем оттуда
                escrow_deal = escrow.pop(code)
                amount = escrow_deal["amount"]
            
            # Перевод денег в зависимости от решения
            if choice == "buyer":
                # Возвращаем деньги покупателю
                await change_balance(buyer_id, amount)
                
                # Отправляем уведомления
                await app.send_message(
                    buyer_id,
                    f"💰 <b>Спор решён в вашу пользу!</b>\n\n"
                    f"📦 Сделка: {name}\n"
                    f"🔢 Код: {code}\n"
                    f"💰 Сумма: {amount} ₽\n\n"
                    f"Средства возвращены на ваш баланс."
                )
                await app.send_message(
                    seller_id,
                    f"❌ <b>Спор решён в пользу покупателя</b>\n\n"
                    f"📦 Сделка: {name}\n"
                    f"🔢 Код: {code}\n"
                    f"💰 Сумма: {amount} ₽\n\n"
                    f"Деньги возвращены покупателю."
                )
            else:  # choice == "seller"
                # Переводим деньги продавцу
                await change_balance(seller_id, amount)
                await update_seller_deal_stats(seller_id, amount)
                
                # Отправляем уведомления
                await app.send_message(
                    seller_id,
                    f"💰 <b>Спор решён в вашу пользу!</b>\n\n"
                    f"📦 Сделка: {name}\n"
                    f"🔢 Код: {code}\n"
                    f"💰 Сумма: {amount} ₽\n\n"
                    f"Средства зачислены на ваш баланс."
                )
                await app.send_message(
                    buyer_id,
                    f"❌ <b>Спор решён в пользу продавца</b>\n\n"
                    f"📦 Сделка: {name}\n"
                    f"🔢 Код: {code}\n"
                    f"💰 Сумма: {amount} ₽\n\n"
                    f"Деньги переведены продавцу."
                )
            
            # Закрываем сделку
            async with aiosqlite.connect(DB) as db:
                await db.execute("UPDATE deals SET status='closed' WHERE code=?", (code,))
                await db.commit()
            
            # Удаляем из escrow если там ещё есть
            if code in escrow:
                escrow.pop(code)
                
            await call.message.edit("✅ Спор решён. Уведомления отправлены сторонам.")
            await call.answer("✅ Спор решён", show_alert=False)
            return

        await call.answer()
    except Exception as e:
        print(f"Ошибка в callback: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)

# ================= JOIN DEAL =================
async def join_deal(message, code):
    try:
        async with aiosqlite.connect(DB) as db:
            cur = await db.execute("SELECT seller_id, name, description, amount, status, buyer_id FROM deals WHERE code=?", (code,))
            deal = await cur.fetchone()
        if not deal:
            await message.reply("❌ Сделка не найдена.")
            return
        seller_id, name, desc, amount, status, buyer_id = deal
        if status != "open":
            await message.reply("❌ Сделка недоступна.")
            return
            
        # Если у сделки уже есть покупатель
        if buyer_id is not None:
            await message.reply("❌ К этой сделке уже подключился покупатель.")
            return

        buyer = message.from_user
        username = f"@{buyer.username}" if buyer.username else "без username"

        # Обновляем buyer_id в сделке
        async with aiosqlite.connect(DB) as db:
            await db.execute("UPDATE deals SET buyer_id=? WHERE code=?", (buyer.id, code))
            await db.commit()

        # уведомление продавцу
        await app.send_message(
            seller_id,
            f"🔔 <b>К вашей сделке подключился покупатель!</b>\n\n"
            f"📦 {name}\n"
            f"💰 {amount} ₽\n"
            f"🔢 Код: {code}\n\n"
            f"👤 Покупатель: {username}\n"
            f"🆔 <code>{buyer.id}</code>",
            parse_mode=ParseMode.HTML
        )

        # кнопка оплаты для покупателя
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Оплатить", callback_data=f"pay_{code}")]
        ])
        await message.reply(
            f"📦 {name}\n📄 {desc}\n💰 {amount} ₽\n\nНажмите кнопку ниже, чтобы оплатить сделку.",
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
    """Очищает старые неиспользованные платежи"""
    current_time = time.time()
    expired = []
    
    for label, payment in app.yoomoney_payments.items():
        # Удаляем платежи старше 1 часа
        if current_time - payment.get("timestamp", 0) > 3600:
            expired.append(label)
    
    for label in expired:
        del app.yoomoney_payments[label]
    
    return len(expired)

# ================= RUN =================
if __name__ == "__main__":
    # Удаляем старую базу данных для пересоздания (раскомментируйте если нужно)
    # if os.path.exists(DB):
    #     os.remove(DB)
    #     print("Старая база данных удалена")
    
    asyncio.get_event_loop().run_until_complete(init_db())
    print("🚀 Бот запущен")
    app.run()
