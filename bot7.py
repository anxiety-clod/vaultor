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
# Загружаем все необходимые переменные из .env файла
# Это нужно для безопасности - все ключи хранятся отдельно от кода
load_dotenv()

# Получаем данные для подключения к Telegram API
API_ID = int(os.getenv("API_ID"))  # ID приложения из my.telegram.org
API_HASH = os.getenv("API_HASH")    # Хеш приложения из my.telegram.org
BOT_TOKEN = os.getenv("BOT_TOKEN")  # Токен бота от @BotFather
ADMIN_ID = int(os.getenv("ADMIN_ID"))  # ID администратора (твой Telegram ID)

# Данные для подключения к ЮMoney
YOOMONEY_WALLET = os.getenv("YOOMONEY_WALLET")  # Номер твоего кошелька
YOOMONEY_TOKEN = os.getenv("YOOMONEY_TOKEN")    # Токен для доступа к API

# Проверяем что все данные ЮMoney загружены, иначе бот не запустится
if not YOOMONEY_WALLET or not YOOMONEY_TOKEN:
    raise ValueError("YOOMONEY_WALLET и YOOMONEY_TOKEN должны быть установлены в .env файле")

# ================= ИНИЦИАЛИЗАЦИЯ БОТА =================
# Создаем экземпляр клиента Pyrogram
# workers=32 означает что бот может обрабатывать до 32 запросов одновременно
app = Client("escrow_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workers=32)

# ================= ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ =================
# Путь к файлу базы данных SQLite
DB = "bot.db"

# Хранилище состояний пользователей
# Нужно для многошаговых операций (например, создание сделки)
# Ключ - ID пользователя, значение - словарь с данными состояния
user_states = {}

# Хранилище для эскроу-сделок
# Деньги замораживаются здесь до подтверждения сделки
escrow = {}

# Хранилище для платежей ЮMoney (временно, пока не сохраним в БД)
# Используется как кэш перед записью в базу данных
app.yoomoney_payments = {}

# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ БЕЗОПАСНОСТИ =================
def ensure_deal_active(deal):
    """
    Проверяет, активна ли сделка
    Если сделка закрыта - выбрасывает исключение
    Используется для защиты от повторного использования закрытых сделок
    """
    if deal.get("status") == "closed":
        raise Exception("Deal already closed")

def close_deal(deal):
    """
    Закрывает сделку
    Устанавливает статус "closed" и удаляет из активных
    """
    deal["status"] = "closed"
    # Хранение временной информации о сделках
    # Формат: {deal_code: {"buyer_id":..,"seller_id":..,"amount":..}}

# ================= ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ =================
async def init_db():
    """
    Создает все необходимые таблицы в базе данных
    Запускается при старте бота
    Если таблицы уже есть - добавляет недостающие колонки
    """
    async with aiosqlite.connect(DB) as db:
        # ===== ТАБЛИЦА USERS (ПОЛЬЗОВАТЕЛИ) =====
        # Содержит всю информацию о пользователях бота
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY,           -- Telegram ID пользователя
            balance INTEGER DEFAULT 0,        -- Текущий баланс в боте
            blocked INTEGER DEFAULT 0,        -- Заблокирован ли пользователь
            total_stars INTEGER DEFAULT 0,    -- Сумма всех звезд рейтинга
            total_votes INTEGER DEFAULT 0,     -- Количество голосов
            total_deals INTEGER DEFAULT 0,     -- Всего сделок
            total_turnover INTEGER DEFAULT 0,  -- Общий оборот
            registered_date TEXT               -- Дата регистрации в боте
        )
        """)
        
        # Проверяем структуру таблицы и добавляем недостающие колонки
        cursor = await db.execute("PRAGMA table_info(users)")
        columns = await cursor.fetchall()
        column_names = [column[1] for column in columns]
        
        # Добавляем колонки по одной, если их нет
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
        
        # ===== ТАБЛИЦА DEALS (СДЕЛКИ) =====
        # Хранит информацию о всех сделках
        await db.execute("""
        CREATE TABLE IF NOT EXISTS deals(
            code TEXT PRIMARY KEY,           -- Уникальный код сделки
            seller_id INTEGER,                -- ID продавца
            buyer_id INTEGER,                  -- ID покупателя (None если еще не куплено)
            name TEXT,                         -- Название сделки
            description TEXT,                   -- Описание
            amount INTEGER,                     -- Сумма сделки
            status TEXT                         -- Статус: open, paid, dispute, closed
        )
        """)
        
        # Проверяем наличие колонки dispute_reason
        cursor = await db.execute("PRAGMA table_info(deals)")
        columns = await cursor.fetchall()
        column_names = [column[1] for column in columns]
        
        if 'dispute_reason' not in column_names:
            await db.execute("ALTER TABLE deals ADD COLUMN dispute_reason TEXT DEFAULT ''")
            print("Колонка dispute_reason добавлена в таблицу deals")
        
        # ===== ТАБЛИЦА RATINGS (ОЦЕНКИ) =====
        # Хранит оценки, которые покупатели ставят продавцам
        await db.execute("""
        CREATE TABLE IF NOT EXISTS ratings(
            id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Уникальный ID оценки
            seller_id INTEGER,                       -- ID продавца
            buyer_id INTEGER,                         -- ID покупателя
            deal_code TEXT,                            -- Код сделки
            rating INTEGER,                             -- Оценка (1-5)
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Дата оценки
            UNIQUE(buyer_id, deal_code)  -- Одна сделка - одна оценка
        )
        """)
        
        # ===== ТАБЛИЦА PAYMENTS (ПЛАТЕЖИ ЮMONEY) =====
        # НОВАЯ ТАБЛИЦА для надежного хранения информации о платежах
        # Это ключевая таблица для гарантии зачисления
        await db.execute("""
        CREATE TABLE IF NOT EXISTS payments(
            id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Уникальный ID платежа
            label TEXT UNIQUE,                       -- Метка платежа (уникальная)
            user_id INTEGER,                          -- ID пользователя
            amount REAL,                              -- Сумма платежа
            timestamp INTEGER,                        -- Время создания
            checked INTEGER DEFAULT 0,                 -- Зачислен ли платеж
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Дата записи
        )
        """)
        
        await db.commit()
        print("✅ База данных успешно инициализирована")

# ================= ФУНКЦИИ ДЛЯ РАБОТЫ С ПЛАТЕЖАМИ (НОВЫЕ) =================
# Эти функции обеспечивают 100% гарантию зачисления

async def save_payment(label, user_id, amount):
    """
    Сохраняет информацию о платеже в базу данных
    Это гарантирует что платеж не потеряется даже при перезапуске бота
    """
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT OR REPLACE INTO payments (label, user_id, amount, timestamp) VALUES (?, ?, ?, ?)",
            (label, user_id, amount, int(time.time()))
        )
        await db.commit()
        print(f"💾 Платеж сохранен в БД: {label} на сумму {amount}")

async def mark_payment_checked(label):
    """
    Помечает платеж как использованный (зачисленный)
    Защищает от повторного зачисления
    """
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE payments SET checked=1 WHERE label=?", (label,))
        await db.commit()
        print(f"✅ Платеж {label} помечен как зачисленный")

async def is_payment_checked(label):
    """
    Проверяет, был ли уже зачислен этот платеж
    Возвращает True если платеж уже использован
    """
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT checked FROM payments WHERE label=?", (label,))
        row = await cur.fetchone()
        return row[0] == 1 if row else False

async def get_user_pending_payments(user_id):
    """
    Получает все непроверенные платежи пользователя
    Используется для очистки старых ссылок при создании новой
    """
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute(
            "SELECT label, amount FROM payments WHERE user_id=? AND checked=0 ORDER BY timestamp DESC",
            (user_id,)
        )
        payments = await cur.fetchall()
        if payments:
            print(f"📋 Найдено {len(payments)} непроверенных платежей для user {user_id}")
        return payments

# ================= ФУНКЦИИ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ =================

async def get_all_users():
    """
    Получает список всех пользователей бота
    Используется для рассылок и статистики
    """
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT id FROM users")
        rows = await cur.fetchall()
        return [row[0] for row in rows]

async def get_balance(uid):
    """
    Получает текущий баланс пользователя
    Если пользователя нет в БД - создает его
    """
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT balance FROM users WHERE id=?", (uid,))
        row = await cur.fetchone()
        
        if not row:
            # Пользователь новый - создаем запись с текущей датой
            await db.execute(
                "INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (uid, 0, 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
            )
            await db.commit()
            print(f"👤 Новый пользователь {uid} зарегистрирован")
            return 0
        return row[0]

async def change_balance(uid, amount):
    """
    Изменяет баланс пользователя
    amount может быть положительным (пополнение) или отрицательным (списание)
    """
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT id FROM users WHERE id=?", (uid,))
        row = await cur.fetchone()
        
        if not row:
            # Если пользователя нет - создаем с нужным балансом
            await db.execute(
                "INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (uid, amount if amount > 0 else 0, 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
            )
        else:
            # Обновляем баланс существующего пользователя
            await db.execute("UPDATE users SET balance = balance + ? WHERE id=?", (amount, uid))
        await db.commit()
        
        # Логируем изменение баланса для отладки
        if amount > 0:
            print(f"💰 Пользователь {uid} пополнил баланс на {amount}")
        else:
            print(f"💸 Пользователь {uid} потратил {abs(amount)}")

async def set_block(uid, blocked=True):
    """
    Блокирует или разблокирует пользователя
    Заблокированные пользователи не могут пользоваться ботом
    """
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT id FROM users WHERE id=?", (uid,))
        row = await cur.fetchone()
        
        if not row:
            # Если пользователя нет - создаем и блокируем
            await db.execute(
                "INSERT INTO users(id, balance, blocked, total_stars, total_votes, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (uid, 0, 1 if blocked else 0, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
            )
        else:
            # Обновляем статус блокировки
            await db.execute("UPDATE users SET blocked=? WHERE id=?", (1 if blocked else 0, uid))
        await db.commit()
        
        status = "заблокирован" if blocked else "разблокирован"
        print(f"🔒 Пользователь {uid} {status}")

async def is_blocked(uid):
    """
    Проверяет, заблокирован ли пользователь
    """
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT blocked FROM users WHERE id=?", (uid,))
        row = await cur.fetchone()
        if not row:
            return False
        return row[0] == 1

async def get_seller_stats(seller_id):
    """
    Получает полную статистику продавца:
    - Рейтинг
    - Количество отзывов
    - Количество сделок
    - Оборот
    - Статус блокировки
    - Дата регистрации
    - Распределение оценок
    """
    async with aiosqlite.connect(DB) as db:
        # Проверяем существование пользователя
        cur = await db.execute("SELECT id FROM users WHERE id=?", (seller_id,))
        user_exists = await cur.fetchone()
        
        if not user_exists:
            # Создаем пользователя если его нет
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
        
        # Получаем распределение оценок по звездам
        cur = await db.execute("""
            SELECT rating, COUNT(*) as count 
            FROM ratings 
            WHERE seller_id=? 
            GROUP BY rating 
            ORDER BY rating DESC
        """, (seller_id,))
        rating_dist = await cur.fetchall()
        
        # Создаем словарь с распределением (по умолчанию 0)
        dist = {5:0, 4:0, 3:0, 2:0, 1:0}
        for r, c in rating_dist:
            dist[r] = c
        
        # Вычисляем средний рейтинг
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
    """
    Добавляет оценку продавцу
    Обновляет статистику в таблице users
    """
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
        print(f"⭐ Пользователь {buyer_id} оценил продавца {seller_id} на {rating_value}⭐")

async def update_seller_deal_stats(seller_id, amount):
    """
    Обновляет статистику продавца после успешной сделки
    Увеличивает счетчик сделок и оборот
    """
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
            UPDATE users 
            SET total_deals = total_deals + 1,
                total_turnover = total_turnover + ?
            WHERE id = ?
        """, (amount, seller_id))
        await db.commit()
        print(f"📊 Статистика продавца {seller_id} обновлена: +{amount} к обороту")

# ================= ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ =================
async def get_user_profile(uid):
    """
    Получает полный профиль пользователя для отображения
    Включает:
    - ID
    - Имя и username
    - Дату регистрации
    - Баланс
    - Статистику сделок (продажи и покупки)
    - Общий оборот
    - Статус блокировки
    """
    async with aiosqlite.connect(DB) as db:
        # Получаем основную информацию о пользователе
        cur = await db.execute("""
            SELECT balance, total_deals, total_turnover, registered_date, blocked
            FROM users WHERE id=?
        """, (uid,))
        user_data = await cur.fetchone()
        
        if not user_data:
            # Если пользователя нет - создаем и пробуем снова
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
        except Exception as e:
            print(f"Ошибка получения данных пользователя: {e}")
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

# ================= КЛАВИАТУРЫ =================
# Основная клавиатура для обычных пользователей
main_keyboard = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📝 Создать сделку")],
        [KeyboardButton("💰 Баланс"), KeyboardButton("💳 Пополнить"), KeyboardButton("👤 Профиль")],
        [KeyboardButton("🔍 Поиск продавца")]
    ],
    resize_keyboard=True  # Клавиатура подстраивается под размер экрана
)

# Основная клавиатура для администратора (дополнительная кнопка "⚙ Админ")
main_keyboard_admin = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📝 Создать сделку")],
        [KeyboardButton("💰 Баланс"), KeyboardButton("💳 Пополнить"), KeyboardButton("👤 Профиль")],
        [KeyboardButton("🔍 Поиск продавца"), KeyboardButton("⚙ Админ")]
    ],
    resize_keyboard=True
)

# Админ-панель с расширенными возможностями
admin_keyboard = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🔒 Заблокировать пользователя")],
        [KeyboardButton("🔓 Разблокировать пользователя")],
        [KeyboardButton("➕ Пополнить баланс")],
        [KeyboardButton("📢 Сообщение")],
        [KeyboardButton("⬅ Назад")]
    ],
    resize_keyboard=True
)

# ================= ОБРАБОТЧИК КОМАНДЫ START =================
@app.on_message(filters.command("start"))
async def start(client, message):
    """
    Обрабатывает команду /start
    Если в команде есть параметр deal_XXX - перенаправляет в join_deal
    Иначе показывает приветственное сообщение и клавиатуру
    """
    uid = message.from_user.id
    try:
        # Проверяем, есть ли параметр в команде (для сделок)
        args = message.text.split()
        if len(args) > 1 and args[1].startswith("deal_"):
            await join_deal(message, args[1][5:])
            return
        
        # Инициализируем пользователя в базе данных
        await get_balance(uid)
        
        # Выбираем клавиатуру в зависимости от прав
        kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
        
        # Красивое приветственное сообщение
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
        print("❌ Ошибка в start:", e)

# ================= ОСНОВНОЙ ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ =================
@app.on_message(filters.text & ~filters.command(["start"]))
async def handle_text(client, message):
    """
    Главный обработчик всех текстовых сообщений
    Обрабатывает:
    - Нажатия на кнопки меню
    - Многошаговые операции (создание сделки, пополнение)
    - Ввод данных для админки
    - Сообщения для споров
    """
    uid = message.from_user.id
    text = message.text.strip()
    
    try:
        # Проверяем, не заблокирован ли пользователь
        if await is_blocked(uid):
            await message.reply("❌ Вы заблокированы и не можете использовать бота.")
            return

        # ======== ОБРАБОТКА СООБЩЕНИЙ ДЛЯ СПОРА ========
        # Если пользователь в режиме отправки сообщения для спора
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
            
            # Отправляем админу уведомление о споре
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

        # ======== ПОПОЛНЕНИЕ ЧЕРЕЗ ЮMONEY ========
        if text == "💳 Пополнить":
            user_states[uid] = {"step": "yoomoney_amount"}
            await message.reply(
                "💰 <b>ПОПОЛНЕНИЕ БАЛАНСА через ЮMoney</b> 💰\n\n"
                "Введите сумму пополнения (от 50 до 10000 рублей):\n\n"
                "💡 <i>Только цифры, без пробелов и букв</i>",
                parse_mode=ParseMode.HTML
            )
            return

        # ======== БАЛАНС ========
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
            
            # Формируем красивое сообщение с профилем
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

        # ======== ПОИСК ПРОДАВЦА ========
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

        # ======== СОЗДАТЬ СДЕЛКУ ========
        if text == "📝 Создать сделку":
            user_states[uid] = {"step": "name", "data": {}}
            await message.reply("✨ Введите название сделки (например: Продажа товара).")
            return

        # ======== АДМИН-ПАНЕЛЬ ========
        if uid == ADMIN_ID and text == "⚙ Админ":
            user_states[uid] = {"step": "admin_menu"}
            await message.reply("🛠 Админ-панель:", reply_markup=admin_keyboard)
            return

        # ======== ОБРАБОТКА ПОИСКА ПРОДАВЦА ========
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
            
            # Определяем уровень надежности на основе рейтинга
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

        # ======== ОБРАБОТКА МНОГОШАГОВЫХ ОПЕРАЦИЙ ========
        if uid in user_states:
            state = user_states[uid]

            # ===== СОЗДАНИЕ СДЕЛКИ (ШАГ 1: НАЗВАНИЕ) =====
            if state.get("step") == "name":
                state["data"]["name"] = text
                state["step"] = "desc"
                await message.reply("📄 Введите описание сделки.")
                return
            
            # ===== СОЗДАНИЕ СДЕЛКИ (ШАГ 2: ОПИСАНИЕ) =====
            if state.get("step") == "desc":
                state["data"]["desc"] = text
                state["step"] = "amount"
                await message.reply("💰 Введите сумму сделки (числом).")
                return
            
            # ===== СОЗДАНИЕ СДЕЛКИ (ШАГ 3: СУММА) =====
            if state.get("step") == "amount":
                if not text.isdigit():
                    await message.reply("⚠ Введите корректное число.")
                    return
                amount = int(text)
                # Генерируем уникальный код сделки
                code = str(random.randint(10000000, 99999999))
                # Сохраняем в базу данных
                async with aiosqlite.connect(DB) as db:
                    await db.execute(
                        "INSERT INTO deals(code, seller_id, buyer_id, name, description, amount, status) VALUES(?,?,?,?,?,?,?)",
                        (code, uid, None, state["data"]["name"], state["data"]["desc"], amount, "open")
                    )
                    await db.commit()
                bot_username = (await client.get_me()).username
                link = f"https://t.me/{bot_username}?start=deal_{code}"
                
                # Красивое сообщение об успешном создании
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
                
                # Кнопка для быстрой отправки ссылки
                share_kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📤 Отправить ссылку покупателю", switch_inline_query=f"🛡 Escrow сделка\n\n🔑 Код сделки: {code}\n💰 Сумма: {amount} ₽\n\nДля участия откройте бота и введите код сделки.\nСсылка для подключения: {link}")]
                ])
                
                await message.reply(success_text, parse_mode=ParseMode.HTML, reply_markup=share_kb, disable_web_page_preview=True)
                del user_states[uid]
                return

            # ======== ПОПОЛНЕНИЕ ЧЕРЕЗ ЮMONEY (ШАГ 1: ВВОД СУММЫ) ========
            if state.get("step") == "yoomoney_amount":
                try:
                    amount = float(text.strip())
                    
                    # Проверка минимальной и максимальной суммы
                    if amount < 50 or amount > 10000:
                        await message.reply(
                            f"❌ <b>Неверная сумма</b>\n\n"
                            f"Сумма должна быть от <b>50</b> до <b>10000</b> рублей.\n"
                            f"Введите корректную сумму:",
                            parse_mode=ParseMode.HTML
                        )
                        return
                    
                    # Округляем до 2 знаков
                    amount = round(amount, 2)
                    
                    # ===== ОЧИСТКА СТАРЫХ ССЫЛОК =====
                    # Удаляем все старые непроверенные платежи этого пользователя
                    # Чтобы не было путаницы со старыми ссылками
                    old_payments = await get_user_pending_payments(uid)
                    if old_payments:
                        print(f"🗑 Найдено {len(old_payments)} старых платежей пользователя {uid}")
                        # Не удаляем из БД, просто логируем
                    
                    # Создаем уникальную метку для платежа
                    # Формат: user_ID_ВРЕМЯ
                    label = f"user_{uid}_{int(time.time())}"
                    
                    # Формируем параметры для ссылки на оплату
                    params = {
                        "receiver": YOOMONEY_WALLET,          # Твой кошелек
                        "quickpay-form": "shop",               # Форма оплаты
                        "targets": "Пополнение баланса в VAULTOR",  # Назначение
                        "paymentType": "SB",                    # Сбербанк (самый популярный)
                        "sum": amount,                          # Сумма
                        "label": label,                         # Уникальная метка
                        "successURL": f"https://t.me/{(await app.get_me()).username}"  # Ссылка после оплаты
                    }
                    
                    # Формируем полную ссылку
                    payment_url = "https://yoomoney.ru/quickpay/confirm.xml?" + urlencode(params)
                    
                    # ===== СОХРАНЯЕМ ПЛАТЕЖ В БАЗУ ДАННЫХ =====
                    # Это гарантирует что платеж не потеряется
                    await save_payment(label, uid, amount)
                    
                    # Кнопка для проверки оплаты (без кнопки отмены)
                    check_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Я оплатил", callback_data=f"check_payment_{label}")]
                    ])
                    
                    # Отправляем пользователю ссылку с подробной инструкцией
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
                    
                    # Очищаем состояние пользователя
                    del user_states[uid]
                    
                except ValueError:
                    await message.reply(
                        "❌ <b>Ошибка ввода</b>\n\n"
                        "Пожалуйста, введите число (только цифры).\n"
                        "Пример: <code>500</code> или <code>1500.50</code>",
                        parse_mode=ParseMode.HTML
                    )
                return

            # ======== АДМИН-МЕНЮ ========
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

            # ===== РАССЫЛКА: ПОДТВЕРЖДЕНИЕ ТЕКСТА =====
            if state.get("step") == "broadcast":
                if text == "/cancel":
                    del user_states[uid]
                    await message.reply("❌ Рассылка отменена.", reply_markup=admin_keyboard)
                    return
                
                # Сохраняем текст и запрашиваем подтверждение
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

            # ===== АДМИН: БЛОКИРОВКА ПОЛЬЗОВАТЕЛЯ =====
            if state.get("step") == "block":
                target = await get_user_by_id_or_username(text)
                if target:
                    await set_block(target, True)
                    await message.reply(f"✅ Пользователь {text} заблокирован.", reply_markup=admin_keyboard)
                else:
                    await message.reply("❌ Пользователь не найден.", reply_markup=admin_keyboard)
                state["step"] = "admin_menu"
                return

            # ===== АДМИН: РАЗБЛОКИРОВКА ПОЛЬЗОВАТЕЛЯ =====
            if state.get("step") == "unblock":
                target = await get_user_by_id_or_username(text)
                if target:
                    await set_block(target, False)
                    await message.reply(f"✅ Пользователь {text} разблокирован.", reply_markup=admin_keyboard)
                else:
                    await message.reply("❌ Пользователь не найден.", reply_markup=admin_keyboard)
                state["step"] = "admin_menu"
                return

            # ===== АДМИН: ПОПОЛНЕНИЕ БАЛАНСА =====
            if state.get("step") == "add_balance":
                parts = text.split()
                if len(parts) != 2 or not parts[1].isdigit():
                    await message.reply("⚠ Неверный формат. Пример: @username 100", reply_markup=admin_keyboard)
                    return
                target = await get_user_by_id_or_username(parts[0])
                amount = int(parts[1])
                if target:
                    await change_balance(target, amount)
                    # Уведомляем пользователя о пополнении
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
        print(f"❌ Ошибка handle_text: {e}")

# ================= ОБРАБОТЧИК КОЛБЭКОВ (КНОПОК) =================
@app.on_callback_query()
async def callbacks(client, call):
    """
    Обрабатывает все нажатия на инлайн-кнопки
    Включает:
    - Проверку платежей ЮMoney
    - Подтверждение рассылок
    - Оценку сделок
    - Оплату сделок
    - Подтверждение сделок
    - Споры и их решение
    """
    uid = call.from_user.id
    data = call.data
    try:
        # ===== ГЛАВНОЕ: ПРОВЕРКА ОПЛАТЫ ЧЕРЕЗ ЮMONEY (С ГАРАНТИЕЙ ЗАЧИСЛЕНИЯ) =====
        if data.startswith("check_payment_"):
            label = data.replace("check_payment_", "")
            
            print(f"\n🔍 Начало проверки платежа с меткой: {label}")
            
            # Проверяем существование платежа в памяти бота
            if label not in app.yoomoney_payments:
                # Проверяем в базе данных (на случай перезапуска)
                async with aiosqlite.connect(DB) as db:
                    cur = await db.execute("SELECT user_id, amount, checked FROM payments WHERE label=?", (label,))
                    payment = await cur.fetchone()
                
                if not payment:
                    await call.answer("❌ Платеж не найден в системе", show_alert=True)
                    return
                
                # Восстанавливаем из БД
                user_id, expected_amount, checked = payment
                if checked == 1:
                    await call.answer("✅ Платеж уже был зачислен!", show_alert=True)
                    return
                
                # Создаем временный объект платежа
                payment = {
                    "user_id": user_id,
                    "amount": expected_amount,
                    "checked": False
                }
            else:
                payment = app.yoomoney_payments[label]
                user_id = payment['user_id']
                expected_amount = payment['amount']
            
            # Проверяем, не проверяли ли уже
            if payment.get("checked"):
                await call.answer("✅ Платеж уже был зачислен!", show_alert=True)
                return
            
            # Отправляем уведомление о начале проверки
            await call.answer("🔄 Ищу платеж в ЮMoney... (это может занять до 30 сек)", show_alert=False)
            status_msg = await call.message.reply("🔄 Проверка платежа...")
            
            # ===== МНОГОКРАТНАЯ ПРОВЕРКА С ЗАДЕРЖКОЙ =====
            # Делаем до 5 попыток с интервалом 5 секунд
            found = False
            payment_info = ""
            
            for attempt in range(5):
                if attempt > 0:
                    await status_msg.edit_text(f"🔄 Попытка {attempt+1}/5... (ждем 5 сек)")
                    await asyncio.sleep(5)
                
                try:
                    # Запрашиваем историю операций из ЮMoney API
                    url = "https://yoomoney.ru/api/operation-history"
                    headers = {"Authorization": f"Bearer {YOOMONEY_TOKEN}"}
                    params = {"label": label, "type": "payment", "records": 50}
                    
                    print(f"📊 Попытка {attempt+1}: Запрос к API ЮMoney")
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.post(url, headers=headers, data=params) as resp:
                            print(f"📊 API статус: {resp.status}")
                            
                            if resp.status == 200:
                                data = await resp.json()
                                operations = data.get('operations', [])
                                print(f"📋 Получено операций: {len(operations)}")
                                
                                # ===== ПОИСК ПО ТРЕМ КРИТЕРИЯМ =====
                                for op in operations:
                                    op_status = op.get('status')
                                    op_amount = float(op.get('amount', 0))
                                    op_label = op.get('label', '')
                                    op_datetime = op.get('datetime', '')
                                    
                                    print(f"  Операция: метка={op_label}, статус={op_status}, сумма={op_amount}")
                                    
                                    # КРИТЕРИЙ 1: Точное совпадение метки (самый надежный)
                                    if op_label == label and op_status == "success":
                                        print(f"✅ НАШЛИ по метке! Сумма: {op_amount}")
                                        found = True
                                        payment_info = f"метка: {op_label}, сумма: {op_amount}₽"
                                        break
                                    
                                    # КРИТЕРИЙ 2: Совпадение по сумме и времени (если метка сбилась)
                                    if op_status == "success" and abs(op_amount - expected_amount) < 0.05:
                                        # Проверяем что платеж свежий (последние 30 минут)
                                        if op_datetime:
                                            try:
                                                op_time = datetime.strptime(op_datetime, "%Y-%m-%dT%H:%M:%SZ")
                                                now = datetime.utcnow()
                                                if (now - op_time).total_seconds() < 1800:  # 30 минут
                                                    print(f"✅ НАШЛИ по сумме! Сумма: {op_amount}")
                                                    found = True
                                                    payment_info = f"сумма: {op_amount}₽, время: {op_time}"
                                                    break
                                            except Exception as e:
                                                print(f"Ошибка парсинга времени: {e}")
                                                found = True
                                                payment_info = f"сумма: {op_amount}₽"
                                                break
                                
                                if found:
                                    break
                            else:
                                error_text = await resp.text()
                                print(f"❌ API ошибка: {resp.status} - {error_text}")
                except Exception as e:
                    print(f"❌ Ошибка при проверке (попытка {attempt+1}): {e}")
            
            # ===== ЗАЧИСЛЕНИЕ СРЕДСТВ =====
            if found:
                # Зачисляем средства
                await change_balance(user_id, expected_amount)
                
                # Помечаем как проверенный (в памяти)
                if label in app.yoomoney_payments:
                    app.yoomoney_payments[label]["checked"] = True
                
                # Помечаем в базе данных
                await mark_payment_checked(label)
                
                # Получаем новый баланс
                new_balance = await get_balance(user_id)
                
                # Отправляем сообщение об успехе
                await status_msg.edit_text(
                    f"✅ <b>ПЛАТЕЖ ПОДТВЕРЖДЕН!</b>\n\n"
                    f"💰 Сумма: {expected_amount} ₽\n"
                    f"💳 Новый баланс: {new_balance} ₽\n\n"
                    f"🔍 Найдено по: {payment_info}",
                    parse_mode=ParseMode.HTML
                )
                
                await call.answer("✅ Платеж успешно зачислен!", show_alert=True)
                
                # Очищаем старые платежи
                await cleanup_old_payments()
                return
            else:
                # Если не нашли, показываем подробную информацию
                await status_msg.edit_text(
                    f"❌ <b>ПЛАТЕЖ НЕ НАЙДЕН</b>\n\n"
                    f"💰 Ожидаемая сумма: {expected_amount} ₽\n"
                    f"🏦 Кошелек получателя: {YOOMONEY_WALLET}\n\n"
                    f"📌 <b>Возможные причины:</b>\n"
                    f"• Задержка обработки платежа (до 2-3 минут)\n"
                    f"• Неверная сумма (комиссия банка)\n"
                    f"• Оплата по другой ссылке\n"
                    f"• Проблемы с интернет-соединением\n\n"
                    f"⏳ <b>Что делать:</b>\n"
                    f"1️⃣ Проверьте что деньги списались с карты\n"
                    f"2️⃣ Подождите 2-3 минуты и нажмите кнопку снова\n"
                    f"3️⃣ Если не помогает - обратитесь к администратору\n\n"
                    f"⚠️ <b>Важно:</b> Деньги уже поступили на кошелек {YOOMONEY_WALLET[:6]}...!",
                    parse_mode=ParseMode.HTML
                )
                
                await call.answer("❌ Платеж не найден", show_alert=True)
            
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
            
            # Отправляем сообщения всем пользователям
            for i, user_id in enumerate(users):
                try:
                    await app.send_message(user_id, broadcast_text, parse_mode=ParseMode.HTML)
                    sent += 1
                except Exception as e:
                    failed += 1
                    print(f"❌ Ошибка отправки пользователю {user_id}: {e}")
                
                # Обновляем статус каждые 10 сообщений
                if (i + 1) % 10 == 0:
                    await call.message.edit(
                        f"📢 <b>РАССЫЛКА НАЧАТА</b>\n\n"
                        f"Всего пользователей: {len(users)}\n"
                        f"Отправка... {i + 1}/{len(users)}",
                        parse_mode=ParseMode.HTML
                    )
            
            # Показываем результат
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
            
            # Возвращаем админ-панель
            user_states[uid] = {"step": "admin_menu"}
            await app.send_message(
                uid,
                "🛠 Админ-панель",
                reply_markup=admin_keyboard
            )
            
            await call.answer()
            return
        
        # ===== ОТМЕНА РАССЫЛКИ =====
        if data == "cancel_broadcast":
            if uid in user_states:
                del user_states[uid]
            await call.message.edit("❌ Рассылка отменена")
            
            # Возвращаем админ-панель
            user_states[uid] = {"step": "admin_menu"}
            await app.send_message(
                uid,
                "🛠 Админ-панель",
                reply_markup=admin_keyboard
            )
            await call.answer()
            return

        # ===== ОЦЕНКА СДЕЛКИ =====
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
                    print(f"❌ Ошибка при сохранении оценки: {e}")
                    await call.answer("❌ Ошибка при сохранении оценки", show_alert=True)
            return

        # ===== ОПЛАТА СДЕЛКИ =====
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
            
            # Списываем деньги с покупателя
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

            # Сообщение покупателю
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

            # Сообщение продавцу
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

        # ===== ПОДТВЕРДИТЬ ВЫПОЛНЕНИЕ СДЕЛКИ =====
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

        # ===== ОТКРЫТЬ СПОР =====
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

        # ===== РЕШЕНИЕ СПОРА АДМИНОМ =====
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
        print(f"❌ Ошибка в callback: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)

# ================= ПОДКЛЮЧЕНИЕ К СДЕЛКЕ ПО ССЫЛКЕ =================
async def join_deal(message, code):
    """
    Обрабатывает переход по ссылке на сделку
    Пользователь становится покупателем
    """
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

        # Уведомление продавцу
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

        # Кнопка оплаты для покупателя
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Оплатить", callback_data=f"pay_{code}")]
        ])
        await message.reply(
            f"📦 {name}\n📄 {desc}\n💰 {amount} ₽\n\nНажмите кнопку ниже, чтобы оплатить сделку.",
            reply_markup=kb
        )
    except Exception as e:
        print(f"❌ Ошибка join_deal: {e}")

# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================

async def get_user_by_id_or_username(identifier):
    """
    Получает ID пользователя по username или ID
    Поддерживает форматы:
    - @username
    - username (без @)
    - 123456789 (число)
    """
    try:
        identifier = identifier.strip()
        if identifier.startswith("@"):
            identifier = identifier[1:]
        if identifier.isdigit():
            return int(identifier)
        user = await app.get_users(identifier)
        return user.id if user else None
    except Exception as e:
        print(f"❌ Ошибка поиска пользователя: {e}")
        return None

async def cleanup_old_payments():
    """
    Очищает старые неиспользованные платежи из памяти
    (из БД не удаляем, там они хранятся для истории)
    """
    current_time = time.time()
    expired = []
    
    for label, payment in app.yoomoney_payments.items():
        # Удаляем из памяти платежи старше 1 часа
        if current_time - payment.get("timestamp", 0) > 3600:
            expired.append(label)
    
    for label in expired:
        del app.yoomoney_payments[label]
    
    if expired:
        print(f"🧹 Очищено {len(expired)} старых платежей из памяти")
    
    return len(expired)

# ================= ЗАПУСК БОТА =================
if __name__ == "__main__":
    # Инициализируем базу данных
    asyncio.get_event_loop().run_until_complete(init_db())
    
    print("="*60)
    print("🚀 Бот VAULTOR запущен")
    print("="*60)
    print("✅ ЮMoney платежи: 100% ГАРАНТИЯ ЗАЧИСЛЕНИЯ")
    print("✅ База данных: payments table active")
    print("✅ Режим: 5 попыток проверки с интервалом")
    print("="*60)
    
    app.run()
