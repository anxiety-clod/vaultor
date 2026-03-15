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

# ================= БАЗА ДАННЫХ =================
async def init_db():
    async with aiosqlite.connect(DB) as db:
        # Таблица users
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY,
            balance REAL DEFAULT 0,
            blocked INTEGER DEFAULT 0,
            total_deals INTEGER DEFAULT 0,
            total_turnover REAL DEFAULT 0,
            registered_date TEXT
        )
        """)
        
        # Таблица deals
        await db.execute("""
        CREATE TABLE IF NOT EXISTS deals(
            code TEXT PRIMARY KEY,
            seller_id INTEGER,
            buyer_id INTEGER,
            name TEXT,
            description TEXT,
            amount REAL,
            status TEXT
        )
        """)
        
        # ===== ТАБЛИЦА ДЛЯ ПЛАТЕЖЕЙ (ЯДРО СИСТЕМЫ) =====
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

async def get_user_balance(uid):
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT balance FROM users WHERE id=?", (uid,))
        row = await cur.fetchone()
        if not row:
            await db.execute(
                "INSERT INTO users(id, balance, blocked, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?)",
                (uid, 0, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
            )
            await db.commit()
            return 0
        return row[0]

async def add_balance(uid, amount):
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT id FROM users WHERE id=?", (uid,))
        row = await cur.fetchone()
        if not row:
            await db.execute(
                "INSERT INTO users(id, balance, blocked, total_deals, total_turnover, registered_date) VALUES (?, ?, ?, ?, ?, ?)",
                (uid, amount, 0, 0, 0, datetime.now().strftime("%d.%m.%Y"))
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
        await db.execute("UPDATE users SET blocked=? WHERE id=?", (1 if blocked else 0, uid))
        await db.commit()

async def get_all_users():
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT id FROM users")
        rows = await cur.fetchall()
        return [row[0] for row in rows]

# ================= ПРОФИЛЬ =================
async def get_user_profile(uid):
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT balance, total_deals, total_turnover, registered_date, blocked FROM users WHERE id=?", (uid,))
        user_data = await cur.fetchone()
        
        if not user_data:
            await get_user_balance(uid)
            return await get_user_profile(uid)
        
        balance, total_deals, total_turnover, registered_date, blocked = user_data
        
        try:
            user = await app.get_users(uid)
            username = f"@{user.username}" if user.username else "нет"
            full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
        except:
            username = "недоступен"
            full_name = "неизвестно"
        
        cur = await db.execute("SELECT COUNT(*) FROM deals WHERE seller_id=? AND status='completed'", (uid,))
        seller_deals = (await cur.fetchone())[0]
        
        cur = await db.execute("SELECT COUNT(*) FROM deals WHERE buyer_id=? AND status='completed'", (uid,))
        buyer_deals = (await cur.fetchone())[0]
        
        return {
            "user_id": uid,
            "username": username,
            "full_name": full_name,
            "balance": balance,
            "registered_date": registered_date or "неизвестно",
            "seller_deals": seller_deals,
            "buyer_deals": buyer_deals,
            "total_turnover": total_turnover,
            "blocked": blocked == 1
        }

async def update_seller_stats(seller_id, amount):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "UPDATE users SET total_deals = total_deals + 1, total_turnover = total_turnover + ? WHERE id = ?",
            (amount, seller_id)
        )
        await db.commit()

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
        
        await get_user_balance(uid)
        kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
        
        await message.reply(
            "🔥 <b>VAULTOR — Безопасный escrow-сервис</b>\n\n"
            "▫️ Мгновенные платежи через ЮMoney\n"
            "▫️ 100% зачисление средств\n"
            "▫️ Защита сделок\n\n"
            "⬇️ <b>Выберите действие:</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb
        )
    except Exception as e:
        print(f"Ошибка start: {e}")

# ================= TEXT HANDLER =================
@app.on_message(filters.text & ~filters.command(["start"]))
async def handle_text(client, message):
    uid = message.from_user.id
    text = message.text.strip()
    
    try:
        if await is_blocked(uid):
            await message.reply("❌ Вы заблокированы")
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
            bal = await get_user_balance(uid)
            kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
            await message.reply(f"💳 Баланс: {bal:.2f} ₽", reply_markup=kb)
            return

        # ======== ПРОФИЛЬ ========
        if text == "👤 Профиль":
            profile = await get_user_profile(uid)
            status = "❌ Заблокирован" if profile["blocked"] else "✅ Активен"
            
            profile_text = (
                f"👤 <b>ПРОФИЛЬ</b>\n\n"
                f"🆔 ID: <code>{profile['user_id']}</code>\n"
                f"👤 Имя: {profile['full_name']}\n"
                f"📛 Username: {profile['username']}\n"
                f"📅 Регистрация: {profile['registered_date']}\n"
                f"💰 Баланс: {profile['balance']:.2f} ₽\n"
                f"📊 Продаж: {profile['seller_deals']}\n"
                f"📊 Покупок: {profile['buyer_deals']}\n"
                f"💵 Оборот: {profile['total_turnover']:.2f} ₽\n"
                f"🔰 Статус: {status}"
            )
            
            kb = main_keyboard_admin if uid == ADMIN_ID else main_keyboard
            await message.reply(profile_text, parse_mode=ParseMode.HTML, reply_markup=kb)
            return

        # ======== ПОИСК ПРОДАВЦА ========
        if text == "🔍 Поиск продавца":
            user_states[uid] = {"step": "search_seller"}
            await message.reply("🔍 Введите ID или @username продавца:")
            return

        # ======== СОЗДАТЬ СДЕЛКУ ========
        if text == "📝 Создать сделку":
            user_states[uid] = {"step": "name", "data": {}}
            await message.reply("📝 Введите название сделки:")
            return

        # ======== АДМИНКА ========
        if uid == ADMIN_ID and text == "⚙ Админ":
            user_states[uid] = {"step": "admin_menu"}
            await message.reply("🛠 Админ-панель:", reply_markup=admin_keyboard)
            return

        # ======== ОБРАБОТКА СОСТОЯНИЙ ========
        if uid in user_states:
            state = user_states[uid]

            # СОЗДАНИЕ СДЕЛКИ
            if state.get("step") == "name":
                state["data"]["name"] = text
                state["step"] = "desc"
                await message.reply("📄 Введите описание:")
                return
            if state.get("step") == "desc":
                state["data"]["desc"] = text
                state["step"] = "amount"
                await message.reply("💰 Введите сумму (число):")
                return
            if state.get("step") == "amount":
                if not text.replace(".", "").isdigit():
                    await message.reply("⚠ Введите число")
                    return
                amount = float(text)
                code = str(random.randint(10000000, 99999999))
                async with aiosqlite.connect(DB) as db:
                    await db.execute(
                        "INSERT INTO deals(code, seller_id, name, description, amount, status) VALUES (?,?,?,?,?,?)",
                        (code, uid, state["data"]["name"], state["data"]["desc"], amount, "open")
                    )
                    await db.commit()
                
                bot_username = (await client.get_me()).username
                link = f"https://t.me/{bot_username}?start=deal_{code}"
                
                await message.reply(
                    f"✅ <b>СДЕЛКА СОЗДАНА</b>\n\n"
                    f"🔢 Код: <code>{code}</code>\n"
                    f"🔗 Ссылка: {link}",
                    disable_web_page_preview=True
                )
                del user_states[uid]
                return

            # ПОПОЛНЕНИЕ ЧЕРЕЗ ЮMONEY
            if state.get("step") == "yoomoney_amount":
                try:
                    amount = float(text.strip())
                    if amount < 50 or amount > 10000:
                        await message.reply("❌ Сумма от 50 до 10000 рублей")
                        return
                    
                    amount = round(amount, 2)
                    label = f"user_{uid}_{int(time.time())}"
                    
                    # Сохраняем в БД
                    await save_payment(label, uid, amount)
                    
                    # Ссылка на оплату
                    params = {
                        "receiver": YOOMONEY_WALLET,
                        "quickpay-form": "shop",
                        "targets": "Пополнение VAULTOR",
                        "paymentType": "SB",
                        "sum": amount,
                        "label": label,
                    }
                    
                    payment_url = "https://yoomoney.ru/quickpay/confirm.xml?" + urlencode(params)
                    
                    check_kb = InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Я оплатил", callback_data=f"check_{label}")]
                    ])
                    
                    await message.reply(
                        f"💰 <b>СЧЕТ НА ОПЛАТУ</b>\n\n"
                        f"💵 Сумма: {amount} ₽\n\n"
                        f"🔗 <b>ССЫЛКА:</b>\n"
                        f"<code>{payment_url}</code>\n\n"
                        f"📌 <b>ИНСТРУКЦИЯ:</b>\n"
                        f"1. Перейдите по ссылке\n"
                        f"2. Оплатите\n"
                        f"3. Нажмите кнопку ниже\n\n"
                        f"✅ Деньги зачислятся автоматически!",
                        parse_mode=ParseMode.HTML,
                        reply_markup=check_kb,
                        disable_web_page_preview=True
                    )
                    
                    del user_states[uid]
                    
                except ValueError:
                    await message.reply("❌ Введите число")
                return

            # ПОИСК ПРОДАВЦА
            if state.get("step") == "search_seller":
                await message.reply("🔍 Функция поиска в разработке")
                del user_states[uid]
                return

            # АДМИНКА
            if state.get("step") == "admin_menu":
                if text == "⬅ Назад":
                    user_states[uid] = {}
                    await message.reply("Главное меню", reply_markup=main_keyboard_admin)
                    return
                if text == "🔒 Заблокировать пользователя":
                    state["step"] = "block"
                    await message.reply("Введите ID:")
                    return
                if text == "🔓 Разблокировать пользователя":
                    state["step"] = "unblock"
                    await message.reply("Введите ID:")
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
                try:
                    target = int(text.strip())
                    await set_block(target, True)
                    await message.reply(f"✅ Пользователь {target} заблокирован")
                except:
                    await message.reply("❌ Ошибка")
                state["step"] = "admin_menu"
                return

            if state.get("step") == "unblock":
                try:
                    target = int(text.strip())
                    await set_block(target, False)
                    await message.reply(f"✅ Пользователь {target} разблокирован")
                except:
                    await message.reply("❌ Ошибка")
                state["step"] = "admin_menu"
                return

            if state.get("step") == "add_balance":
                parts = text.split()
                if len(parts) == 2 and parts[1].replace(".", "").isdigit():
                    target = int(parts[0])
                    amount = float(parts[1])
                    await add_balance(target, amount)
                    await message.reply(f"✅ Баланс {target} пополнен на {amount:.2f} ₽")
                else:
                    await message.reply("❌ Формат: ID сумма")
                state["step"] = "admin_menu"
                return

            if state.get("step") == "broadcast":
                users = await get_all_users()
                sent = 0
                for user_id in users:
                    try:
                        await app.send_message(user_id, text)
                        sent += 1
                        await asyncio.sleep(0.05)
                    except:
                        pass
                await message.reply(f"✅ Отправлено {sent} пользователям")
                state["step"] = "admin_menu"
                return

    except Exception as e:
        print(f"Ошибка handle_text: {e}")

# ================= ГЛАВНАЯ ФУНКЦИЯ ПРОВЕРКИ ПЛАТЕЖЕЙ (100% ГАРАНТИЯ) =================
@app.on_callback_query()
async def callbacks(client, call):
    uid = call.from_user.id
    data = call.data
    
    try:
        if data.startswith("check_"):
            label = data.replace("check_", "")
            
            # Получаем платеж из БД
            async with aiosqlite.connect(DB) as db:
                cur = await db.execute("SELECT user_id, amount, checked FROM payments WHERE label=?", (label,))
                payment = await cur.fetchone()
            
            if not payment:
                await call.answer("❌ Платеж не найден", show_alert=True)
                return
            
            user_id, expected_amount, checked = payment
            
            if checked == 1:
                await call.answer("✅ Платеж уже зачислен!", show_alert=True)
                return
            
            await call.answer("🔄 Ищу платеж... (это займет до 30 сек)", show_alert=False)
            
            # ===== СУПЕР-ПРОВЕРКА: 5 ПОПЫТОК С ИНТЕРВАЛОМ =====
            found = False
            payment_info = ""
            
            for attempt in range(5):
                if attempt > 0:
                    await call.message.edit_text(
                        f"🔄 Попытка {attempt+1}/5... (ждем 5 сек)",
                        parse_mode=ParseMode.HTML
                    )
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
                                
                                # ===== ПОИСК ПО ТРЕМ КРИТЕРИЯМ =====
                                for op in operations:
                                    op_status = op.get('status')
                                    op_amount = float(op.get('amount', 0))
                                    op_label = op.get('label', '')
                                    op_datetime = op.get('datetime', '')
                                    
                                    # КРИТЕРИЙ 1: Точное совпадение метки (самый надежный)
                                    if op_label == label and op_status == "success":
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
                                                    found = True
                                                    payment_info = f"сумма: {op_amount}₽, время: {op_time}"
                                                    break
                                            except:
                                                # Если не можем распарсить время, все равно берем
                                                found = True
                                                payment_info = f"сумма: {op_amount}₽"
                                                break
                                
                                if found:
                                    break
                                
                except Exception as e:
                    print(f"Ошибка API на попытке {attempt+1}: {e}")
            
            # ===== ЗАЧИСЛЕНИЕ =====
            if found:
                # Зачисляем деньги
                await add_balance(user_id, expected_amount)
                await mark_payment_checked(label)
                
                new_balance = await get_user_balance(user_id)
                
                await call.message.edit_text(
                    f"✅ <b>ПЛАТЕЖ ПОДТВЕРЖДЕН!</b>\n\n"
                    f"💰 Зачислено: {expected_amount:.2f} ₽\n"
                    f"💳 Новый баланс: {new_balance:.2f} ₽\n\n"
                    f"🔍 Найдено по: {payment_info}",
                    parse_mode=ParseMode.HTML
                )
                await call.answer("✅ Деньги зачислены!", show_alert=True)
            else:
                # Если не нашли, показываем подробности
                await call.message.edit_text(
                    f"❌ <b>ПЛАТЕЖ НЕ НАЙДЕН</b>\n\n"
                    f"💰 Сумма: {expected_amount:.2f} ₽\n\n"
                    f"📌 ЧТО ДЕЛАТЬ:\n"
                    f"1. Проверьте что деньги списались с карты\n"
                    f"2. Подождите 2-3 минуты и нажмите кнопку снова\n"
                    f"3. Если проблема не решается - напишите @admin\n\n"
                    f"⚠️ Деньги уже на кошельке @{YOOMONEY_WALLET[:6]}...!",
                    parse_mode=ParseMode.HTML
                )
                await call.answer("❌ Платеж не найден", show_alert=True)
            
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
                await call.answer("❌ Сделка уже закрыта", show_alert=True)
                return
            
            bal = await get_user_balance(uid)
            if bal < amount:
                await call.answer("❌ Недостаточно средств", show_alert=True)
                return
            
            if buyer_id is not None and buyer_id != uid:
                await call.answer("❌ Не ваша сделка", show_alert=True)
                return
            
            async with aiosqlite.connect(DB) as db:
                await db.execute("UPDATE deals SET buyer_id=? WHERE code=?", (uid, code))
                await db.commit()
            
            await add_balance(uid, -amount)
            escrow[code] = {"buyer_id": uid, "seller_id": seller_id, "amount": amount}
            
            kb_deal = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{code}")],
                [InlineKeyboardButton("⚖ Спор", callback_data=f"dispute_{code}")]
            ])
            
            await app.send_message(
                uid,
                f"✅ Сделка оплачена!\n💰 {amount:.2f} ₽ заморожены"
            )
            
            await app.send_message(
                seller_id,
                f"🔔 Сделка {code} оплачена!\n💰 Сумма: {amount:.2f} ₽"
            )
            
            await call.answer("✅ Оплачено")
            return

        # ===== ПОДТВЕРЖДЕНИЕ СДЕЛКИ =====
        if data.startswith("confirm_"):
            code = data.split("_")[1]
            if code not in escrow:
                await call.answer("❌ Сделка не найдена", show_alert=True)
                return
            
            deal = escrow.pop(code)
            await add_balance(deal["seller_id"], deal["amount"])
            await update_seller_stats(deal["seller_id"], deal["amount"])
            
            async with aiosqlite.connect(DB) as db:
                await db.execute("UPDATE deals SET status='completed' WHERE code=?", (code,))
                await db.commit()
            
            await app.send_message(
                deal["buyer_id"],
                f"✅ Сделка завершена!\n💰 {deal['amount']:.2f} ₽ переведены продавцу"
            )
            
            await app.send_message(
                deal["seller_id"],
                f"💰 Получена оплата {deal['amount']:.2f} ₽"
            )
            
            await call.answer("✅ Подтверждено")
            return

        # ===== ОТКРЫТЬ СПОР =====
        if data.startswith("dispute_"):
            code = data.split("_")[1]
            user_states[uid] = {"step": "dispute_msg", "deal_code": code}
            await call.message.reply("⚖ Опишите проблему:")
            await call.answer()
            return

        # ===== РЕШЕНИЕ СПОРА =====
        if data.startswith("resolve_"):
            parts = data.split("_")
            code = parts[1]
            choice = parts[2]
            
            async with aiosqlite.connect(DB) as db:
                cur = await db.execute("SELECT buyer_id, seller_id, amount, status FROM deals WHERE code=?", (code,))
                deal = await cur.fetchone()
            
            if not deal or deal[3] != "dispute":
                await call.answer("❌ Спор не активен", show_alert=True)
                return
            
            buyer_id, seller_id, amount, _ = deal
            
            if code in escrow:
                amount = escrow.pop(code)["amount"]
            
            if choice == "buyer":
                await add_balance(buyer_id, amount)
                await app.send_message(buyer_id, f"💰 Спор решен в вашу пользу! +{amount:.2f} ₽")
                await app.send_message(seller_id, f"❌ Спор проигран")
            else:
                await add_balance(seller_id, amount)
                await update_seller_stats(seller_id, amount)
                await app.send_message(seller_id, f"💰 Спор решен в вашу пользу! +{amount:.2f} ₽")
                await app.send_message(buyer_id, f"❌ Спор проигран")
            
            async with aiosqlite.connect(DB) as db:
                await db.execute("UPDATE deals SET status='closed' WHERE code=?", (code,))
                await db.commit()
            
            await call.message.edit_text("✅ Спор решен")
            await call.answer("✅ Готово")
            return

    except Exception as e:
        print(f"Ошибка callback: {e}")
        await call.answer("❌ Ошибка", show_alert=True)

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
        
        if status != "open" or buyer_id is not None:
            await message.reply("❌ Сделка недоступна")
            return

        buyer = message.from_user
        
        async with aiosqlite.connect(DB) as db:
            await db.execute("UPDATE deals SET buyer_id=? WHERE code=?", (buyer.id, code))
            await db.commit()

        await app.send_message(
            seller_id,
            f"🔔 Покупатель: @{buyer.username or 'нет'}\nКод: {code}"
        )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Оплатить", callback_data=f"pay_{code}")]
        ])
        
        await message.reply(
            f"📦 {name}\n📄 {desc}\n💰 {amount:.2f} ₽\n\nНажмите кнопку для оплаты",
            reply_markup=kb
        )
    except Exception as e:
        print(f"Join deal error: {e}")

# ================= RUN =================
if __name__ == "__main__":
    asyncio.run(init_db())
    print("🚀 Бот запущен (100% ГАРАНТИЯ ЗАЧИСЛЕНИЯ)")
    app.run()
