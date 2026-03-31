import logging
import asyncio
import random
import aiohttp
from datetime import datetime, timedelta
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, ConversationHandler, filters
)
from telegram.constants import ParseMode, ChatAction

from config import *
from database import DatabaseManager
from utils import emojify, check_bot_premium, check_required_subscriptions, notify_users_with_new_tasks, bot_has_premium
from api_handlers import (
    get_subgram_tasks, get_flyer_tasks, get_botohub_tasks,
    check_subgram_subscription, check_flyer_subscription, cryptobot_check_invoices
)

logger = logging.getLogger(__name__)

pending_tasks = {}
last_verify_time = {}


def normalize_task_link(value):
    if not value:
        return ""
    link = str(value).strip()
    if not link:
        return ""
    link = link.rstrip("/")
    if link.startswith("https://t.me/"):
        link = link.replace("https://t.me/", "t.me/", 1)
    elif link.startswith("http://t.me/"):
        link = link.replace("http://t.me/", "t.me/", 1)
    elif link.startswith("t.me/"):
        pass
    elif link.startswith("@"):
        link = f"t.me/{link[1:]}"
    return link.lower()


def get_main_keyboard():
    keyboard = [
        [KeyboardButton("Приступить к заданию"), KeyboardButton("Личный кабинет")],
        [KeyboardButton("Реферальная программа"), KeyboardButton("Помощь")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    referrer_id = None
    if context.args:
        try:
            referrer_id = int(context.args[0].replace('user', ''))
        except ValueError:
            pass
    is_new = DatabaseManager.create_user(user.id, user.username, user.first_name, user.last_name, referrer_id)
    if is_new and referrer_id and referrer_id != user.id:
        DatabaseManager.execute_query('UPDATE users SET ref_level_1 = ref_level_1 + 1 WHERE user_id = ?', (referrer_id,))
        try:
            username_display = f"@{user.username}" if user.username else user.first_name
            await context.bot.send_message(
                chat_id=referrer_id,
                text=f"🎉 <b>Новый реферал 1 уровня!</b>\n👤 {username_display} (<code>{user.id}</code>)",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
    if not await check_required_subscriptions(user.id, context):
        return
    welcome = emojify(
        "🤝 <b>Добро пожаловать в Neo Link!</b>\n\n<i>Зарабатывайте деньги за подписку на каналы!</i>\n"
        "💰 <b>1 канал = 0.3 ₽</b>\n\n⚠️ <b>ВАЖНО:</b> Включите уведомления от бота!\n\n"
        "Воспользуйтесь меню ниже 👇",
        use_premium=bot_has_premium
    )
    await update.message.reply_text(welcome, reply_markup=get_main_keyboard(), parse_mode=ParseMode.HTML)


async def check_required_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if await check_required_subscriptions(user_id, context):
        await query.message.delete()
        welcome = emojify(
            "🤝 <b>Добро пожаловать в Neo Link!</b>\n\n💰 <b>1 канал = 0.3 ₽</b>\n\nВоспользуйтесь меню ниже 👇",
            use_premium=bot_has_premium
        )
        await context.bot.send_message(chat_id=user_id, text=welcome, reply_markup=get_main_keyboard(), parse_mode=ParseMode.HTML)
    else:
        await query.answer("Вы ещё не подписались на все каналы", show_alert=True)


async def show_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        can_edit = query.message.text is not None
    user_id = update.effective_user.id
    chat_id = query.message.chat_id if query else update.message.chat_id
    await context.bot.send_chat_action(chat_id, ChatAction.TYPING)

    subgram_result = await get_subgram_tasks(user_id)
    if isinstance(subgram_result, dict) and subgram_result.get("type") == "register":
        reg_url = subgram_result.get("url")
        text_reg = f"🔑 <b>Требуется регистрация в SubGram</b>\n\nПерейдите по ссылке:\n{reg_url}\n\nПосле регистрации нажмите /start"
        if query and can_edit:
            await query.edit_message_text(text_reg, parse_mode=ParseMode.HTML)
        else:
            await context.bot.send_message(chat_id=chat_id, text=text_reg, parse_mode=ParseMode.HTML)
        return

    subgram_tasks = subgram_result if isinstance(subgram_result, list) else []
    flyer_tasks = await get_flyer_tasks(user_id)
    botohub_tasks = await get_botohub_tasks(user_id)
    admin_channels = DatabaseManager.execute_query('SELECT channel_id, link, name FROM channels', fetch='all') or []
    completed = DatabaseManager.execute_query('SELECT task_id FROM completed_tasks WHERE user_id = ?', (user_id,), fetch='all') or []
    completed_ids = [c[0] for c in completed]
    traffic_orders = DatabaseManager.execute_query(
        "SELECT id, link, channel_id, amount, done FROM orders WHERE status = 'active' AND done < amount",
        fetch='all'
    ) or []

    all_tasks = []
    for ch in admin_channels:
        if ch[0] not in completed_ids:
            all_tasks.append({"id": ch[0], "link": ch[1], "name": ch[2] or f"Канал {ch[0]}", "type": "admin"})
    completed_normalized = {normalize_task_link(item) for item in completed_ids}
    for task in subgram_tasks + flyer_tasks + botohub_tasks:
        task_identity = normalize_task_link(task["id"]) if task["type"] == "botohub" else task["id"]
        if task_identity not in completed_ids and task_identity not in completed_normalized:
            all_tasks.append(task)
    for order in traffic_orders:
        order_id, link, channel_id, amount, done = order
        task_key = f"traffic_order_{order_id}"
        if task_key in completed_ids:
            continue
        is_subscribed = False
        if channel_id:
            try:
                member = await context.bot.get_chat_member(chat_id=int(channel_id), user_id=user_id)
                is_subscribed = member.status in ['member', 'administrator', 'creator']
            except Exception:
                is_subscribed = False
        if is_subscribed:
            continue
        if True:
            all_tasks.append({
                "id": task_key,
                "link": link,
                "name": "🔥 Спонсорский канал",
                "type": "traffic",
                "order_id": order_id,
                "channel_id": channel_id
            })

    if not all_tasks:
        text_no = "❌ <b>На данный момент заданий нет</b>\n\nОжидайте пополнения базы."
        if query and can_edit:
            await query.edit_message_text(text_no, parse_mode=ParseMode.HTML)
        else:
            await context.bot.send_message(chat_id=chat_id, text=text_no, parse_mode=ParseMode.HTML)
        return

    pending_tasks[user_id] = all_tasks.copy()

    if 3 <= len(all_tasks) <= 4:
        urgency_text = random.choice([
            "🔥 <b>Торопись — мест немного!</b>\n\n👥 Прямо сейчас несколько пользователей выполняют эти же задания!\n⚡️ Успей заработать раньше других!",
            "⚡️ <b>Горячие задания!</b>\n\n🏃 Другие участники уже приступили.\n💰 Действуй быстро — задания ограничены!",
        ])
        try:
            await context.bot.send_message(chat_id=chat_id, text=urgency_text, parse_mode=ParseMode.HTML)
        except Exception:
            pass

    header = emojify(
        f"📋 <b>Ваши задания ({len(all_tasks)})</b>\n\nПодпишитесь на каналы ниже\n1 подписка = {FIXED_REWARD:.2f} ₽",
        use_premium=bot_has_premium
    )
    keyboard = []
    row = []
    for i, task in enumerate(all_tasks, 1):
        row.append(InlineKeyboardButton(f"➕ Подписаться #{i}", url=task['link']))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("✅ Проверить подписки", callback_data="verify_all_tasks")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    if query and can_edit:
        await query.edit_message_text(header, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    else:
        await context.bot.send_message(chat_id=chat_id, text=header, reply_markup=reply_markup, parse_mode=ParseMode.HTML)


async def verify_all_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    now = datetime.now().timestamp()

    last_time = last_verify_time.get(user_id, 0)
    if now - last_time < 60:
        remaining = int(60 - (now - last_time))
        await query.answer(f"⏳ Подождите {remaining} сек перед следующей проверкой!", show_alert=True)
        return
    last_verify_time[user_id] = now

    tasks = pending_tasks.get(user_id, [])
    if not tasks:
        await query.edit_message_text("❌ Нет активных заданий.\nНажмите «Приступить к заданию».")
        return

    current_botohub_links = []
    current_botohub_links_normalized = set()
    botohub_pending = [t for t in tasks if t["type"] == "botohub"]
    if botohub_pending:
        try:
            async with aiohttp.ClientSession() as session:
                headers = {"Auth": BOTOHUB_API_KEY, "Content-Type": "application/json"}
                async with session.post(BOTOHUB_API_URL, json={"chat_id": user_id}, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        current_botohub_links = data.get("tasks", [])
                        current_botohub_links_normalized = {normalize_task_link(link) for link in current_botohub_links}
                    else:
                        current_botohub_links = [t["id"] for t in botohub_pending]
                        current_botohub_links_normalized = {normalize_task_link(t["id"]) for t in botohub_pending}
        except Exception:
            current_botohub_links = [t["id"] for t in botohub_pending]
            current_botohub_links_normalized = {normalize_task_link(t["id"]) for t in botohub_pending}

    completed_count = 0
    total_reward = 0.0
    subgram_new = 0
    subgram_earn = 0.0
    still_pending = []

    for task in tasks:
        task_id = task["id"]
        task_type = task["type"]

        task_lookup_id = normalize_task_link(task_id) if task_type == "botohub" else task_id
        if DatabaseManager.execute_query('SELECT id FROM completed_tasks WHERE user_id = ? AND task_id = ?', (user_id, task_lookup_id), 'one'):
            continue

        is_done = False

        if task_type == "admin":
            try:
                member = await context.bot.get_chat_member(chat_id=int(task_id), user_id=user_id)
                is_done = member.status in ['member', 'administrator', 'creator']
            except Exception:
                is_done = False
        elif task_type == "subgram":
            is_done = await check_subgram_subscription(user_id, task_id)
            if is_done:
                subgram_new += 1
                subgram_earn += FIXED_REWARD
        elif task_type == "flyer":
            is_done = await check_flyer_subscription(user_id, task_id)
        elif task_type == "botohub":
            is_done = normalize_task_link(task_id) not in current_botohub_links_normalized
        elif task_type == "traffic":
            order_id = task.get("order_id")
            channel_id = task.get("channel_id")
            if channel_id:
                try:
                    member = await context.bot.get_chat_member(chat_id=int(channel_id), user_id=user_id)
                    is_done = member.status in ['member', 'administrator', 'creator']
                    if is_done and order_id:
                        DatabaseManager.execute_query(
                            'UPDATE orders SET done = done + 1 WHERE id = ? AND done < amount',
                            (order_id,)
                        )
                        order_status = DatabaseManager.execute_query('SELECT done, amount FROM orders WHERE id = ?', (order_id,), 'one')
                        if order_status and order_status[0] >= order_status[1]:
                            DatabaseManager.execute_query("UPDATE orders SET status = 'completed' WHERE id = ?", (order_id,))
                except Exception:
                    is_done = False

        if is_done:
            DatabaseManager.execute_query(
                'INSERT INTO completed_tasks (user_id, task_id, task_type, reward) VALUES (?, ?, ?, ?)',
                (user_id, task_lookup_id, task_type, FIXED_REWARD)
            )
            DatabaseManager.execute_query(
                'UPDATE users SET balance = balance + ?, earnings_tasks = earnings_tasks + ?, total_earnings = total_earnings + ? WHERE user_id = ?',
                (FIXED_REWARD, FIXED_REWARD, FIXED_REWARD, user_id)
            )
            referrer = DatabaseManager.execute_query('SELECT referrer_id FROM users WHERE user_id = ?', (user_id,), 'one')
            if referrer and referrer[0]:
                ref_reward = FIXED_REWARD * 0.20
                DatabaseManager.execute_query(
                    'UPDATE users SET balance = balance + ?, earnings_ref = earnings_ref + ? WHERE user_id = ?',
                    (ref_reward, ref_reward, referrer[0])
                )
            completed_count += 1
            total_reward += FIXED_REWARD
        else:
            still_pending.append(task)

    if still_pending:
        pending_tasks[user_id] = still_pending
    else:
        pending_tasks.pop(user_id, None)

    if subgram_new > 0:
        DatabaseManager.add_subgram_stat(datetime.now().strftime('%Y-%m-%d'), subgram_new, subgram_earn)

    if completed_count > 0:
        if still_pending:
            msg = emojify(
                f"✅ <b>Вы заработали {total_reward:.2f} ₽!</b>\n\nОсталось заданий: {len(still_pending)}\nНажмите «Приступить к заданию» чтобы продолжить.",
                use_premium=bot_has_premium
            )
        else:
            msg = emojify(
                f"🎉 <b>Поздравляем!</b>\n\nВсе задания выполнены! Вы заработали {total_reward:.2f} ₽.\nНовые задания появятся скоро!",
                use_premium=bot_has_premium
            )
        await query.edit_message_text(msg, parse_mode=ParseMode.HTML)
    else:
        await query.edit_message_text("❌ Вы не подписались ни на один новый канал.\n\nВыполните задания и нажмите «Проверить» снова.", parse_mode=ParseMode.HTML)


async def cabinet_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    stats = DatabaseManager.execute_query(
        'SELECT balance, earnings_tasks, total_earnings, earnings_ref, ref_level_1, ref_level_2, joined_date FROM users WHERE user_id = ?',
        (user_id,), 'one'
    )
    if not stats:
        return
    balance, e_tasks, t_earn, e_ref, r1, r2, joined = stats
    joined_dt = datetime.fromisoformat(joined)
    diff = datetime.now() - joined_dt
    months = diff.days // 30
    days = diff.days % 30
    current_time = datetime.now().strftime('%H:%M:%S %d-%m-%Y')
    earn_rank, ref_count_rank, ref_earn_rank = DatabaseManager.get_user_rank(user_id)
    username_display = f"@{update.effective_user.username}" if update.effective_user.username else update.effective_user.first_name

    text = f"""🖥️ <b>ЛИЧНЫЙ КАБИНЕТ</b>
{current_time}

🆔 <b>Ваш ID:</b> <code>{user_id}</code>
⚜️ <b>Логин:</b> {username_display}
💫 <b>Статус:</b> 👤 user
🫂 <b>Вы с нами:</b> {months} мес. {days} дн.

💰 <b>Баланс:</b> <b>{balance:.2f} ₽</b>
🧾 <b>Заработано всего:</b> {t_earn:.2f} ₽
✍️ <b>С заданий:</b> {e_tasks:.2f} ₽

— <b>Рефералы 1 ур.:</b> {r1}
— <b>Рефералы 2 ур.:</b> {r2}
🤝 <b>С рефералов:</b> {e_ref:.2f} ₽

🔝 <b>Место в ТОПЕ:</b>
├ по заработку #{earn_rank}
├ по кол-ву рефералов #{ref_count_rank}
└ по заработку с рефералов #{ref_earn_rank}

🗣 <a href='https://t.me/Neo_newschannels'>Новостной канал</a>
📑 <a href='https://t.me/Neootzyvs'>Отзывы</a>
🛠 <a href='https://t.me/Neosupports_bot'>Поддержка</a>"""
    text = emojify(text, use_premium=bot_has_premium)
    keyboard = [
        [InlineKeyboardButton("💳 Вывести деньги", callback_data="start_withdraw")],
        [InlineKeyboardButton("📊 История выводов", callback_data="withdraw_history")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True)


# ---------- Вывод денег ----------
async def withdraw_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    balance = DatabaseManager.execute_query('SELECT balance FROM users WHERE user_id = ?', (query.from_user.id,), 'one')[0]
    if balance < MIN_WITHDRAW:
        await query.message.reply_text(f"❌ <b>Недостаточно средств</b>\n\nМинимум вывода: {MIN_WITHDRAW} ₽\nВаш баланс: {balance:.2f} ₽", parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    await query.message.reply_text("💳 <b>Введите номер банковской карты (16 цифр):</b>", parse_mode=ParseMode.HTML)
    return WITHDRAW_CARD


async def withdraw_card(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.replace(" ", "")
    if not (len(text) == 16 and text.isdigit()):
        await update.message.reply_text("❌ Введите 16 цифр карты:")
        return WITHDRAW_CARD
    context.user_data['card'] = text
    await update.message.reply_text("👤 <b>Введите Имя и Фамилию получателя:</b>", parse_mode=ParseMode.HTML)
    return WITHDRAW_NAME


async def withdraw_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    card = context.user_data['card']
    name = update.message.text
    if len(name) < 3:
        await update.message.reply_text("❌ Слишком короткое имя. Введите ФИО:")
        return WITHDRAW_NAME
    balance = DatabaseManager.execute_query('SELECT balance FROM users WHERE user_id = ?', (user.id,), 'one')[0]
    DatabaseManager.execute_query('INSERT INTO withdrawals (user_id, amount, card, name) VALUES (?, ?, ?, ?)', (user.id, balance, card, name))
    DatabaseManager.execute_query('UPDATE users SET balance = 0 WHERE user_id = ?', (user.id,))
    await update.message.reply_text(
        f"✅ <b>Заявка создана!</b>\n\nСумма: {balance:.2f} ₽\nСтатус: ⏳ На рассмотрении",
        parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard()
    )
    admin_text = (f"🚨 <b>НОВАЯ ЗАЯВКА НА ВЫВОД</b>\n\n👤 ID: <code>{user.id}</code>\n👤 @{user.username or user.first_name}\n💰 Сумма: {balance:.2f} ₽\n💳 Карта: <code>{card}</code>\n📝 ФИО: {name}")
    admin_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Принять", callback_data=f"withdraw_accept_{user.id}_{balance}"),
         InlineKeyboardButton("❌ Отклонить", callback_data=f"withdraw_reject_{user.id}")]
    ])
    await context.bot.send_message(chat_id=ADMIN_ID, text=admin_text, reply_markup=admin_kb, parse_mode=ParseMode.HTML)
    return ConversationHandler.END


async def withdraw_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    withdrawals = DatabaseManager.execute_query(
        'SELECT amount, card, status, date FROM withdrawals WHERE user_id = ? ORDER BY date DESC LIMIT 10',
        (user_id,), fetch='all'
    )
    if not withdrawals:
        await query.answer("У вас нет заявок на вывод", show_alert=True)
        return
    text = "📊 <b>ИСТОРИЯ ВЫВОДОВ</b>\n\n"
    for w in withdrawals:
        amount, card, status, date = w
        status_emoji = "✅" if status == "accepted" else "❌" if status == "rejected" else "⏳"
        status_text = "Выполнен" if status == "accepted" else "Отклонён" if status == "rejected" else "В обработке"
        text += f"{status_emoji} <b>{amount:.2f} ₽</b> - {status_text}\n   Карта: ...{card[-4:]}\n   Дата: {date[:10]}\n\n"
    await query.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cancel_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Вывод отменён.", reply_markup=get_main_keyboard())
    return ConversationHandler.END


# ---------- Рефералы ----------
async def referral_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    r1, r2, e_ref = DatabaseManager.execute_query(
        'SELECT ref_level_1, ref_level_2, earnings_ref FROM users WHERE user_id = ?', (user_id,), 'one'
    )
    text = f"""🧑‍🤝‍🧑 <b>РЕФЕРАЛЬНАЯ ПРОГРАММА</b>

❗️ <b>Реферал 1 уровня</b> — 20% от заработка
❗️ <b>Реферал 2 уровня</b> — 5% от заработка

📊 <b>Статистика:</b>
• Рефералов 1 ур.: {r1}
• Рефералов 2 ур.: {r2}
• Заработано: {e_ref:.2f} ₽

👁‍🗨 <b>Ваша реферальная ссылка:</b>
<code>https://t.me/{MAIN_BOT_USERNAME}?start=user{user_id}</code>"""
    text = emojify(text, use_premium=bot_has_premium)
    keyboard = [[InlineKeyboardButton("📋 Скопировать ссылку", callback_data=f"copy_ref_{user_id}")]]
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)


async def copy_referral_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.data.split('_')[2]
    await query.answer(f"Ссылка:\nhttps://t.me/{MAIN_BOT_USERNAME}?start=user{user_id}", show_alert=True)


# ---------- Помощь ----------
async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = """❓ <b>ПОМОЩЬ И ПОДДЕРЖКА</b>

<b>🗣 Наши ресурсы:</b>
• Новости: <a href='https://t.me/Neo_newschannels'>@Neo_newschannels</a>
• Отзывы: <a href='https://t.me/Neootzyvs'>@Neootzyvs</a>
• Поддержка: <a href='https://t.me/Neosupports_bot'>@Neosupports_bot</a>

<b>📝 О нас:</b>
Neo Link — сервис заработка на подписках. Сотрудничаем с лучшими рекламодателями и гарантируем своевременные выплаты."""
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=get_main_keyboard())


# ---------- Админ-команды ----------
async def check_orders_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Доступ запрещён")
        return

    active_orders = DatabaseManager.execute_query("SELECT COUNT(*) FROM orders WHERE status = 'active'", fetch='one')[0]
    total_ordered = DatabaseManager.execute_query("SELECT SUM(amount) FROM orders", fetch='one')[0] or 0
    total_done = DatabaseManager.execute_query("SELECT SUM(done) FROM orders", fetch='one')[0] or 0
    today = datetime.now().strftime('%Y-%m-%d')
    today_done = DatabaseManager.execute_query("SELECT SUM(done) FROM orders WHERE DATE(created) = ?", (today,), fetch='one')[0] or 0
    total_channels = DatabaseManager.execute_query("SELECT COUNT(DISTINCT link) FROM orders", fetch='one')[0] or 0
    total_revenue = DatabaseManager.execute_query("SELECT SUM(price) FROM orders", fetch='one')[0] or 0.0

    text = f"""📊 <b>СТАТИСТИКА СЕРВИСА NEO LINK</b>

🔄 Активных заказов: <b>{active_orders}</b>
📦 Куплено подписчиков: <b>{int(total_ordered):,}</b>
✅ Уже вступило: <b>{int(total_done):,}</b>
📅 Сегодня выполнено: <b>{int(today_done):,}</b>
📢 Всего каналов: <b>{total_channels}</b>
💰 Выручка всего: <b>{total_revenue:.2f} ₽</b>"""

    orders = DatabaseManager.execute_query(
        "SELECT id, user_id, link, amount, done, price, status, created FROM orders ORDER BY created DESC LIMIT 10",
        fetch='all'
    )
    if orders:
        text += "\n\n<b>Последние заказы:</b>\n"
        for o in orders:
            oid, uid, link, amount, done, price, status, created = o
            short_link = link[:40] + "..." if len(link) > 40 else link
            status_emoji = "✅" if status == "completed" else "🔄"
            text += f"#{oid} | {status_emoji} | {short_link} | {done}/{amount} | {price:.2f}₽ | {created[:10]}\n"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Доступ запрещён")
        return
    total_users = DatabaseManager.execute_query('SELECT COUNT(*) FROM users', fetch='one')[0]
    today = datetime.now().strftime('%Y-%m-%d')
    today_users_row = DatabaseManager.execute_query('SELECT new_users FROM stats WHERE date = ?', (today,), 'one')
    today_users = today_users_row[0] if today_users_row else 0
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    week_users = DatabaseManager.execute_query('SELECT SUM(new_users) FROM stats WHERE date >= ?', (week_ago,), fetch='one')[0] or 0
    total_balance = DatabaseManager.execute_query('SELECT SUM(balance) FROM users', fetch='one')[0] or 0
    pending_w = DatabaseManager.execute_query('SELECT SUM(amount) FROM withdrawals WHERE status = "pending"', fetch='one')[0] or 0
    text = f"""📊 <b>СТАТИСТИКА БОТА</b>

👥 <b>Пользователи:</b>
├ Всего: {total_users}
├ Сегодня: +{today_users}
└ За неделю: +{week_users}

💰 <b>Финансы:</b>
├ Баланс пользователей: {total_balance:.2f} ₽
└ Ожидает вывода: {pending_w:.2f} ₽"""
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    text = """<b>📚 АДМИН-КОМАНДЫ</b>

/stats — статистика бота
/check — статистика заказов трафика
/stat — статистика SubGram
/board — простая рассылка
/board2 — рассылка с этапами
/stats_ref — топ по рефералам
/add_ch &lt;ID&gt; &lt;ссылка&gt; &lt;название&gt; — добавить канал
/list — список каналов
/del &lt;ID&gt; — удалить канал
/work &lt;ID&gt; &lt;ссылка&gt; &lt;название&gt; — обязательный канал
/wlist — список обязательных
/delw &lt;ID&gt; — удалить обязательный"""
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /board <текст>")
        return
    message = " ".join(context.args)
    users = DatabaseManager.execute_query('SELECT user_id FROM users', fetch='all')
    sent = failed = 0
    await update.message.reply_text("⏳ Рассылка...")
    for user in users:
        try:
            await context.bot.send_message(chat_id=user[0], text=f"📢 <b>Уведомление Neo Link</b>\n\n{message}", parse_mode=ParseMode.HTML)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    await update.message.reply_text(f"✅ Отправлено: {sent}\nНе доставлено: {failed}")


async def stats_ref_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    top = DatabaseManager.execute_query('SELECT user_id, username, ref_level_1, ref_level_2 FROM users ORDER BY (ref_level_1 + ref_level_2) DESC LIMIT 10', fetch='all')
    text = "🏆 <b>ТОП-10 ПО РЕФЕРАЛАМ</b>\n\n"
    for i, (uid, uname, r1, r2) in enumerate(top, 1):
        text += f"{i}. @{uname or uid}: {r1+r2} реф. (1: {r1}, 2: {r2})\n"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def add_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    args = context.args
    if len(args) < 3:
        await update.message.reply_text("❌ /add_ch <ID> <ссылка> <название>")
        return
    channel_id, link = args[0], args[1]
    name = " ".join(args[2:])
    is_request = 0
    try:
        int(channel_id)
    except ValueError:
        is_request = 1
    DatabaseManager.execute_query('INSERT INTO channels (channel_id, link, name, is_request) VALUES (?, ?, ?, ?)', (channel_id, link, name, is_request))
    await update.message.reply_text(f"✅ Канал добавлен: {name}", parse_mode=ParseMode.HTML)


async def list_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    channels = DatabaseManager.execute_query('SELECT id, channel_id, name, link FROM channels', fetch='all')
    if not channels:
        await update.message.reply_text("📋 Список пуст.")
        return
    text = "📋 <b>Каналы в заданиях:</b>\n\n"
    for ch in channels:
        text += f"ID: {ch[0]} | {ch[2]} | {ch[3]}\n"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def delete_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("❌ /del <Telegram ID>")
        return
    DatabaseManager.execute_query('DELETE FROM channels WHERE channel_id = ?', (context.args[0],))
    await update.message.reply_text(f"✅ Канал {context.args[0]} удалён.")


async def add_required_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    args = context.args
    if len(args) < 3:
        await update.message.reply_text("❌ /work <ID> <ссылка> <название>")
        return
    channel_id, link = args[0], args[1]
    name = " ".join(args[2:])
    DatabaseManager.execute_query('INSERT INTO required_channels (channel_id, link, name) VALUES (?, ?, ?)', (channel_id, link, name))
    await update.message.reply_text(f"✅ Обязательный канал добавлен: {name}")


async def list_required_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    channels = DatabaseManager.execute_query('SELECT id, channel_id, name, link FROM required_channels', fetch='all')
    if not channels:
        await update.message.reply_text("📋 Список обязательных каналов пуст.")
        return
    text = "📋 <b>Обязательные каналы:</b>\n\n"
    for ch in channels:
        text += f"ID: {ch[0]} | {ch[2]} | {ch[3]}\n"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def delete_required_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("❌ /delw <ID>")
        return
    try:
        channel_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ ID должен быть числом")
        return
    DatabaseManager.execute_query('DELETE FROM required_channels WHERE id = ?', (channel_id,))
    await update.message.reply_text(f"✅ Обязательный канал {channel_id} удалён.")


async def handle_withdraw_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌ Только администратор", show_alert=True)
        return
    data = query.data.split('_')
    action = data[1]
    user_id = int(data[2])
    if action == "accept":
        amount = float(data[3])
        DatabaseManager.execute_query('UPDATE withdrawals SET status = "accepted" WHERE user_id = ? AND status = "pending"', (user_id,))
        await query.edit_message_text(f"{query.message.text}\n\n✅ <b>ВЫВОД ПРИНЯТ</b>", parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(chat_id=user_id, text=f"✅ <b>Вывод {amount:.2f} ₽ одобрен!</b>\n\nСредства поступят в течение 2 часов.", parse_mode=ParseMode.HTML)
        except Exception:
            pass
    elif action == "reject":
        withdrawal = DatabaseManager.execute_query('SELECT amount FROM withdrawals WHERE user_id = ? AND status = "pending" ORDER BY date DESC LIMIT 1', (user_id,), 'one')
        if withdrawal:
            DatabaseManager.execute_query('UPDATE users SET balance = balance + ? WHERE user_id = ?', (withdrawal[0], user_id))
        DatabaseManager.execute_query('UPDATE withdrawals SET status = "rejected" WHERE user_id = ? AND status = "pending"', (user_id,))
        await query.edit_message_text(f"{query.message.text}\n\n❌ <b>ВЫВОД ОТКЛОНЁН</b>", parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(chat_id=user_id, text="❌ <b>Вывод отклонён.</b>\n\nСредства возвращены на баланс. Обратитесь в поддержку.", parse_mode=ParseMode.HTML)
        except Exception:
            pass


# ---------- Рассылка с этапами ----------
async def broadcast2_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    context.user_data.clear()
    await update.message.reply_text("📢 <b>Рассылка</b> (1/3)\n\nОтправьте текст сообщения.\n/cancel для отмены.", parse_mode=ParseMode.HTML)
    return BROADCAST_TEXT


async def broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['broadcast_text'] = update.message.text_html
    await update.message.reply_text("📢 <b>Рассылка</b> (2/3)\n\nОтправьте медиа или /skip", parse_mode=ParseMode.HTML)
    return BROADCAST_MEDIA


async def broadcast_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "/skip":
        context.user_data['broadcast_media'] = None
        context.user_data['broadcast_media_type'] = None
    else:
        media = None
        media_type = None
        if update.message.photo:
            media = update.message.photo[-1].file_id
            media_type = 'photo'
        elif update.message.video:
            media = update.message.video.file_id
            media_type = 'video'
        elif update.message.document:
            media = update.message.document.file_id
            media_type = 'document'
        elif update.message.animation:
            media = update.message.animation.file_id
            media_type = 'animation'
        elif update.message.sticker:
            media = update.message.sticker.file_id
            media_type = 'sticker'
        else:
            await update.message.reply_text("❌ Неподдерживаемый тип. Или /skip")
            return BROADCAST_MEDIA
        context.user_data['broadcast_media'] = media
        context.user_data['broadcast_media_type'] = media_type
        if update.message.caption_html:
            context.user_data['broadcast_text'] = update.message.caption_html
    await update.message.reply_text("📢 <b>Рассылка</b> (3/3)\n\nКнопки: Название - ссылка (каждая на новой строке)\nИли /skip", parse_mode=ParseMode.HTML)
    return BROADCAST_BUTTONS


async def broadcast_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "/skip":
        buttons = []
    else:
        lines = update.message.text.strip().split('\n')
        buttons = []
        for line in lines:
            if ' - ' in line:
                name, data = line.split(' - ', 1)
                buttons.append((name.strip(), data.strip()))
    text = context.user_data.get('broadcast_text')
    media = context.user_data.get('broadcast_media')
    media_type = context.user_data.get('broadcast_media_type')
    text_to_display = emojify(text, use_premium=bot_has_premium) if text else None
    keyboard = []
    for name, data in buttons:
        keyboard.append([InlineKeyboardButton(name, url=data if data.startswith("http") else None, callback_data=None if data.startswith("http") else data)])
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    context.user_data['broadcast_text_to_send'] = text_to_display
    context.user_data['broadcast_reply_markup'] = reply_markup
    try:
        if media_type == 'photo':
            await update.message.reply_photo(media, caption=text_to_display, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        elif media_type == 'video':
            await update.message.reply_video(media, caption=text_to_display, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        elif not media_type:
            await update.message.reply_text(text_to_display, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка предпросмотра: {e}")
        return ConversationHandler.END
    await update.message.reply_text(
        "✅ Предпросмотр готов.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Начать рассылку", callback_data="confirm_broadcast")],
            [InlineKeyboardButton("❌ Отмена", callback_data="cancel_broadcast")]
        ])
    )
    return ConversationHandler.END


async def confirm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data != "confirm_broadcast":
        await query.edit_message_text("Рассылка отменена.")
        return ConversationHandler.END
    text = context.user_data.get('broadcast_text_to_send')
    media = context.user_data.get('broadcast_media')
    media_type = context.user_data.get('broadcast_media_type')
    reply_markup = context.user_data.get('broadcast_reply_markup')
    users = DatabaseManager.execute_query('SELECT user_id FROM users', fetch='all')
    await query.edit_message_text(f"🚀 Рассылка для {len(users)} пользователей...")
    sent = failed = 0
    for user in users:
        try:
            if media_type == 'photo':
                await context.bot.send_photo(user[0], media, caption=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            elif media_type == 'video':
                await context.bot.send_video(user[0], media, caption=text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            elif not media_type:
                await context.bot.send_message(user[0], text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    await query.message.reply_text(f"✅ <b>Рассылка завершена!</b>\n\nОтправлено: {sent}\nНе доставлено: {failed}", parse_mode=ParseMode.HTML)
    for key in ['broadcast_text', 'broadcast_media', 'broadcast_media_type', 'broadcast_text_to_send', 'broadcast_reply_markup']:
        context.user_data.pop(key, None)
    return ConversationHandler.END


async def cancel_broadcast2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Рассылка отменена.", reply_markup=get_main_keyboard())
    return ConversationHandler.END


# ---------- Статистика SubGram ----------
async def subgram_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    text = "📊 <b>Статистика SubGram за 7 дней:</b>\n\n"
    for i in range(7):
        date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        row = DatabaseManager.execute_query('SELECT subscriptions, earnings FROM subgram_stats WHERE date = ?', (date,), 'one')
        subs, earn = row if row else (0, 0.0)
        text += f"📅 {date} — 👥 {subs} подписок, 💰 {earn:.2f}₽\n"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ---------- Сборка основного бота ----------
def build_main_bot():
    app = Application.builder().token(MAIN_BOT_TOKEN).build()

    async def post_init(application):
        await check_bot_premium(application)
        application.job_queue.run_repeating(notify_users_with_new_tasks, interval=NOTIFY_INTERVAL, first=60)
        application.job_queue.run_repeating(lambda c: asyncio.create_task(cryptobot_check_invoices()), interval=300, first=30)

    app.post_init = post_init

    broadcast_conv = ConversationHandler(
        entry_points=[CommandHandler("board2", broadcast2_start)],
        states={
            BROADCAST_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_text)],
            BROADCAST_MEDIA: [
                MessageHandler(filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.ANIMATION | filters.Sticker.ALL, broadcast_media),
                CommandHandler("skip", broadcast_media)
            ],
            BROADCAST_BUTTONS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_buttons),
                CommandHandler("skip", broadcast_buttons)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_broadcast2)]
    )
    app.add_handler(broadcast_conv)

    conv_withdraw = ConversationHandler(
        entry_points=[CallbackQueryHandler(withdraw_start, pattern="^start_withdraw$")],
        states={
            WITHDRAW_CARD: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_card)],
            WITHDRAW_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, withdraw_name)],
        },
        fallbacks=[CommandHandler("cancel", cancel_withdraw)]
    )
    app.add_handler(conv_withdraw)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("check", check_orders_stats))
    app.add_handler(CommandHandler("board", broadcast_command))
    app.add_handler(CommandHandler("stats_ref", stats_ref_command))
    app.add_handler(CommandHandler("help", admin_help))
    app.add_handler(CommandHandler("stat", subgram_stats))
    app.add_handler(CommandHandler("add_ch", add_channel_command))
    app.add_handler(CommandHandler("list", list_channels))
    app.add_handler(CommandHandler("del", delete_channel))
    app.add_handler(CommandHandler("work", add_required_channel))
    app.add_handler(CommandHandler("wlist", list_required_channels))
    app.add_handler(CommandHandler("delw", delete_required_channel))

    app.add_handler(MessageHandler(filters.Regex('^Приступить к заданию$'), show_tasks))
    app.add_handler(MessageHandler(filters.Regex('^Личный кабинет$'), cabinet_handler))
    app.add_handler(MessageHandler(filters.Regex('^Реферальная программа$'), referral_handler))
    app.add_handler(MessageHandler(filters.Regex('^Помощь$'), help_handler))

    app.add_handler(CallbackQueryHandler(show_tasks, pattern="^start_tasks$"))
    app.add_handler(CallbackQueryHandler(verify_all_tasks, pattern="^verify_all_tasks$"))
    app.add_handler(CallbackQueryHandler(cabinet_handler, pattern="^cabinet$"))
    app.add_handler(CallbackQueryHandler(referral_handler, pattern="^referral$"))
    app.add_handler(CallbackQueryHandler(withdraw_history, pattern="^withdraw_history$"))
    app.add_handler(CallbackQueryHandler(handle_withdraw_action, pattern="^withdraw_(accept|reject)_"))
    app.add_handler(CallbackQueryHandler(copy_referral_link, pattern="^copy_ref_"))
    app.add_handler(CallbackQueryHandler(check_required_callback, pattern="^check_required$"))
    app.add_handler(CallbackQueryHandler(confirm_broadcast, pattern="^confirm_broadcast$"))
    app.add_handler(CallbackQueryHandler(lambda u, c: u.edit_message_text("Отменено."), pattern="^cancel_broadcast$"))

    return app
