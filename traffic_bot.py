import asyncio
import logging
import secrets
import warnings
from pathlib import Path

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import NetworkError
from telegram.warnings import PTBUserWarning
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from api_handlers import cryptobot_check_invoices, cryptobot_create_invoice, cryptobot_transfer_rub
from config import *
from database import DatabaseManager

logger = logging.getLogger(__name__)
DOCS_PATH = Path(__file__).resolve().parent / "docs" / "NEO_LINK_INTEGRATION.md"
BOT_PUBLIC_URL = "https://t.me/NeoLink_sellrobot"

warnings.filterwarnings("ignore", category=PTBUserWarning)


def get_traffic_main_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Купить подписчиков", callback_data="tb_buy")],
        [InlineKeyboardButton("🧩 Продажа трафика", callback_data="tb_sell")],
        [InlineKeyboardButton("📦 Мои заказы", callback_data="tb_orders"),
         InlineKeyboardButton("💰 Баланс", callback_data="tb_balance")],
        [InlineKeyboardButton("👤 Профиль", callback_data="tb_profile"),
         InlineKeyboardButton("🛠 Тех поддержка", url="https://t.me/Neosupports_bot")],
    ])


def get_sell_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить нового бота", callback_data="tb_sell_add")],
        [InlineKeyboardButton("🤖 Мои боты", callback_data="tb_sell_list")],
        [InlineKeyboardButton("📚 Документация", callback_data="tb_sell_docs")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="tb_back_main")],
    ])


def get_docs_deeplink():
    return "https://neolink12.github.io/neolink-docs/"


def get_sell_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить нового бота", callback_data="tb_sell_add")],
        [InlineKeyboardButton("🤖 Мои боты", callback_data="tb_sell_list")],
        [InlineKeyboardButton("📚 Документация", url=get_docs_deeplink())],
        [InlineKeyboardButton("⬅️ Назад", callback_data="tb_back_main")],
    ])


def get_status_text(status: str) -> str:
    return {
        "pending": "🟡 На модерации",
        "approved": "🟢 Допущено",
        "rejected": "🔴 Отклонено",
    }.get(status, status)


def generate_api_key() -> str:
    return secrets.token_hex(32)


async def validate_bot_token(bot_token: str):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://api.telegram.org/bot{bot_token}/getMe", timeout=10) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    return None
                result = data["result"]
                return {
                    "bot_id": result["id"],
                    "bot_username": result.get("username"),
                    "bot_name": result.get("first_name") or result.get("username") or "Telegram Bot",
                }
    except Exception as exc:
        logger.warning(f"Failed to validate bot token: {exc}")
        return None


def build_sell_bot_card(bot_row) -> str:
    _, bot_name, bot_username, theme, status, rejection_reason, sold_count, charged_total = bot_row
    text = (
        f"{bot_name or 'Без названия'} (@{bot_username or 'unknown'})\n\n"
        f"Тематика: {theme or 'Другое'}\n"
        f"Статус: {get_status_text(status)}\n\n"
        f"Продано подписок: {sold_count}\n"
        f"Заработано: {charged_total:.2f} ₽"
    )
    if status == "rejected" and rejection_reason:
        text += f"\nПричина: {rejection_reason}"
    return text


def build_sell_bot_button(bot_row) -> str:
    _, bot_name, bot_username, *_rest = bot_row
    title = f"(@{bot_username or 'unknown'}) {bot_name or 'Без названия'}"
    return title[:60]


async def notify_admin_about_new_bot(context: ContextTypes.DEFAULT_TYPE, bot_row_id: int):
    bot_data = DatabaseManager.get_traffic_bot(bot_row_id)
    if not bot_data:
        return
    (
        _,
        owner_user_id,
        bot_id,
        bot_username,
        bot_name,
        _bot_token,
        api_key,
        theme,
        status,
        _reason,
        *_rest,
    ) = bot_data
    text = (
        "🆕 <b>Новая заявка на модерацию Neo Link</b>\n\n"
        f"Бот: {bot_name} (@{bot_username or 'unknown'})\n"
        f"Owner ID: <code>{owner_user_id}</code>\n"
        f"Bot ID: <code>{bot_id}</code>\n"
        f"Тематика: {theme}\n"
        f"Статус: {get_status_text(status)}\n"
        f"API key: <code>{api_key}</code>"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Одобрить", callback_data=f"tb_mod_approve_{bot_row_id}"),
         InlineKeyboardButton("❌ Отклонить", callback_data=f"tb_mod_reject_{bot_row_id}")]
    ])
    try:
        await context.bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    except Exception as exc:
        logger.warning(f"Failed to notify admin about bot moderation: {exc}")


async def send_integration_docs(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    messages = [
        (
            "📚 <b>Документация Neo Link</b>\n\n"
            "1. В разделе <b>Продажа трафика</b> добавьте своего бота по токену.\n"
            "2. После проверки получите <b>API ключ</b>.\n"
            "3. Встройте в своего бота запросы к Neo Link.\n"
            "4. Передавайте события подписки и запускайте проверку через Telegram API.\n"
            "5. Если пользователь отписался, Neo Link спишет сумму автоматически."
        ),
        (
            "🔌 <b>Что должен делать ваш бот</b>\n\n"
            "• получать список спонсоров\n"
            "• проверять подписку через <code>getChatMember</code>\n"
            "• регистрировать подтверждённую подписку\n"
            "• обрабатывать ошибки Telegram API и таймауты\n\n"
            "Пример клиента для aiogram подготовлен в проекте."
        ),
        (
            "💡 <b>Куда смотреть разработчику</b>\n\n"
            f"Локальная документация: <code>{DOCS_PATH.name}</code>\n"
            "Пример кода: <code>docs/neolink_aiogram_example.py</code>\n\n"
            "Если нужно, могу дополнительно развернуть это как полноценную web-страницу документации."
        ),
    ]
    for text in messages:
        await context.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML)


async def traffic_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    referrer_id = None
    if context.args:
        if context.args[0] == "docs":
            DatabaseManager.create_traffic_user(user.id, user.username, user.first_name, None)
            await send_integration_docs(update.effective_chat.id, context)
            return
        try:
            referrer_id = int(context.args[0].replace("ref", ""))
        except ValueError:
            pass
    DatabaseManager.create_traffic_user(user.id, user.username, user.first_name, referrer_id)
    text = (
        "🚀 <b>Neo Link — сервис покупки и продажи мотивированного трафика</b>\n\n"
        "Вы можете покупать подписчиков, управлять заказами, подключать свои Telegram-боты к продаже трафика "
        "и отслеживать списания за отписки.\n\n"
        "Выберите действие ниже."
    )
    await update.message.reply_text(text, reply_markup=get_traffic_main_keyboard(), parse_mode=ParseMode.HTML)


async def traffic_back_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    text = (
        "🚀 <b>Neo Link</b>\n\n"
        "Покупка и продажа мотивированного трафика в Telegram.\n\n"
        "Выберите действие ниже."
    )
    await query.edit_message_text(text, reply_markup=get_traffic_main_keyboard(), parse_mode=ParseMode.HTML)


async def traffic_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Канал / Чат", callback_data="tb_buy_channel")],
        [InlineKeyboardButton("🌐 Ресурс (без проверки)", callback_data="tb_buy_resource")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="tb_back_main")],
    ])
    await query.edit_message_text(
        "🚀 <b>Покупка подписчиков</b>\n\nВыберите тип продвижения.",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )


async def traffic_buy_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["tb_order_type"] = "channel"
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_buy")]])
    await query.edit_message_text(
        "📢 <b>Продвижение канала или чата</b>\n\n"
        f"Сначала добавьте бота <b>@{CHECKSUB_BOT_USERNAME}</b> в администраторы канала.\n\n"
        "После этого отправьте ссылку/юзернейм канала либо перешлите любое сообщение из приватного канала.",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )
    return TRAFFIC_CHANNEL_LINK


async def traffic_channel_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    chat_id = None
    username = None
    link = None

    if getattr(message, "forward_from_chat", None):
        chat = message.forward_from_chat
        chat_id = str(chat.id)
        username = chat.username
    elif getattr(message, "forward_origin", None):
        origin = message.forward_origin
        chat = getattr(origin, "chat", None) or getattr(origin, "sender_chat", None)
        if chat:
            chat_id = str(chat.id)
            username = getattr(chat, "username", None)
    elif message.text:
        text = message.text.strip()
        if text.startswith("@"):
            username = text[1:]
            chat_id = f"@{username}"
            link = f"https://t.me/{username}"
        elif text.startswith("https://t.me/"):
            username = text.rstrip("/").split("/")[-1].replace("@", "")
            chat_id = f"@{username}"
            link = text

    if not chat_id:
        await message.reply_text("❌ Отправьте ссылку, @username или пересланный пост из канала.")
        return TRAFFIC_CHANNEL_LINK

    progress = await message.reply_text("⏳ Проверяю права checksub-бота в канале...", parse_mode=ParseMode.HTML)
    try:
        checksub_bot_id = int(CHECKSUB_BOT_TOKEN.split(":")[0])
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://api.telegram.org/bot{CHECKSUB_BOT_TOKEN}/getChatMember?chat_id={chat_id}&user_id={checksub_bot_id}",
                timeout=10,
            ) as resp:
                data = await resp.json()
        if not data.get("ok"):
            await progress.edit_text(
                f"❌ Бот @{CHECKSUB_BOT_USERNAME} не найден в канале. Добавьте его администратором и повторите попытку.",
                parse_mode=ParseMode.HTML,
            )
            return TRAFFIC_CHANNEL_LINK
        status = data["result"]["status"]
        if status not in {"administrator", "creator"}:
            await progress.edit_text(
                f"❌ Бот @{CHECKSUB_BOT_USERNAME} должен быть администратором канала.",
                parse_mode=ParseMode.HTML,
            )
            return TRAFFIC_CHANNEL_LINK
    except Exception as exc:
        logger.warning(f"Channel validation error: {exc}")
        await progress.edit_text("❌ Не удалось проверить канал. Попробуйте ещё раз позже.", parse_mode=ParseMode.HTML)
        return TRAFFIC_CHANNEL_LINK

    if str(chat_id).startswith("@"):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://api.telegram.org/bot{CHECKSUB_BOT_TOKEN}/getChat?chat_id={chat_id}",
                    timeout=10,
                ) as resp:
                    chat_data = await resp.json()
            if chat_data.get("ok"):
                chat_id = str(chat_data["result"]["id"])
        except Exception:
            pass

    invite_link = None
    if chat_id and not username:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://api.telegram.org/bot{CHECKSUB_BOT_TOKEN}/exportChatInviteLink?chat_id={chat_id}",
                    timeout=10,
                ) as resp:
                    invite_data = await resp.json()
            if invite_data.get("ok"):
                invite_link = invite_data.get("result")
                if invite_link and not link:
                    link = invite_link
        except Exception as exc:
            logger.warning(f"Failed to export invite link for {chat_id}: {exc}")

    context.user_data["tb_channel_id"] = chat_id
    context.user_data["tb_channel_username"] = username
    context.user_data["tb_is_private"] = not bool(username)
    context.user_data["tb_link"] = link
    context.user_data["tb_invite_link"] = invite_link

    await progress.edit_text(
        "✅ <b>Канал подтверждён.</b>\n\nВведите, сколько подписчиков нужно. Минимум: 10.",
        parse_mode=ParseMode.HTML,
    )
    return TRAFFIC_CHANNEL_COUNT


async def traffic_channel_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        count = int(update.message.text.strip().replace(" ", "").replace(",", ""))
        if count < 10 or count > 10_000_000:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Введите целое число от 10 до 10 000 000.")
        return TRAFFIC_CHANNEL_COUNT

    context.user_data["tb_count"] = count
    price = round(count * PRICE_WITH_CHECK, 2)
    context.user_data["tb_price"] = price
    username = context.user_data.get("tb_channel_username")
    channel_display = f"@{username}" if username else context.user_data.get("tb_link", "Приватный канал")
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Оплатить", callback_data="tb_pay_order")],
        [InlineKeyboardButton("❌ Отмена", callback_data="tb_back_main")],
    ])
    await update.message.reply_text(
        f"📊 <b>Параметры заказа</b>\n\n"
        f"📢 Канал: {channel_display}\n"
        f"👥 Подписчиков: <b>{count:,}</b>\n"
        f"💰 Стоимость: <b>{price:.2f} ₽</b>",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )
    return ConversationHandler.END


async def traffic_buy_resource(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["tb_order_type"] = "resource"
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_buy")]])
    await query.edit_message_text(
        "🌐 <b>Продвижение ресурса</b>\n\n"
        "Отправьте ссылку на сайт или внешний ресурс, начиная с <code>https://</code>.",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )
    return TRAFFIC_RESOURCE_LINK


async def traffic_resource_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    link = update.message.text.strip()
    if not link.startswith("http"):
        await update.message.reply_text("❌ Ссылка должна начинаться с http:// или https://")
        return TRAFFIC_RESOURCE_LINK
    context.user_data["tb_link"] = link
    await update.message.reply_text(
        "ℹ️ <b>Для внешних ресурсов переход считается предположительным.</b>\n\n"
        "Введите количество переходов. Минимум: 10.",
        parse_mode=ParseMode.HTML,
    )
    return TRAFFIC_RESOURCE_COUNT


async def traffic_resource_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        count = int(update.message.text.strip().replace(" ", "").replace(",", ""))
        if count < 10 or count > 10_000_000:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Введите целое число от 10 до 10 000 000.")
        return TRAFFIC_RESOURCE_COUNT

    context.user_data["tb_count"] = count
    context.user_data["tb_channel_id"] = None
    context.user_data["tb_price"] = round(count * PRICE_NO_CHECK, 2)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Оплатить", callback_data="tb_pay_order")],
        [InlineKeyboardButton("❌ Отмена", callback_data="tb_back_main")],
    ])
    await update.message.reply_text(
        f"📊 <b>Параметры заказа</b>\n\n"
        f"🌐 Ресурс: {context.user_data['tb_link']}\n"
        f"👥 Переходов: <b>{count:,}</b>\n"
        f"💰 Стоимость: <b>{context.user_data['tb_price']:.2f} ₽</b>",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )
    return ConversationHandler.END


async def traffic_pay_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    link = context.user_data.get("tb_link")
    count = context.user_data.get("tb_count", 0)
    price = context.user_data.get("tb_price", 0)
    channel_id = context.user_data.get("tb_channel_id")
    invite_link = context.user_data.get("tb_invite_link")
    order_type = context.user_data.get("tb_order_type", "channel")

    balance_row = DatabaseManager.execute_query(
        "SELECT balance FROM traffic_users WHERE user_id = ?",
        (user_id,),
        "one",
    )
    balance = balance_row[0] if balance_row else 0
    if balance < price:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Пополнить баланс", callback_data="tb_topup")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="tb_back_main")],
        ])
        await query.edit_message_text(
            f"❌ <b>Недостаточно средств</b>\n\n"
            f"Нужно: {price:.2f} ₽\nВаш баланс: {balance:.2f} ₽\nНе хватает: {price - balance:.2f} ₽",
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML,
        )
        return

    debit_result = DatabaseManager.debit_traffic_user_balance(
        user_id,
        price,
        f"Order {order_type}: {link}",
        "order",
    )
    if not debit_result or not debit_result.get("ok"):
        await query.edit_message_text(
            "❌ Не удалось списать средства. Проверьте баланс и попробуйте снова.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")]]),
            parse_mode=ParseMode.HTML,
        )
        return
    DatabaseManager.execute_query(
        "INSERT INTO orders (user_id, link, order_type, amount, price, channel_id, invite_link) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (user_id, link, order_type, count, price, channel_id, invite_link),
    )
    DatabaseManager.execute_query(
        "SELECT ?, ?, ?, ?",
        (user_id, -price, "order", f"Заказ {order_type}: {link}"),
    )
    order_row = DatabaseManager.execute_query(
        "SELECT id FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT 1",
        (user_id,),
        "one",
    )
    order_id = order_row[0] if order_row else "?"
    await query.edit_message_text(
        f"✅ <b>Заказ #{order_id} создан</b>\n\n"
        f"Ресурс: {link}\n"
        f"Объём: {count:,}\n"
        f"Списано: {price:.2f} ₽",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📦 Мои заказы", callback_data="tb_orders")]]),
    )


async def traffic_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    orders = DatabaseManager.execute_query(
        "SELECT id, link, amount, done, price, status, created FROM orders WHERE user_id = ? ORDER BY created DESC LIMIT 10",
        (user_id,),
        "all",
    ) or []
    if not orders:
        await query.edit_message_text(
            "📦 <b>У вас пока нет заказов</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_back_main")]]),
        )
        return

    status_map = {"active": "🔄 Выполняется", "completed": "✅ Выполнен", "paused": "⏸ Пауза"}
    text = "📦 <b>Ваши заказы</b>\n\n"
    for order_id, link, amount, done, price, status, created in orders:
        short_link = link[:36] + "..." if len(link) > 36 else link
        text += (
            f"<b>#{order_id}</b>\n"
            f"🔗 {short_link}\n"
            f"👥 Заказано: {amount:,} | Выполнено: {done:,}\n"
            f"💰 {price:.2f} ₽ | {status_map.get(status, status)}\n"
            f"📅 {created[:16]}\n\n"
        )
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить", callback_data="tb_orders")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="tb_back_main")],
        ]),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def traffic_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    row = DatabaseManager.get_traffic_user_finances(user_id)
    balance, deposit_balance, earned_balance, total_spent, total_earned, total_withdrawn, _last_cryptobot_user_id = (
        row if row else (0, 0, 0, 0, 0, 0, None)
    )
    total_income = total_earned
    text = (
        "Финансы:\n\n"
        "📈 Доходы\n"
        f"Всего заработано: {total_income:.2f} ₽\n\n"
        "📉 Расходы\n"
        f"Всего потрачено: {total_spent:.2f} ₽\n"
        f"Всего выведено: {total_withdrawn:.2f} ₽\n\n"
        "💳 Состояние счета\n"
        f"Текущий баланс: {balance:.2f} ₽\n"
        f"Пополнения: {deposit_balance:.2f} ₽\n"
        f"Заработок интеграции: {earned_balance:.2f} ₽\n\n"
        "💸 Вывод средств\n"
        f"Доступно: {earned_balance:.2f} ₽\n"
        "К выводу доступны только деньги, заработанные через интеграцию."
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить баланс", callback_data="tb_balance")],
            [InlineKeyboardButton("➕ Пополнить баланс", callback_data="tb_topup")],
            [InlineKeyboardButton("➖ Вывести средства", callback_data="tb_withdraw_start")],
            [InlineKeyboardButton("↔️ Перевести средства", callback_data="tb_transfer_placeholder")],
            [InlineKeyboardButton("📄 Мои транзакции", callback_data="tb_transactions")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="tb_back_main")],
        ]),
    )


async def traffic_transactions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    transactions = DatabaseManager.execute_query(
        "SELECT amount, description, date FROM transactions WHERE user_id = ? ORDER BY date DESC LIMIT 15",
        (user_id,),
        "all",
    ) or []
    if not transactions:
        await query.answer("Транзакций пока нет", show_alert=True)
        return
    text = "📄 <b>Мои транзакции</b>\n\n"
    for amount, description, date in transactions:
        sign = "+" if amount > 0 else ""
        text += f"{sign}{amount:.2f} ₽ — {description}\n{date[:16]}\n\n"
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")]]),
    )


async def traffic_topup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "💳 <b>Пополнение баланса</b>\n\nВыберите сумму или введите свою.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("100 ₽", callback_data="tb_topup_100"),
             InlineKeyboardButton("300 ₽", callback_data="tb_topup_300"),
             InlineKeyboardButton("500 ₽", callback_data="tb_topup_500")],
            [InlineKeyboardButton("1000 ₽", callback_data="tb_topup_1000"),
             InlineKeyboardButton("5000 ₽", callback_data="tb_topup_5000")],
            [InlineKeyboardButton("💸 Другая сумма", callback_data="tb_topup_other")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")],
        ]),
    )


async def create_invoice_message(query, amount: float):
    invoice = await cryptobot_create_invoice(amount, query.from_user.id)
    if not invoice:
        await query.edit_message_text("❌ Не удалось создать счёт. Попробуйте позже.")
        return ConversationHandler.END
    amount_with_fee = round(amount * 1.03, 2)
    await query.edit_message_text(
        f"💳 <b>Счёт на оплату</b>\n\n"
        f"Сумма: {amount:.2f} ₽\n"
        f"Комиссия: {amount_with_fee - amount:.2f} ₽\n"
        f"Итого: {amount_with_fee:.2f} ₽",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Оплатить", url=invoice["bot_invoice_url"])],
            [InlineKeyboardButton("🔄 Проверить оплату", callback_data="tb_check_payment")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")],
        ]),
    )
    return ConversationHandler.END


async def traffic_topup_amount_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    amount_key = query.data.split("_", 2)[2]
    if amount_key == "other":
        await query.edit_message_text(
            "💳 <b>Введите сумму пополнения</b>\n\nМинимум: 50 ₽",
            parse_mode=ParseMode.HTML,
        )
        return TRAFFIC_TOPUP_AMOUNT
    return await create_invoice_message(query, int(amount_key))


async def traffic_topup_amount_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        amount = float(update.message.text.strip().replace(",", ".").replace(" ", ""))
        if amount < 50:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Введите сумму от 50 ₽.")
        return TRAFFIC_TOPUP_AMOUNT

    invoice = await cryptobot_create_invoice(amount, update.effective_user.id)
    if not invoice:
        await update.message.reply_text("❌ Не удалось создать счёт. Попробуйте позже.")
        return ConversationHandler.END
    amount_with_fee = round(amount * 1.03, 2)
    await update.message.reply_text(
        f"💳 <b>Счёт на оплату</b>\n\n"
        f"Сумма: {amount:.2f} ₽\n"
        f"Комиссия: {amount_with_fee - amount:.2f} ₽\n"
        f"Итого: {amount_with_fee:.2f} ₽",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Оплатить", url=invoice["bot_invoice_url"])],
            [InlineKeyboardButton("🔄 Проверить оплату", callback_data="tb_check_payment")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")],
        ]),
    )
    return ConversationHandler.END


async def traffic_check_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    message = await query.edit_message_text("🔄 Проверяю оплаченные счета...", parse_mode=ParseMode.HTML)
    result = await cryptobot_check_invoices(manual=True)
    balance_row = DatabaseManager.execute_query(
        "SELECT balance FROM traffic_users WHERE user_id = ?",
        (query.from_user.id,),
        "one",
    )
    balance = balance_row[0] if balance_row else 0
    if result and result.get("ok"):
        updated = result.get("updated", 0)
        await message.edit_text(
            f"✅ Проверка завершена.\n\nНайдено новых оплат: {updated}\nБаланс: {balance:.2f} ₽",
            parse_mode=ParseMode.HTML,
        )
    else:
        error = result.get("error", "unknown error") if result else "unknown error"
        await message.edit_text(f"❌ Ошибка проверки оплаты: {error}", parse_mode=ParseMode.HTML)


async def traffic_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user = DatabaseManager.execute_query(
        "SELECT balance, total_spent, ref_count FROM traffic_users WHERE user_id = ?",
        (user_id,),
        "one",
    )
    balance, total_spent, ref_count = user if user else (0, 0, 0)
    orders_count = DatabaseManager.execute_query("SELECT COUNT(*) FROM orders WHERE user_id = ?", (user_id,), "one")[0]
    total_ordered = DatabaseManager.execute_query("SELECT SUM(amount) FROM orders WHERE user_id = ?", (user_id,), "one")[0] or 0
    username = query.from_user.username or query.from_user.first_name
    text = (
        "👤 <b>Ваш профиль</b>\n\n"
        f"ID: <code>{user_id}</code>\n"
        f"Пользователь: @{username}\n\n"
        f"Заказов создано: {orders_count}\n"
        f"Всего заказано: {int(total_ordered):,}\n"
        f"Рефералов: {ref_count}\n"
        f"Потрачено: {total_spent:.2f} ₽\n"
        f"Баланс: <b>{balance:.2f} ₽</b>"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 Реферальная программа", callback_data="tb_referral")],
            [InlineKeyboardButton("🤖 Мои боты", callback_data="tb_sell_list")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="tb_back_main")],
        ]),
    )


async def traffic_referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    await query.edit_message_text(
        f"👥 <b>Реферальная программа</b>\n\n"
        f"Ваша ссылка:\n<code>https://t.me/NeoLink_traffic_bot?start=ref{user_id}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_profile")]]),
    )


async def traffic_sell_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await traffic_sell_list(update, context)


async def traffic_sell_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    bots = DatabaseManager.list_traffic_bots(query.from_user.id)
    if not bots:
        await query.edit_message_text(
            "Выберите нужный пункт 👇",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ Добавить нового бота", callback_data="tb_sell_add")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="tb_back_main")],
            ]),
        )
        return
    text = "Выбери нужный пункт 👇"
    keyboard = [[InlineKeyboardButton("➕ Добавить нового бота", callback_data="tb_sell_add")]]
    for row in bots:
        bot_id = row[0]
        keyboard.append([InlineKeyboardButton(build_sell_bot_button(row), callback_data=f"tb_sell_bot_{bot_id}")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="tb_back_main")])
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))


async def traffic_sell_open_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    bot_row_id = int(query.data.rsplit("_", 1)[1])
    bot_data = DatabaseManager.get_traffic_bot(bot_row_id)
    if not bot_data or bot_data[1] != query.from_user.id:
        await query.answer("Бот не найден", show_alert=True)
        return
    (
        _,
        _owner_user_id,
        _bot_id,
        bot_username,
        bot_name,
        _bot_token,
        api_key,
        theme,
        status,
        is_enabled,
        rejection_reason,
        display_bots,
        display_resources,
        max_sponsors,
        _reset_hours,
        _new_sponsors_after_hours,
        price_per_subscription,
        *_rest,
    ) = bot_data
    sold_count = DatabaseManager.execute_query(
        "SELECT COUNT(*) FROM traffic_bot_subscriptions WHERE traffic_bot_id = ?",
        (bot_row_id,),
        "one",
    )[0]
    charged_total = DatabaseManager.execute_query(
        "SELECT SUM(charge_amount) FROM traffic_bot_subscriptions WHERE traffic_bot_id = ? AND rewarded = 1",
        (bot_row_id,),
        "one",
    )[0] or 0
    text = (
        f"<b>{bot_name}</b> (@{bot_username or 'unknown'})\n"
        f"<i>Тематика:</i> {theme}\n"
        f"<i>Статус:</i> {'⏸ Остановлен' if status == 'approved' and not is_enabled else get_status_text(status)}\n\n"
        f"👥 Продано подписок всего: {sold_count}\n"
        f"💳 Заработано: {charged_total:.2f} ₽\n"
        f"💰 Доход владельца за 1 подписку: {price_per_subscription:.2f} ₽"
    )
    if status == "rejected" and rejection_reason:
        text += f"\nПричина отклонения: {rejection_reason}"
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить статистику", callback_data=f"tb_sell_bot_{bot_row_id}")],
            [InlineKeyboardButton("⚙️ Настройки", callback_data=f"tb_sell_settings_{bot_row_id}"),
             InlineKeyboardButton("📊 Статистика", callback_data=f"tb_sell_stats_{bot_row_id}")],
            [InlineKeyboardButton("💻 Интеграция (API)", url=get_docs_deeplink()),
             InlineKeyboardButton("↔️ Передать бота", callback_data=f"tb_sell_transfer_{bot_row_id}")],
            [InlineKeyboardButton("▶️ Включить" if not is_enabled else "⏸ Остановить", callback_data=f"tb_sell_toggle_status_{bot_row_id}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="tb_sell_list")],
        ]),
    )


async def traffic_sell_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "Выберите способ подключения бота к сервису 👇\n\n"
        "Сейчас доступен способ: <b>с токеном</b>\n\n"
        "Отправьте токен бота.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_sell")]]),
    )
    return TRAFFIC_SELL_TOKEN


async def traffic_sell_add_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_token = update.message.text.strip()
    existing = DatabaseManager.get_traffic_bot_by_token(bot_token)
    if existing:
        await update.message.reply_text("❌ Этот токен уже добавлен в систему.")
        return TRAFFIC_SELL_TOKEN

    bot_info = await validate_bot_token(bot_token)
    if not bot_info:
        await update.message.reply_text("❌ Токен невалиден или Telegram API временно недоступен.")
        return TRAFFIC_SELL_TOKEN

    api_key = generate_api_key()
    DatabaseManager.create_traffic_bot(
        update.effective_user.id,
        bot_info["bot_id"],
        bot_info["bot_username"],
        bot_info["bot_name"],
        bot_token,
        api_key,
    )
    bot_row = DatabaseManager.execute_query(
        "SELECT id FROM traffic_bots WHERE owner_user_id = ? ORDER BY id DESC LIMIT 1",
        (update.effective_user.id,),
        "one",
    )
    bot_row_id = bot_row[0]
    DatabaseManager.add_traffic_bot_log(bot_row_id, "info", "bot_added", "Bot added and sent to moderation")
    await notify_admin_about_new_bot(context, bot_row_id)

    await update.message.reply_text(
        "✅ <b>Успешно добавлено в систему!</b>\n\n"
        f"Ваш API ключ от бота:\n<code>{api_key}</code>\n\n"
        "Теперь необходимо выполнить интеграцию с сервисом. Откройте документацию по кнопке ниже.\n\n"
        "После этого бот будет находиться на модерации.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📚 Открыть документацию", url=get_docs_deeplink())],
            [InlineKeyboardButton("⚙️ Приступить к настройке", callback_data=f"tb_sell_settings_{bot_row_id}")],
            [InlineKeyboardButton("Пропустить", callback_data="tb_sell_list")],
        ]),
    )
    return ConversationHandler.END


async def traffic_sell_docs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    bot_row_id = None
    if query.data.startswith("tb_sell_docs_"):
        bot_row_id = int(query.data.rsplit("_", 1)[1])
    docs_hint = (
        "📚 <b>Документация по интеграции</b>\n\n"
        "Нажмите кнопку ниже, и бот откроет документацию прямо в Telegram через отдельную ссылку.\n\n"
        "Там будет пошаговое подключение, логика работы, пример aiogram и обработка ошибок."
    )
    keyboard = [
        [InlineKeyboardButton("📚 Читать документацию", url=get_docs_deeplink())],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"tb_sell_bot_{bot_row_id}" if bot_row_id else "tb_sell")],
    ]
    await query.edit_message_text(docs_hint, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))


async def traffic_sell_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    bot_row_id = int(query.data.rsplit("_", 1)[1])
    bot_data = DatabaseManager.get_traffic_bot(bot_row_id)
    if not bot_data or bot_data[1] != query.from_user.id:
        await query.answer("Бот не найден", show_alert=True)
        return
    context.user_data["sell_bot_id"] = bot_row_id
    (
        _,
        _owner_user_id,
        _bot_id,
        bot_username,
        bot_name,
        _bot_token,
        _api_key,
        _theme,
        _status,
        _is_enabled,
        _rejection_reason,
        display_bots,
        display_resources,
        max_sponsors,
        _reset_hours,
        _new_sponsors_after_hours,
        _price_per_subscription,
        _anti_scam_enabled,
        _suspicious_limit,
    ) = bot_data
    text = (
        f"⚙️ <b>Настройки бота</b>\n\n"
        f"{bot_name} (@{bot_username or 'unknown'})\n\n"
        f"Боты: {'Да' if display_bots else 'Нет'}\n"
        f"Ресурсы: {'Да' if display_resources else 'Нет'}\n"
        f"Макс. спонсоров: {max_sponsors}"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 Актуализировать токен", callback_data=f"tb_sell_set_token_{bot_row_id}")],
            [InlineKeyboardButton(f"Боты: {'Да' if display_bots else 'Нет'}", callback_data=f"tb_sell_toggle_bots_{bot_row_id}")],
            [InlineKeyboardButton(f"Ресурсы: {'Да' if display_resources else 'Нет'}", callback_data=f"tb_sell_toggle_resources_{bot_row_id}")],
            [InlineKeyboardButton("📊 Макс. количество спонсоров", callback_data=f"tb_sell_set_max_{bot_row_id}")],
            [InlineKeyboardButton("📚 Документация", url=get_docs_deeplink())],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"tb_sell_bot_{bot_row_id}")],
        ]),
    )


async def traffic_sell_toggle_setting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = query.data.split("_")
    bot_row_id = int(parts[-1])
    setting_type = parts[3]
    bot_data = DatabaseManager.get_traffic_bot(bot_row_id)
    if not bot_data or bot_data[1] != query.from_user.id:
        await query.answer("Бот не найден", show_alert=True)
        return
    if setting_type == "bots":
        new_value = 0 if bot_data[10] else 1
        DatabaseManager.update_traffic_bot_settings(bot_row_id, display_bots=new_value)
    elif setting_type == "resources":
        new_value = 0 if bot_data[11] else 1
        DatabaseManager.update_traffic_bot_settings(bot_row_id, display_resources=new_value)
    await traffic_sell_settings(update, context)


async def traffic_sell_token_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["sell_bot_id"] = int(query.data.rsplit("_", 1)[1])
    await query.edit_message_text(
        "🔑 <b>Введите новое значение токена бота</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"tb_sell_settings_{context.user_data['sell_bot_id']}")]]),
    )
    return TRAFFIC_SETTINGS_TOKEN


async def traffic_sell_update_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_row_id = context.user_data.get("sell_bot_id")
    if not bot_row_id:
        return ConversationHandler.END
    bot_data = DatabaseManager.get_traffic_bot(bot_row_id)
    if not bot_data or bot_data[1] != update.effective_user.id:
        await update.message.reply_text("❌ Бот не найден.")
        return ConversationHandler.END
    token = update.message.text.strip()
    bot_info = await validate_bot_token(token)
    if not bot_info:
        await update.message.reply_text("❌ Токен невалиден.")
        return TRAFFIC_SETTINGS_TOKEN
    DatabaseManager.update_traffic_bot_token(
        bot_row_id,
        token,
        bot_info["bot_id"],
        bot_info["bot_username"],
        bot_info["bot_name"],
    )
    await update.message.reply_text(
        "✅ Токен обновлён.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В настройки", callback_data=f"tb_sell_settings_{bot_row_id}")]]),
    )
    return ConversationHandler.END


async def traffic_sell_max_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["sell_bot_id"] = int(query.data.rsplit("_", 1)[1])
    await query.edit_message_text("Введите максимальное количество спонсоров: от 1 до 10.")
    return TRAFFIC_SETTINGS_MAX_SPONSORS


async def traffic_sell_set_max(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        value = int(update.message.text.strip())
        if value < 1 or value > 10:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Введите число от 1 до 10.")
        return TRAFFIC_SETTINGS_MAX_SPONSORS
    DatabaseManager.update_traffic_bot_settings(context.user_data["sell_bot_id"], max_sponsors=value)
    await update.message.reply_text(
        "✅ Максимальное количество спонсоров обновлено.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В настройки", callback_data=f"tb_sell_settings_{context.user_data['sell_bot_id']}")]]),
    )
    return ConversationHandler.END


async def handle_moderation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        await query.answer("Недостаточно прав", show_alert=True)
        return
    bot_row_id = int(query.data.rsplit("_", 1)[1])
    bot_data = DatabaseManager.get_traffic_bot(bot_row_id)
    if not bot_data:
        await query.edit_message_text("Бот уже удалён или не найден.")
        return
    action = "approve" if "_approve_" in query.data else "reject"
    if action == "approve":
        DatabaseManager.set_traffic_bot_status(bot_row_id, "approved")
        text = "✅ Бот одобрен и допущен к работе."
        notify_text = f"✅ Ваш бот @{bot_data[3] or 'unknown'} прошёл модерацию и допущен к продаже трафика."
    else:
        reason = "Бот отклонён системой безопасности"
        DatabaseManager.set_traffic_bot_status(bot_row_id, "rejected", reason)
        text = f"❌ Бот отклонён.\nПричина: {reason}"
        notify_text = f"❌ Ваш бот @{bot_data[3] or 'unknown'} отклонён.\nПричина: {reason}"
    try:
        await context.bot.send_message(bot_data[1], notify_text, parse_mode=ParseMode.HTML)
    except Exception as exc:
        logger.warning(f"Failed to notify bot owner after moderation: {exc}")
    await query.edit_message_text(text, parse_mode=ParseMode.HTML)


async def traffic_sell_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    bot_row_id = int(query.data.rsplit("_", 1)[1])
    bot_data = DatabaseManager.get_traffic_bot(bot_row_id)
    if not bot_data or bot_data[1] != query.from_user.id:
        await query.answer("Бот не найден", show_alert=True)
        return
    sold_count = DatabaseManager.execute_query(
        "SELECT COUNT(*) FROM traffic_bot_subscriptions WHERE traffic_bot_id = ?",
        (bot_row_id,),
        "one",
    )[0]
    unsub_count = DatabaseManager.execute_query(
        "SELECT COUNT(*) FROM traffic_bot_subscriptions WHERE traffic_bot_id = ? AND charged = 1",
        (bot_row_id,),
        "one",
    )[0]
    earned = DatabaseManager.execute_query(
        "SELECT SUM(charge_amount) FROM traffic_bot_subscriptions WHERE traffic_bot_id = ? AND rewarded = 1",
        (bot_row_id,),
        "one",
    )[0] or 0
    text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"Всего подписок: {sold_count}\n"
        f"Зафиксировано отписок: {unsub_count}\n"
        f"Начислено владельцу: {earned:.2f} ₽"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"tb_sell_bot_{bot_row_id}")]]),
    )


async def traffic_sell_placeholder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    bot_row_id = int(query.data.rsplit("_", 1)[1])
    if "_transfer_" in query.data:
        text = (
            "Введите user id пользователя, на которого требуется передать бота.\n\n"
            "⚠️ Пользователь должен быть авторизован в сервисе.\n\n"
            "‼️ После передачи вы потеряете доступ к этому боту в сервисе."
        )
    else:
        text = "Этот раздел будет подключён следующим этапом."
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data=f"tb_sell_bot_{bot_row_id}")]]),
    )


async def traffic_withdraw_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    row = DatabaseManager.get_traffic_user_finances(user_id)
    balance, _deposit_balance, earned_balance, _total_spent, _total_earned, _total_withdrawn, last_cryptobot_user_id = (
        row if row else (0, 0, 0, 0, 0, 0, None)
    )
    if earned_balance < MIN_WITHDRAW:
        await query.edit_message_text(
            f"❌ Недостаточно средств для вывода.\n\n"
            f"Минимум: {MIN_WITHDRAW:.2f} ₽\n"
            f"Доступно к выводу: {earned_balance:.2f} ₽\n\n"
            f"Пополнения выводить нельзя. К выводу доступны только деньги, заработанные через интеграцию.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")]]),
        )
        return ConversationHandler.END

    context.user_data["tb_withdraw_available"] = float(earned_balance)
    context.user_data["tb_withdraw_last_cryptobot_user_id"] = last_cryptobot_user_id
    hint = f"\nПоследний CryptoBot user id: <code>{last_cryptobot_user_id}</code>" if last_cryptobot_user_id else ""
    await query.edit_message_text(
        f"💸 <b>Вывод средств через CryptoBot</b>\n\n"
        f"Текущий баланс: {balance:.2f} ₽\n"
        f"Доступно к выводу: {earned_balance:.2f} ₽\n\n"
        f"Введите сумму вывода в рублях.{hint}",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")]]),
    )
    return TRAFFIC_WITHDRAW_AMOUNT


async def traffic_withdraw_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    available = float(context.user_data.get("tb_withdraw_available", 0))
    try:
        amount = float(update.message.text.strip().replace(",", ".").replace(" ", ""))
    except ValueError:
        await update.message.reply_text("❌ Введите сумму числом.")
        return TRAFFIC_WITHDRAW_AMOUNT

    if amount < MIN_WITHDRAW:
        await update.message.reply_text(f"❌ Минимальная сумма вывода: {MIN_WITHDRAW:.2f} ₽")
        return TRAFFIC_WITHDRAW_AMOUNT
    if amount > available:
        await update.message.reply_text(
            f"❌ Нельзя вывести больше доступного.\nДоступно к выводу: {available:.2f} ₽"
        )
        return TRAFFIC_WITHDRAW_AMOUNT

    context.user_data["tb_withdraw_amount"] = round(amount, 2)
    last_cryptobot_user_id = context.user_data.get("tb_withdraw_last_cryptobot_user_id")
    hint = f"\nПоследний CryptoBot user id: <code>{last_cryptobot_user_id}</code>" if last_cryptobot_user_id else ""
    await update.message.reply_text(
        "Введите Telegram user id, который уже открывал @CryptoBot.\n"
        "Именно на этот user id Crypto Pay отправит перевод."
        f"{hint}",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")]]),
    )
    return TRAFFIC_WITHDRAW_USER_ID


async def traffic_withdraw_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        cryptobot_user_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Введите корректный Telegram user id числом.")
        return TRAFFIC_WITHDRAW_USER_ID

    user_id = update.effective_user.id
    amount = float(context.user_data.get("tb_withdraw_amount", 0))
    if amount <= 0:
        await update.message.reply_text("❌ Сумма вывода не найдена. Начните заново.")
        return ConversationHandler.END

    progress = await update.message.reply_text("⏳ Выполняю перевод через Crypto Pay...")
    transfer = await cryptobot_transfer_rub(
        amount,
        cryptobot_user_id,
        comment=f"Neo Link withdrawal for user {user_id}",
    )
    if not transfer.get("ok"):
        await progress.edit_text(
            f"❌ Не удалось выполнить вывод.\n\nПричина: {transfer.get('error', 'unknown error')}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")]]),
        )
        return ConversationHandler.END

    spend_id = transfer["spend_id"]
    DatabaseManager.create_traffic_withdrawal_request(
        user_id,
        amount,
        transfer["asset"],
        transfer["asset_amount"],
        cryptobot_user_id,
        spend_id,
    )
    completed = DatabaseManager.complete_traffic_withdrawal(
        user_id,
        amount,
        transfer["asset"],
        transfer["asset_amount"],
        cryptobot_user_id,
        spend_id,
        transfer["transfer_id"],
    )
    if not completed:
        DatabaseManager.fail_traffic_withdrawal(spend_id, "db_finalize_failed")
        await progress.edit_text(
            "❌ Перевод в Crypto Pay отправлен, но не удалось завершить запись в базе. Проверьте логи.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")]]),
        )
        return ConversationHandler.END

    DatabaseManager.set_traffic_user_cryptobot_id(user_id, cryptobot_user_id)
    await progress.edit_text(
        f"✅ Вывод выполнен.\n\n"
        f"Списано: {amount:.2f} ₽\n"
        f"Отправлено: {transfer['asset_amount']:.6f} {transfer['asset']}\n"
        f"Transfer ID: <code>{transfer['transfer_id']}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")]]),
    )
    return ConversationHandler.END


async def traffic_finance_placeholder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    text = "Перевод средств будет подключён следующим этапом."
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="tb_balance")]]),
    )


async def traffic_sell_toggle_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    bot_row_id = int(query.data.rsplit("_", 1)[1])
    bot_data = DatabaseManager.get_traffic_bot(bot_row_id)
    if not bot_data or bot_data[1] != query.from_user.id:
        await query.answer("Бот не найден", show_alert=True)
        return
    is_enabled = bot_data[9]
    DatabaseManager.set_traffic_bot_enabled(bot_row_id, not bool(is_enabled))
    await traffic_sell_open_bot(update, context)


async def admin_list_bots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    rows = DatabaseManager.execute_query(
        """SELECT
            tb.id,
            tb.bot_name,
            tb.bot_username,
            tb.status,
            COALESCE(tb.is_enabled, 1),
            tb.owner_user_id,
            COALESCE(COUNT(s.id), 0) AS sold_count,
            COALESCE(SUM(CASE WHEN s.rewarded = 1 THEN s.charge_amount ELSE 0 END), 0) AS earned
           FROM traffic_bots tb
           LEFT JOIN traffic_bot_subscriptions s ON s.traffic_bot_id = tb.id
           GROUP BY tb.id, tb.bot_name, tb.bot_username, tb.status, tb.is_enabled, tb.owner_user_id
           ORDER BY tb.id DESC""",
        fetch="all",
    ) or []
    if not rows:
        await update.message.reply_text("Ботов пока нет.")
        return
    lines = ["🤖 Боты в системе\n"]
    for row in rows[:40]:
        bot_id, bot_name, bot_username, status, is_enabled, owner_id, sold_count, earned = row
        status_text = "⏸ Остановлен" if status == "approved" and not is_enabled else get_status_text(status)
        lines.append(
            f"#{bot_id} | @{bot_username or 'unknown'} | {bot_name or 'Без названия'}\n"
            f"Статус: {status_text} | Владелец: {owner_id}\n"
            f"Подписок: {sold_count} | Заработано: {earned:.2f} ₽\n"
        )
    await update.message.reply_text("\n".join(lines))


async def audit_traffic_bot_subscriptions(context: ContextTypes.DEFAULT_TYPE):
    subscriptions = DatabaseManager.list_active_traffic_subscriptions()
    for subscription in subscriptions:
        (
            subscription_id,
            bot_row_id,
            external_user_id,
            sponsor_chat_id,
            sponsor_name,
            charge_amount,
            owner_user_id,
            bot_token,
            bot_name,
        ) = subscription
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://api.telegram.org/bot{bot_token}/getChatMember?chat_id={sponsor_chat_id}&user_id={external_user_id}",
                    timeout=10,
                ) as resp:
                    data = await resp.json()
            DatabaseManager.mark_traffic_subscription_checked(subscription_id)
            status = data.get("result", {}).get("status")
            if status in {"member", "administrator", "creator"}:
                continue
            result = DatabaseManager.charge_for_unsubscribe(subscription_id)
            if result and result.get("charged"):
                text = (
                    f"⚠️ Зафиксирована отписка в боте {bot_name or bot_row_id}.\n"
                    f"Списано: {charge_amount:.2f} ₽\n"
                    f"Спонсор: {sponsor_name or sponsor_chat_id}"
                )
                try:
                    await context.bot.send_message(owner_user_id, text)
                except Exception:
                    pass
        except Exception as exc:
            logger.warning(f"Subscription audit failed for #{subscription_id}: {exc}")
            DatabaseManager.add_traffic_bot_log(bot_row_id, "warning", "audit_failed", str(exc))


async def traffic_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(context.error, NetworkError):
        logger.warning(f"Traffic bot network error: {context.error}")
        return
    logger.exception("Traffic bot error", exc_info=context.error)


async def traffic_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Канал / Чат", callback_data="tb_buy_channel")],
        [InlineKeyboardButton("🌐 Ресурс (без проверки)", callback_data="tb_buy_resource")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="tb_back_main")],
    ])
    await query.edit_message_text(
        "🚀 <b>Покупка подписчиков</b>\n\nВыберите тип продвижения.",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
    )


async def traffic_sell_open_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    bot_row_id = int(query.data.rsplit("_", 1)[1])
    bot_data = DatabaseManager.get_traffic_bot(bot_row_id)
    if not bot_data or bot_data[1] != query.from_user.id:
        await query.answer("Бот не найден", show_alert=True)
        return

    (
        _,
        _owner_user_id,
        _bot_id,
        bot_username,
        bot_name,
        _bot_token,
        api_key,
        theme,
        status,
        is_enabled,
        rejection_reason,
        _display_bots,
        _display_resources,
        _max_sponsors,
        _reset_hours,
        _new_sponsors_after_hours,
        price_per_subscription,
        *_rest,
    ) = bot_data

    sold_count = DatabaseManager.execute_query(
        "SELECT COUNT(*) FROM traffic_bot_subscriptions WHERE traffic_bot_id = ?",
        (bot_row_id,),
        "one",
    )[0]
    charged_total = DatabaseManager.execute_query(
        "SELECT SUM(charge_amount) FROM traffic_bot_subscriptions WHERE traffic_bot_id = ? AND rewarded = 1",
        (bot_row_id,),
        "one",
    )[0] or 0

    status_text = "⏸ Остановлен" if status == "approved" and not is_enabled else get_status_text(status)
    text = (
        f"<b>{bot_name}</b> (@{bot_username or 'unknown'})\n"
        f"<i>Тематика:</i> {theme}\n"
        f"<i>Статус:</i> {status_text}\n\n"
        f"👥 Продано подписок всего: {sold_count}\n"
        f"💳 Заработано: {charged_total:.2f} ₽\n"
        f"💰 Доход владельца за 1 подписку: {price_per_subscription:.2f} ₽\n"
        f"🔑 API key: <code>{api_key}</code>"
    )
    if status == "rejected" and rejection_reason:
        text += f"\nПричина отклонения: {rejection_reason}"

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Обновить статистику", callback_data=f"tb_sell_bot_{bot_row_id}")],
            [InlineKeyboardButton("⚙️ Настройки", callback_data=f"tb_sell_settings_{bot_row_id}"),
             InlineKeyboardButton("📊 Статистика", callback_data=f"tb_sell_stats_{bot_row_id}")],
            [InlineKeyboardButton("💻 Интеграция (API)", url=get_docs_deeplink()),
             InlineKeyboardButton("↔️ Передать бота", callback_data=f"tb_sell_transfer_{bot_row_id}")],
            [InlineKeyboardButton("▶️ Включить" if not is_enabled else "⏸ Остановить", callback_data=f"tb_sell_toggle_status_{bot_row_id}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="tb_sell_list")],
        ]),
    )


async def traffic_sell_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    bot_row_id = int(query.data.rsplit("_", 1)[1])
    bot_data = DatabaseManager.get_traffic_bot(bot_row_id)
    if not bot_data or bot_data[1] != query.from_user.id:
        await query.answer("Бот не найден", show_alert=True)
        return

    context.user_data["sell_bot_id"] = bot_row_id
    (
        _,
        _owner_user_id,
        _bot_id,
        bot_username,
        bot_name,
        _bot_token,
        api_key,
        _theme,
        _status,
        _is_enabled,
        _rejection_reason,
        display_bots,
        display_resources,
        max_sponsors,
        _reset_hours,
        _new_sponsors_after_hours,
        _price_per_subscription,
        _anti_scam_enabled,
        _suspicious_limit,
    ) = bot_data

    text = (
        f"⚙️ <b>Настройки бота</b>\n\n"
        f"{bot_name} (@{bot_username or 'unknown'})\n\n"
        f"Боты: {'Да' if display_bots else 'Нет'}\n"
        f"Ресурсы: {'Да' if display_resources else 'Нет'}\n"
        f"Макс. спонсоров: {max_sponsors}\n"
        f"API key: <code>{api_key}</code>"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 Актуализировать токен", callback_data=f"tb_sell_set_token_{bot_row_id}")],
            [InlineKeyboardButton(f"Боты: {'Да' if display_bots else 'Нет'}", callback_data=f"tb_sell_toggle_bots_{bot_row_id}")],
            [InlineKeyboardButton(f"Ресурсы: {'Да' if display_resources else 'Нет'}", callback_data=f"tb_sell_toggle_resources_{bot_row_id}")],
            [InlineKeyboardButton("📊 Макс. количество спонсоров", callback_data=f"tb_sell_set_max_{bot_row_id}")],
            [InlineKeyboardButton("📚 Документация", url=get_docs_deeplink())],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"tb_sell_bot_{bot_row_id}")],
        ]),
    )


def build_traffic_bot():
    app = Application.builder().token(TRAFFIC_BOT_TOKEN).build()

    async def post_init(application):
        logger.info("Traffic bot initialized")
        application.job_queue.run_repeating(lambda c: asyncio.create_task(cryptobot_check_invoices()), interval=10, first=5)
        application.job_queue.run_repeating(audit_traffic_bot_subscriptions, interval=300, first=60)

    app.post_init = post_init

    buy_channel_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(traffic_buy_channel, pattern="^tb_buy_channel$")],
        states={
            TRAFFIC_CHANNEL_LINK: [MessageHandler(~filters.COMMAND, traffic_channel_link)],
            TRAFFIC_CHANNEL_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, traffic_channel_count)],
        },
        fallbacks=[CallbackQueryHandler(traffic_back_main, pattern="^tb_back_main$")],
    )

    buy_resource_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(traffic_buy_resource, pattern="^tb_buy_resource$")],
        states={
            TRAFFIC_RESOURCE_LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, traffic_resource_link)],
            TRAFFIC_RESOURCE_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, traffic_resource_count)],
        },
        fallbacks=[CallbackQueryHandler(traffic_back_main, pattern="^tb_back_main$")],
    )

    topup_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(traffic_topup_amount_handler, pattern="^tb_topup_")],
        states={TRAFFIC_TOPUP_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, traffic_topup_amount_manual)]},
        fallbacks=[CallbackQueryHandler(traffic_back_main, pattern="^tb_back_main$")],
    )

    withdraw_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(traffic_withdraw_start, pattern="^tb_withdraw_start$")],
        states={
            TRAFFIC_WITHDRAW_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, traffic_withdraw_amount)],
            TRAFFIC_WITHDRAW_USER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, traffic_withdraw_user_id)],
        },
        fallbacks=[CallbackQueryHandler(traffic_balance, pattern="^tb_balance$")],
    )

    add_sell_bot_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(traffic_sell_add_start, pattern="^tb_sell_add$")],
        states={TRAFFIC_SELL_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, traffic_sell_add_token)]},
        fallbacks=[CallbackQueryHandler(traffic_sell_menu, pattern="^tb_sell$")],
    )

    token_update_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(traffic_sell_token_prompt, pattern="^tb_sell_set_token_\\d+$")],
        states={TRAFFIC_SETTINGS_TOKEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, traffic_sell_update_token)]},
        fallbacks=[CallbackQueryHandler(traffic_sell_menu, pattern="^tb_sell$")],
    )

    max_sponsors_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(traffic_sell_max_prompt, pattern="^tb_sell_set_max_\\d+$")],
        states={TRAFFIC_SETTINGS_MAX_SPONSORS: [MessageHandler(filters.TEXT & ~filters.COMMAND, traffic_sell_set_max)]},
        fallbacks=[CallbackQueryHandler(traffic_sell_menu, pattern="^tb_sell$")],
    )

    app.add_handler(buy_channel_conv)
    app.add_handler(buy_resource_conv)
    app.add_handler(topup_conv)
    app.add_handler(withdraw_conv)
    app.add_handler(add_sell_bot_conv)
    app.add_handler(token_update_conv)
    app.add_handler(max_sponsors_conv)
    app.add_error_handler(traffic_error_handler)

    app.add_handler(CommandHandler("start", traffic_start))
    app.add_handler(CommandHandler("list", admin_list_bots))
    app.add_handler(CallbackQueryHandler(traffic_buy, pattern="^tb_buy$"))
    app.add_handler(CallbackQueryHandler(traffic_back_main, pattern="^tb_back_main$"))
    app.add_handler(CallbackQueryHandler(traffic_orders, pattern="^tb_orders$"))
    app.add_handler(CallbackQueryHandler(traffic_balance, pattern="^tb_balance$"))
    app.add_handler(CallbackQueryHandler(traffic_transactions, pattern="^tb_transactions$"))
    app.add_handler(CallbackQueryHandler(traffic_topup, pattern="^tb_topup$"))
    app.add_handler(CallbackQueryHandler(traffic_check_payment, pattern="^tb_check_payment$"))
    app.add_handler(CallbackQueryHandler(traffic_profile, pattern="^tb_profile$"))
    app.add_handler(CallbackQueryHandler(traffic_referral, pattern="^tb_referral$"))
    app.add_handler(CallbackQueryHandler(traffic_pay_order, pattern="^tb_pay_order$"))
    app.add_handler(CallbackQueryHandler(traffic_finance_placeholder, pattern="^tb_transfer_placeholder$"))
    app.add_handler(CallbackQueryHandler(traffic_sell_menu, pattern="^tb_sell$"))
    app.add_handler(CallbackQueryHandler(traffic_sell_list, pattern="^tb_sell_list$"))
    app.add_handler(CallbackQueryHandler(traffic_sell_open_bot, pattern="^tb_sell_bot_\\d+$"))
    app.add_handler(CallbackQueryHandler(traffic_sell_docs, pattern="^tb_sell_docs(?:_\\d+)?$"))
    app.add_handler(CallbackQueryHandler(traffic_sell_settings, pattern="^tb_sell_settings_\\d+$"))
    app.add_handler(CallbackQueryHandler(traffic_sell_stats, pattern="^tb_sell_stats_\\d+$"))
    app.add_handler(CallbackQueryHandler(traffic_sell_placeholder, pattern="^tb_sell_transfer_\\d+$"))
    app.add_handler(CallbackQueryHandler(traffic_sell_toggle_status, pattern="^tb_sell_toggle_status_\\d+$"))
    app.add_handler(CallbackQueryHandler(traffic_sell_toggle_setting, pattern="^tb_sell_toggle_(bots|resources)_\\d+$"))
    app.add_handler(CallbackQueryHandler(handle_moderation_callback, pattern="^tb_mod_(approve|reject)_\\d+$"))

    return app
