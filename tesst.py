import logging
from typing import Any, Dict, List

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes


BOT_TOKEN = "8168640281:AAFdwRA3mL7ObP8rlnZxunZ9aklj7mNj5FY"
NEOLINK_API_KEY = "cdd057810b3c6f3b9bf3094879eeefae0276501c5a1ab08c1f3283238fcde086"
NEOLINK_BASE_URL = "http://127.0.0.1:8080/api/neolink"

logger = logging.getLogger(__name__)


def ensure_config() -> List[str]:
    missing = []
    if not BOT_TOKEN.strip():
        missing.append("BOT_TOKEN")
    if not NEOLINK_API_KEY.strip():
        missing.append("NEOLINK_API_KEY")
    if not NEOLINK_BASE_URL.strip():
        missing.append("NEOLINK_BASE_URL")
    return missing


async def neolink_request(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = {"api_key": NEOLINK_API_KEY, **payload}
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{NEOLINK_BASE_URL}/{endpoint}", json=body, timeout=15) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(data.get("error") or f"http_{resp.status}")
            return data


async def filter_visible_sponsors(user_id: int, sponsors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    visible = []
    for sponsor in sponsors:
        if sponsor.get("requires_check"):
            try:
                check_result = await neolink_request(
                    "check-member",
                    {
                        "user_id": user_id,
                        "sponsor_chat_id": sponsor["sponsor_chat_id"],
                    },
                )
                if check_result.get("subscribed"):
                    continue
            except Exception as exc:
                logger.warning(f"Failed to pre-check sponsor {sponsor.get('order_id')}: {exc}")
        visible.append(sponsor)
    return visible


async def load_visible_sponsors(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> List[Dict[str, Any]]:
    response = await neolink_request("get-sponsors", {"user_id": user_id})
    raw_sponsors = response.get("sponsors", [])
    visible_sponsors = await filter_visible_sponsors(user_id, raw_sponsors)
    context.user_data["test_visible_sponsors"] = {str(item["order_id"]): item for item in visible_sponsors}
    return visible_sponsors


def build_sponsor_block_text(sponsors: List[Dict[str, Any]]) -> str:
    if not sponsors:
        return (
            "✅ <b>Все обязательные подписки выполнены</b>\n\n"
            "Сейчас новых спонсоров для показа нет."
        )
    return (
        "Чтобы продолжить пользоваться ботом, пожалуйста,\n"
        "подпишись на следующие ресурсы! 🤠"
    )


def build_sponsor_block_keyboard(sponsors: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    keyboard = []
    row = []
    for index, sponsor in enumerate(sponsors, start=1):
        row.append(InlineKeyboardButton(f"Спонсор №{index}", url=sponsor["link"]))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    if sponsors:
        keyboard.append([InlineKeyboardButton("✅ Я подписан", callback_data="test_confirm")])
    else:
        keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="test_refresh")])
    return InlineKeyboardMarkup(keyboard)


async def render_sponsor_block(target, user_id: int, context: ContextTypes.DEFAULT_TYPE, edit: bool = False):
    sponsors = await load_visible_sponsors(user_id, context)
    text = build_sponsor_block_text(sponsors)
    markup = build_sponsor_block_keyboard(sponsors)
    if edit:
        await target.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=markup, disable_web_page_preview=True)
    else:
        await target.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup, disable_web_page_preview=True)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    missing = ensure_config()
    if missing:
        await update.message.reply_text("Не заполнены настройки: " + ", ".join(missing))
        return
    await render_sponsor_block(update.message, update.effective_user.id, context)


async def refresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    missing = ensure_config()
    if missing:
        await update.message.reply_text("Не заполнены настройки: " + ", ".join(missing))
        return
    await render_sponsor_block(update.message, update.effective_user.id, context)


async def confirm_subscriptions(query, user_id: int, context: ContextTypes.DEFAULT_TYPE):
    visible_map = context.user_data.get("test_visible_sponsors", {})
    visible_sponsors = list(visible_map.values())

    if not visible_sponsors:
        await render_sponsor_block(query.message, user_id, context, edit=True)
        return

    success_count = 0
    missing_count = 0

    for sponsor in visible_sponsors:
        sponsor_chat_id = sponsor["sponsor_chat_id"]
        try:
            if sponsor.get("requires_check"):
                check_result = await neolink_request(
                    "check-member",
                    {
                        "user_id": user_id,
                        "sponsor_chat_id": sponsor_chat_id,
                    },
                )
                if not check_result.get("subscribed"):
                    missing_count += 1
                    continue

            register_result = await neolink_request(
                "register-subscription",
                {
                    "user_id": user_id,
                    "order_id": sponsor["order_id"],
                    "sponsor_chat_id": sponsor_chat_id,
                    "sponsor_name": sponsor.get("title"),
                    "charge_amount": sponsor.get("charge_amount", 1),
                },
            )
            if register_result.get("credited") or register_result.get("duplicate"):
                success_count += 1
        except Exception as exc:
            logger.warning(f"Failed to confirm sponsor {sponsor.get('order_id')}: {exc}")
            missing_count += 1

    if missing_count:
        await query.answer(f"Не все подписки выполнены. Осталось: {missing_count}", show_alert=True)
    elif success_count:
        await query.answer("Подписки подтверждены.", show_alert=True)
    else:
        await query.answer("Нечего подтверждать.", show_alert=True)

    await render_sponsor_block(query.message, user_id, context, edit=True)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    missing = ensure_config()
    if missing:
        await query.edit_message_text("Не заполнены настройки: " + ", ".join(missing))
        return

    user_id = query.from_user.id

    if query.data == "test_refresh":
        await render_sponsor_block(query.message, user_id, context, edit=True)
        return

    if query.data == "test_confirm":
        await confirm_subscriptions(query, user_id, context)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("tesst.py error", exc_info=context.error)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    missing = ensure_config()
    if missing:
        raise SystemExit("Не заполнены настройки: " + ", ".join(missing))

    application = ApplicationBuilder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("refresh", refresh_command))
    application.add_handler(CallbackQueryHandler(callback_handler, pattern=r"^test_(confirm|refresh)$"))
    application.add_error_handler(error_handler)
    application.run_polling()


if __name__ == "__main__":
    main()
