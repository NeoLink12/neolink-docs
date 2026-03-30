import logging
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import web

from config import CHECKSUB_BOT_TOKEN, NEOLINK_API_HOST, NEOLINK_API_PORT
from database import DatabaseManager

logger = logging.getLogger(__name__)


def normalize_link(value: Optional[str]) -> str:
    if not value:
        return ""
    link = str(value).strip().rstrip("/")
    if link.startswith("https://t.me/"):
        return link
    if link.startswith("http://t.me/"):
        return "https://" + link[len("http://"):]
    if link.startswith("t.me/"):
        return "https://" + link
    if link.startswith("@"):
        return f"https://t.me/{link[1:]}"
    return link


def sponsor_identifier(order_row) -> str:
    _order_id, _user_id, link, order_type, _amount, _done, channel_id, invite_link = order_row
    if channel_id:
        return str(channel_id)
    if invite_link:
        return normalize_link(invite_link)
    return normalize_link(link)


def sponsor_title(order_row) -> str:
    order_id, _user_id, _link, order_type, _amount, _done, _channel_id, _invite_link = order_row
    return f"Sponsor #{order_id} ({'checked' if order_type == 'channel' else 'direct'})"


def sponsor_payload(order_id: int, sponsor_chat_id: str, link_value: str, order_type: str, amount: int, done: int, channel_id: Optional[str]) -> Dict[str, Any]:
    return {
        "order_id": order_id,
        "title": f"Sponsor #{order_id}",
        "link": link_value,
        "sponsor_chat_id": sponsor_chat_id,
        "requires_check": bool(channel_id and order_type == "channel"),
        "order_type": order_type,
        "remaining": max(int(amount) - int(done), 0),
        "charge_amount": 1,
    }


async def telegram_get_chat_member(chat_id: str, user_id: int) -> Dict[str, Any]:
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"https://api.telegram.org/bot{CHECKSUB_BOT_TOKEN}/getChatMember",
            params={"chat_id": chat_id, "user_id": user_id},
            timeout=15,
        ) as resp:
            return await resp.json()


def json_error(message: str, status: int = 400, **extra):
    payload = {"ok": False, "error": message}
    payload.update(extra)
    return web.json_response(payload, status=status)


async def read_json(request: web.Request) -> Optional[Dict[str, Any]]:
    try:
        return await request.json()
    except Exception:
        return None


async def authenticate_bot(payload: Dict[str, Any]):
    api_key = str(payload.get("api_key", "")).strip()
    if not api_key:
        return None, json_error("api_key_required", 401)

    bot_data = DatabaseManager.get_traffic_bot_by_api_key(api_key)
    if not bot_data:
        return None, json_error("invalid_api_key", 401)
    if bot_data[8] != "approved":
        return None, json_error("bot_not_approved", 403)
    if not bot_data[9]:
        return None, json_error("bot_disabled", 403)
    return bot_data, None


async def handle_get_sponsors(request: web.Request):
    payload = await read_json(request)
    if not payload:
        return json_error("invalid_json")

    bot_data, error = await authenticate_bot(payload)
    if error:
        return error

    user_id = payload.get("user_id")
    if not user_id:
        return json_error("user_id_required")

    display_bots = bool(bot_data[11])
    display_resources = bool(bot_data[12])
    max_sponsors = max(1, min(int(bot_data[13] or 1), 10))

    sponsors = []
    seen = set()
    assigned_rows = DatabaseManager.get_active_traffic_sponsor_assignments(bot_data[0], int(user_id))
    if assigned_rows:
        for row in assigned_rows:
            order_id, sponsor_chat_id, sponsor_link, order_type, amount, done, channel_id, invite_link, link = row
            link_value = normalize_link(sponsor_link or invite_link or link)
            if not link_value or sponsor_chat_id in seen:
                continue
            seen.add(sponsor_chat_id)
            sponsors.append(
                sponsor_payload(
                    order_id=order_id,
                    sponsor_chat_id=sponsor_chat_id,
                    link_value=link_value,
                    order_type=order_type,
                    amount=amount,
                    done=done,
                    channel_id=channel_id,
                )
            )
    else:
        rows = DatabaseManager.get_active_sponsor_orders(
            include_channel=display_bots,
            include_resource=display_resources,
            limit=max_sponsors,
        )

        assignments = []
        for row in rows:
            order_id, _buyer_user_id, link, order_type, amount, done, channel_id, invite_link = row
            link_value = normalize_link(invite_link or link)
            sponsor_chat_id = sponsor_identifier(row)
            if not link_value or sponsor_chat_id in seen:
                continue
            seen.add(sponsor_chat_id)
            assignments.append(
                {
                    "order_id": order_id,
                    "sponsor_chat_id": sponsor_chat_id,
                    "sponsor_link": link_value,
                }
            )
            sponsors.append(
                sponsor_payload(
                    order_id=order_id,
                    sponsor_chat_id=sponsor_chat_id,
                    link_value=link_value,
                    order_type=order_type,
                    amount=amount,
                    done=done,
                    channel_id=channel_id,
                )
            )

        if assignments:
            DatabaseManager.replace_traffic_sponsor_assignments(bot_data[0], int(user_id), assignments)

    DatabaseManager.add_traffic_bot_event(bot_data[0], "get_sponsors", user_id=int(user_id), payload=str(len(sponsors)))
    return web.json_response({"ok": True, "sponsors": sponsors})


async def handle_check_member(request: web.Request):
    payload = await read_json(request)
    if not payload:
        return json_error("invalid_json")

    bot_data, error = await authenticate_bot(payload)
    if error:
        return error

    user_id = payload.get("user_id")
    sponsor_chat_id = str(payload.get("sponsor_chat_id", "")).strip()
    if not user_id:
        return json_error("user_id_required")
    if not sponsor_chat_id:
        return json_error("sponsor_chat_id_required")

    assignment = DatabaseManager.get_valid_traffic_sponsor_assignment(
        bot_data[0],
        int(user_id),
        sponsor_chat_id=sponsor_chat_id,
    )
    if not assignment:
        return json_error("assignment_not_found", 404)

    if sponsor_chat_id.startswith("https://") or sponsor_chat_id.startswith("http://"):
        return web.json_response({"ok": True, "subscribed": False, "status": "link_only"})

    try:
        data = await telegram_get_chat_member(sponsor_chat_id, int(user_id))
        status = data.get("result", {}).get("status")
        subscribed = status in {"member", "administrator", "creator"}
        DatabaseManager.add_traffic_bot_event(bot_data[0], "check_member", user_id=int(user_id), sponsor_chat_id=sponsor_chat_id, payload=status)
        return web.json_response({"ok": True, "subscribed": subscribed, "status": status})
    except Exception as exc:
        logger.error(f"NeoLink check-member error: {exc}")
        return json_error("telegram_check_failed", 502)


async def handle_register_subscription(request: web.Request):
    payload = await read_json(request)
    if not payload:
        return json_error("invalid_json")

    bot_data, error = await authenticate_bot(payload)
    if error:
        return error

    user_id = payload.get("user_id")
    sponsor_chat_id = str(payload.get("sponsor_chat_id", "")).strip()
    sponsor_name = str(payload.get("sponsor_name", "")).strip() or None
    order_id = payload.get("order_id")
    charge_amount = float(payload.get("charge_amount", 1) or 1)

    if not user_id:
        return json_error("user_id_required")
    if not sponsor_chat_id:
        return json_error("sponsor_chat_id_required")

    assignment = DatabaseManager.get_valid_traffic_sponsor_assignment(
        bot_data[0],
        int(user_id),
        sponsor_chat_id=sponsor_chat_id,
        order_id=int(order_id) if order_id else None,
    )
    if not assignment:
        return json_error("assignment_not_found", 404)

    resolved_order_id = int(order_id) if order_id else assignment[0]

    result = DatabaseManager.register_traffic_order_conversion(
        api_key=bot_data[6],
        external_user_id=int(user_id),
        sponsor_chat_id=sponsor_chat_id,
        sponsor_name=sponsor_name,
        order_id=resolved_order_id,
        charge_amount=charge_amount,
    )
    if not result.get("ok"):
        return json_error(result.get("reason", "register_failed"), 400, details=result.get("details"))

    return web.json_response(result)


def build_neolink_api_app():
    app = web.Application()
    app.router.add_post("/api/neolink/get-sponsors", handle_get_sponsors)
    app.router.add_post("/api/neolink/check-member", handle_check_member)
    app.router.add_post("/api/neolink/register-subscription", handle_register_subscription)
    return app


async def start_neolink_api():
    app = build_neolink_api_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, NEOLINK_API_HOST, NEOLINK_API_PORT)
    await site.start()
    logger.info(f"NeoLink API started on http://{NEOLINK_API_HOST}:{NEOLINK_API_PORT}")
    return runner
