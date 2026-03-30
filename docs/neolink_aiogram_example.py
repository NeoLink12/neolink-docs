import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message

BOT_TOKEN = "YOUR_BOT_TOKEN"
NEOLINK_API_KEY = "YOUR_NEO_LINK_API_KEY"
NEOLINK_BASE_URL = "https://your-neolink-gateway.example/api/neolink"

bot = Bot(BOT_TOKEN)
dp = Dispatcher()


async def call_neolink(endpoint: str, payload: dict):
    body = {"api_key": NEOLINK_API_KEY, **payload}
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{NEOLINK_BASE_URL}/{endpoint}", json=body, timeout=10) as resp:
            resp.raise_for_status()
            return await resp.json()


@dp.message(CommandStart())
async def start(message: Message):
    await message.answer("Бот подключён к Neo Link.")


@dp.message(F.text == "/sponsors")
async def sponsors(message: Message):
    try:
        data = await call_neolink("get-sponsors", {"user_id": message.from_user.id})
        sponsors = data.get("sponsors", [])
        if not sponsors:
            await message.answer("Сейчас спонсоров нет.")
            return

        lines = ["Доступные спонсоры:"]
        for sponsor in sponsors:
            lines.append(f"- {sponsor['title']}: {sponsor['link']}")
        await message.answer("\n".join(lines))
    except Exception:
        await message.answer("Neo Link временно недоступен. Попробуйте позже.")


@dp.message(F.text.startswith("/check "))
async def check_subscription(message: Message):
    sponsor_chat_id = message.text.split(maxsplit=1)[1]
    try:
        data = await call_neolink(
            "check-member",
            {
                "user_id": message.from_user.id,
                "sponsor_chat_id": sponsor_chat_id,
            },
        )
        if data.get("subscribed"):
            await message.answer("Подписка подтверждена.")
        else:
            await message.answer("Подписка не подтверждена.")
    except Exception:
        await message.answer("Ошибка проверки подписки.")


@dp.message(F.text.startswith("/report_sub "))
async def report_subscription(message: Message):
    sponsor_chat_id = message.text.split(maxsplit=1)[1]
    try:
        await call_neolink(
            "register-subscription",
            {
                "user_id": message.from_user.id,
                "sponsor_chat_id": sponsor_chat_id,
                "charge_amount": 1,
            },
        )
        await message.answer("Подписка зарегистрирована в Neo Link.")
    except Exception:
        await message.answer("Не удалось зарегистрировать подписку.")
