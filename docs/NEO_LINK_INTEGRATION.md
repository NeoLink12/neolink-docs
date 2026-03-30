# Neo Link: как подключить интеграцию в свой Telegram-бот

## Для чего это нужно

Эта инструкция нужна владельцу Telegram-бота, который хочет подключить свой бот к Neo Link.

После подключения ваш бот сможет:

- получать список спонсоров из Neo Link
- показывать их пользователю
- проверять подписку пользователя
- подтверждать подписку в системе

В этой документации нет внутренних деталей Neo Link. Ниже только то, что нужно для подключения в ваш бот.

## Что нужно перед началом

Подготовьте:

- токен вашего Telegram-бота
- API key, который выдаст Neo Link
- адрес публичного API Neo Link
- бота, написанного на `Python` и `aiogram`

## Шаг 1. Добавьте бота в Neo Link

В интерфейсе Neo Link:

1. Откройте `Продажа трафика`
2. Нажмите `Добавить нового бота`
3. Отправьте токен вашего бота
4. Получите `API key`
5. Дождитесь модерации

После этого у вас будет всё необходимое для интеграции:

- `BOT_TOKEN`
- `NEOLINK_API_KEY`
- адрес API

## Шаг 2. Добавьте настройки в своего бота

Пример:

```python
BOT_TOKEN = "YOUR_BOT_TOKEN"
NEOLINK_API_KEY = "YOUR_NEO_LINK_API_KEY"
NEOLINK_BASE_URL = "https://your-domain.com/api/neolink"
```

## Шаг 3. Добавьте HTTP-клиент для Neo Link

Это базовая функция, через которую ваш бот будет ходить в Neo Link API:

```python
import aiohttp


async def neolink_request(endpoint: str, payload: dict):
    body = {"api_key": NEOLINK_API_KEY, **payload}
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{NEOLINK_BASE_URL}/{endpoint}",
            json=body,
            timeout=10,
        ) as resp:
            resp.raise_for_status()
            return await resp.json()
```

## Шаг 4. Какие методы нужно использовать

### Получить список спонсоров

Ваш бот должен запрашивать список спонсоров для пользователя.

Пример запроса:

```http
POST /api/neolink/get-sponsors
```

Тело:

```json
{
  "api_key": "YOUR_API_KEY",
  "user_id": 123456789
}
```

Пример ответа:

```json
{
  "ok": true,
  "sponsors": [
    {
      "chat_id": "-1001234567890",
      "title": "Sponsor channel",
      "link": "https://t.me/example"
    }
  ]
}
```

### Проверить подписку пользователя

Когда пользователь подписался, ваш бот должен проверить статус подписки.

Пример запроса:

```http
POST /api/neolink/check-member
```

Тело:

```json
{
  "api_key": "YOUR_API_KEY",
  "user_id": 123456789,
  "sponsor_chat_id": "-1001234567890"
}
```

Пример ответа:

```json
{
  "ok": true,
  "subscribed": true,
  "status": "member"
}
```

### Подтвердить подписку в Neo Link

Если подписка реально подтверждена, отправьте это событие в Neo Link.

Пример запроса:

```http
POST /api/neolink/register-subscription
```

Тело:

```json
{
  "api_key": "YOUR_API_KEY",
  "user_id": 123456789,
  "sponsor_chat_id": "-1001234567890",
  "sponsor_name": "Sponsor channel",
  "charge_amount": 1
}
```

Пример ответа:

```json
{
  "ok": true,
  "saved": true
}
```

## Шаг 5. Как проверять подписку правильно

Проверять подписку нужно через Telegram Bot API:

- `getChatMember`

Успешной считается только подписка со статусом:

- `member`
- `administrator`
- `creator`

Если пришёл:

- `left`
- `kicked`
- ошибка доступа
- ошибка сети

то подписку подтверждать нельзя.

## Шаг 6. Пример интеграции на aiogram

Ниже минимальный рабочий пример.

```python
import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message

BOT_TOKEN = "YOUR_BOT_TOKEN"
NEOLINK_API_KEY = "YOUR_NEO_LINK_API_KEY"
NEOLINK_BASE_URL = "https://your-domain.com/api/neolink"

bot = Bot(BOT_TOKEN)
dp = Dispatcher()


async def neolink_request(endpoint: str, payload: dict):
    body = {"api_key": NEOLINK_API_KEY, **payload}
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{NEOLINK_BASE_URL}/{endpoint}",
            json=body,
            timeout=10,
        ) as resp:
            resp.raise_for_status()
            return await resp.json()


@dp.message(CommandStart())
async def start(message: Message):
    await message.answer("Бот подключён к Neo Link.")


@dp.message(F.text == "/sponsors")
async def sponsors(message: Message):
    data = await neolink_request(
        "get-sponsors",
        {"user_id": message.from_user.id},
    )

    sponsors = data.get("sponsors", [])
    if not sponsors:
        await message.answer("Сейчас спонсоров нет.")
        return

    lines = ["Доступные спонсоры:"]
    for sponsor in sponsors:
        lines.append(f"- {sponsor['title']}: {sponsor['link']}")
    await message.answer("\n".join(lines))


@dp.message(F.text.startswith("/check "))
async def check_sponsor(message: Message):
    sponsor_chat_id = message.text.split(maxsplit=1)[1]

    data = await neolink_request(
        "check-member",
        {
            "user_id": message.from_user.id,
            "sponsor_chat_id": sponsor_chat_id,
        },
    )

    if not data.get("subscribed"):
        await message.answer("Подписка не подтверждена.")
        return

    await neolink_request(
        "register-subscription",
        {
            "user_id": message.from_user.id,
            "sponsor_chat_id": sponsor_chat_id,
            "charge_amount": 1,
        },
    )

    await message.answer("Подписка подтверждена и отправлена в Neo Link.")
```

Полный пример лежит в файле:

- `docs/neolink_aiogram_example.py`

## Шаг 7. Что обязательно обработать в коде

В своём боте обязательно обработайте:

- невалидный токен
- таймаут запроса
- ошибку сети
- недоступность API
- случай, когда пользователь не подписался

Пример:

```python
try:
    data = await neolink_request("get-sponsors", {"user_id": user_id})
except Exception:
    await message.answer("Neo Link временно недоступен. Попробуйте позже.")
    return
```

## Шаг 8. Как проверить, что интеграция работает

Проверьте руками:

1. бот получает список спонсоров
2. ссылки на спонсоров открываются
3. после подписки проверка проходит успешно
4. подтверждение уходит в Neo Link
5. если пользователь не подписался, подтверждение не отправляется

## Итог

Чтобы подключить Neo Link в свой бот, вам нужно сделать только 4 вещи:

1. добавить своего бота в Neo Link
2. получить API key
3. вставить HTTP-запросы в своего aiogram-бота
4. проверять подписку и подтверждать её в Neo Link
