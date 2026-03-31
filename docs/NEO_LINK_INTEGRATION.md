# Neo Link: как подключить интеграцию в свой Telegram-бот

Практическая инструкция для разработчика, который хочет встроить Neo Link в собственного Telegram-бота.

## 1. Что нужно получить в Neo Link

В интерфейсе Neo Link:

1. Откройте `Продажа трафика`.
2. Нажмите `Добавить нового бота`.
3. Отправьте токен вашего Telegram-бота.
4. Получите `API key`.
5. Дождитесь модерации бота.

После этого у вас будут:

- `BOT_TOKEN`
- `NEOLINK_API_KEY`
- `NEOLINK_BASE_URL`

Пример:

```python
BOT_TOKEN = "YOUR_BOT_TOKEN"
NEOLINK_API_KEY = "YOUR_NEO_LINK_API_KEY"
NEOLINK_BASE_URL = "http://127.0.0.1:8080/api/neolink"
```

## 2. Какой `NEOLINK_BASE_URL` ставить

Используйте:

```python
NEOLINK_BASE_URL = "http://127.0.0.1:8080/api/neolink"
```

только если Neo Link API и ваш бот работают на одной и той же машине, и API поднят локально через `main.py`.

Если бот будет крутиться на другом сервере, `127.0.0.1` не подойдёт. Тогда нужен реальный адрес сервера:

```python
NEOLINK_BASE_URL = "https://your-domain.com/api/neolink"
```

## 3. Базовый HTTP-клиент

```python
import aiohttp


async def neolink_request(endpoint: str, payload: dict) -> dict:
    body = {"api_key": NEOLINK_API_KEY, **payload}
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{NEOLINK_BASE_URL}/{endpoint}",
            json=body,
            timeout=15,
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(data.get("error") or f"http_{resp.status}")
            return data
```

## 4. Какие методы вызывает ваш бот

Ваш бот использует три метода:

- `POST /api/neolink/get-sponsors`
- `POST /api/neolink/check-member`
- `POST /api/neolink/register-subscription`

### Получить спонсоров

```http
POST /api/neolink/get-sponsors
```

```json
{
  "api_key": "YOUR_API_KEY",
  "user_id": 123456789
}
```

Пример успешного ответа:

```json
{
  "ok": true,
  "status": "ok",
  "chat_id": 123456789,
  "completed": false,
  "skip": false,
  "message": "sponsors loaded",
  "sponsors": [
    {
      "order_id": 55,
      "title": "Sponsor #55",
      "link": "https://t.me/example_channel",
      "sponsor_chat_id": "-1001234567890",
      "requires_check": true,
      "charge_amount": 1
    }
  ]
}
```

### Проверить подписку

```http
POST /api/neolink/check-member
```

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

Подписка считается подтверждённой только если `status` один из:

- `member`
- `administrator`
- `creator`

Если пришёл `left`, `kicked` или ошибка сети, подтверждать подписку нельзя.

### Подтвердить подписку

```http
POST /api/neolink/register-subscription
```

```json
{
  "api_key": "YOUR_API_KEY",
  "user_id": 123456789,
  "order_id": 55,
  "sponsor_chat_id": "-1001234567890",
  "sponsor_name": "Sponsor #55",
  "charge_amount": 1
}
```

Пример ответа при новом начислении:

```json
{
  "ok": true,
  "credited": true,
  "subscription_id": 10,
  "order_id": 55,
  "order_done": 18,
  "order_amount": 100,
  "order_completed": false
}
```

Пример ответа при повторном подтверждении без нового начисления:

```json
{
  "ok": true,
  "credited": false,
  "duplicate": true,
  "order_id": 55
}
```

## 5. Правильная логика показа

Если пользователь уже подписан на канал, этот спонсор не должен показываться в блоке.

Логика должна быть такой:

1. Бот вызывает `get-sponsors`.
2. Для каждого спонсора с `requires_check = true` бот вызывает `check-member`.
3. Если `subscribed = true`, спонсор скрывается.
4. Пользователю показываются только невыполненные спонсоры.

## 6. Готовый пример для aiogram

Ниже рабочая схема: загрузка спонсоров, фильтрация уже выполненных, сохранение в `FSMContext`, показ кнопок и финальная проверка.

```python
import aiohttp

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message


router = Router()

BOT_TOKEN = "YOUR_BOT_TOKEN"
NEOLINK_API_KEY = "YOUR_NEO_LINK_API_KEY"
NEOLINK_BASE_URL = "http://127.0.0.1:8080/api/neolink"


async def neolink_request(endpoint: str, payload: dict) -> dict:
    body = {"api_key": NEOLINK_API_KEY, **payload}
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{NEOLINK_BASE_URL}/{endpoint}",
            json=body,
            timeout=15,
        ) as resp:
            data = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(data.get("error") or f"http_{resp.status}")
            return data


async def load_visible_sponsors(user_id: int) -> list[dict]:
    response = await neolink_request("get-sponsors", {"user_id": user_id})
    sponsors = response.get("sponsors", [])

    visible = []
    for sponsor in sponsors:
        if sponsor.get("requires_check"):
            check_result = await neolink_request(
                "check-member",
                {
                    "user_id": user_id,
                    "sponsor_chat_id": sponsor["sponsor_chat_id"],
                },
            )
            if check_result.get("subscribed"):
                continue
        visible.append(sponsor)

    return visible


def build_sponsor_keyboard(sponsors: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    current_row = []

    for index, sponsor in enumerate(sponsors, start=1):
        current_row.append(
            InlineKeyboardButton(
                text=f"Спонсор №{index}",
                url=sponsor["link"],
            )
        )
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []

    if current_row:
        rows.append(current_row)

    rows.append([InlineKeyboardButton(text="✅ Я подписан", callback_data="check_sponsors")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def show_sponsors(message: Message, state: FSMContext) -> None:
    try:
        visible = await load_visible_sponsors(message.from_user.id)
    except Exception:
        await message.answer("Neo Link временно недоступен. Попробуйте позже.")
        return

    await state.update_data(visible_sponsors=visible)

    if not visible:
        await message.answer("Сейчас активных спонсоров для показа нет.")
        return

    text = (
        "Чтобы продолжить пользоваться ботом, пожалуйста,\n"
        "подпишись на следующие ресурсы."
    )
    await message.answer(text, reply_markup=build_sponsor_keyboard(visible))


@router.callback_query(F.data == "check_sponsors")
async def check_sponsors(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    sponsors = data.get("visible_sponsors", [])

    if not sponsors:
        await callback.answer("Список спонсоров пуст. Обновите экран.", show_alert=True)
        return

    not_completed = 0
    credited_count = 0
    duplicate_count = 0

    for sponsor in sponsors:
        if sponsor.get("requires_check"):
            check_result = await neolink_request(
                "check-member",
                {
                    "user_id": callback.from_user.id,
                    "sponsor_chat_id": sponsor["sponsor_chat_id"],
                },
            )
            if not check_result.get("subscribed"):
                not_completed += 1
                continue

        register_result = await neolink_request(
            "register-subscription",
            {
                "user_id": callback.from_user.id,
                "order_id": sponsor["order_id"],
                "sponsor_chat_id": sponsor["sponsor_chat_id"],
                "sponsor_name": sponsor.get("title"),
                "charge_amount": sponsor.get("charge_amount", 1),
            },
        )

        if register_result.get("credited"):
            credited_count += 1
        elif register_result.get("duplicate"):
            duplicate_count += 1

    if not_completed:
        await callback.answer(
            f"Не все подписки выполнены. Осталось: {not_completed}",
            show_alert=True,
        )
        return

    if credited_count:
        await callback.answer(
            f"Подписки подтверждены. Начислено: {credited_count}.",
            show_alert=True,
        )
        return

    if duplicate_count:
        await callback.answer(
            "Подписка уже была подтверждена ранее.",
            show_alert=True,
        )
        return

    await callback.answer("Новых начислений нет.", show_alert=True)
```

## 7. Что важно обработать в своём боте

Обязательно обработайте:

- неверный `API key`
- таймаут запроса
- ошибку сети
- недоступность Neo Link API
- ситуацию, когда пользователь нажал `Я подписан`, но реально не подписался
- ситуацию, когда `register-subscription` вернул `duplicate`

Минимальный пример:

```python
try:
    sponsors = await load_visible_sponsors(user_id)
except Exception:
    await message.answer("Neo Link временно недоступен. Попробуйте позже.")
    return
```

## 8. Как работает повторная подписка

Если пользователь:

1. подписался,
2. был засчитан,
3. отписался,
4. с владельца было списание,
5. потом этот же пользователь снова подписался,

то повторное начисление возможно снова.

При этом:

- владельцу снова начисляется награда,
- но `done` у заказа второй раз не растёт, потому что это не новый уникальный подписчик, а возврат старого.

## 9. Что проверить перед запуском

Проверьте руками:

1. Бот получает список спонсоров.
2. Уже подписанные каналы не показываются.
3. Кнопки `Спонсор №...` открывают правильные ссылки.
4. Кнопка `✅ Я подписан` действительно проверяет подписку.
5. После подтверждения бот продолжает основной сценарий.
6. При `duplicate` бот не ломается и не считает это сетевой ошибкой.
7. Если Neo Link и бот запущены на разных машинах, используется не `127.0.0.1`, а реальный адрес API.
