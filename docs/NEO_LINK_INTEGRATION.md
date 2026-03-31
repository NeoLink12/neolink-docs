# Neo Link: как подключить интеграцию в свой Telegram-бот

Только практическая инструкция: что получить в Neo Link и какой код вставить в своего бота.

## 1. Что нужно получить

В Neo Link:

1. Откройте `Продажа трафика`
2. Нажмите `Добавить нового бота`
3. Отправьте токен вашего бота
4. Получите `API key`
5. Дождитесь модерации

После этого у вас должно быть:

- `BOT_TOKEN`
- `NEOLINK_API_KEY`
- `NEOLINK_BASE_URL`

Пример:

```python
BOT_TOKEN = "YOUR_BOT_TOKEN"
NEOLINK_API_KEY = "YOUR_NEO_LINK_API_KEY"
NEOLINK_BASE_URL = "http://127.0.0.1:8080/api/neolink"
```

Локально при запущенном [main.py](C:\Users\Admin\Documents\neos\main.py) адрес Neo Link API такой:

```python
NEOLINK_BASE_URL = "http://127.0.0.1:8080/api/neolink"
```

Если потом вынесете API на сервер, тогда заменяете его на свой домен:

```python
NEOLINK_BASE_URL = "https://your-domain.com/api/neolink"
```

## 2. HTTP-клиент Neo Link

```python
import aiohttp


async def neolink_request(endpoint: str, payload: dict):
    body = {"api_key": NEOLINK_API_KEY, **payload}
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{NEOLINK_BASE_URL}/{endpoint}",
            json=body,
            timeout=15,
        ) as resp:
            resp.raise_for_status()
            return await resp.json()
```

## 3. Какие методы вызывает ваш бот

Получить спонсоров:

```http
POST /api/neolink/get-sponsors
```

```json
{
  "api_key": "YOUR_API_KEY",
  "user_id": 123456789
}
```

Проверить подписку:

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

Подтвердить подписку:

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

## 4. Важное правило показа

Если пользователь уже подписан на канал, этот спонсор не должен показываться в блоке.

Логика должна быть такой:

1. Получили список спонсоров из Neo Link
2. Для каждого канала с проверкой вызвали `check-member`
3. Если `subscribed = true`, этот спонсор скрывается
4. Показываются только те спонсоры, на которые пользователь ещё не подписан

Подписка считается подтверждённой только если статус:

- `member`
- `administrator`
- `creator`

Если пришёл `left`, `kicked` или ошибка сети, подтверждать подписку нельзя.

## 5. Готовый блок заданий

Ниже готовый пример под ваш формат: кнопки спонсоров и одна кнопка `Я подписан`.

```python
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


async def load_visible_sponsors(user_id: int):
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
```

Текст сообщения:

```python
text = (
    "Чтобы продолжить пользоваться ботом, пожалуйста,\\n"
    "подпишись на следующие ресурсы! 🤠"
)
```

## 6. Готовая проверка по кнопке `Я подписан`

```python
@router.callback_query(F.data == "check_sponsors")
async def check_sponsors(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    sponsors = data.get("visible_sponsors", [])

    not_completed = 0

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

        await neolink_request(
            "register-subscription",
            {
                "user_id": callback.from_user.id,
                "order_id": sponsor["order_id"],
                "sponsor_chat_id": sponsor["sponsor_chat_id"],
                "sponsor_name": sponsor.get("title"),
                "charge_amount": sponsor.get("charge_amount", 1),
            },
        )

    if not_completed:
        await callback.answer(
            f"Не все подписки выполнены. Осталось: {not_completed}",
            show_alert=True,
        )
        return

    await callback.answer("Подписки подтверждены.", show_alert=True)
```

## 7. Полный сценарий работы

Ваш бот должен делать так:

1. Пользователь нажал `/start`
2. Бот запросил `get-sponsors`
3. Бот убрал из списка уже подписанные каналы
4. Бот показал блок кнопок спонсоров
5. Пользователь перешёл по кнопкам и подписался
6. Пользователь нажал `✅ Я подписан`
7. Бот вызвал `check-member`
8. Бот вызвал `register-subscription`
9. После этого бот продолжил основной сценарий

## 8. Что обязательно обработать

- невалидный `API key`
- таймаут запроса
- ошибку сети
- недоступность API
- случай, когда пользователь не подписался

Пример:

```python
try:
    sponsors = await load_visible_sponsors(user_id)
except Exception:
    await message.answer("Neo Link временно недоступен. Попробуйте позже.")
    return
```

## 9. Что проверить перед запуском

Проверьте руками:

1. бот получает список спонсоров
2. уже подписанные каналы не показываются
3. кнопки `Спонсор №...` открывают нужные ссылки
4. кнопка `✅ Я подписан` действительно проверяет подписку
5. после подтверждения бот продолжает работу
