import asyncio
import logging

from telegram import Update

from checksub_bot import build_checksub_bot
from database import DatabaseManager
from main_bot import build_main_bot
from neolink_api import start_neolink_api
from traffic_bot import build_traffic_bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def run_all_bots():
    DatabaseManager.init()

    api_runner = await start_neolink_api()
    main_app = build_main_bot()
    traffic_app = build_traffic_bot()
    checksub_app = build_checksub_bot()

    logger.info("Инициализация всех ботов и Neo Link API...")

    await main_app.initialize()
    await traffic_app.initialize()
    await checksub_app.initialize()

    await main_app.start()
    await traffic_app.start()
    await checksub_app.start()

    await main_app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    await traffic_app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    await checksub_app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

    logger.info("Все три бота и Neo Link API запущены")

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка сервисов...")
    finally:
        await main_app.updater.stop()
        await traffic_app.updater.stop()
        await checksub_app.updater.stop()
        await main_app.stop()
        await traffic_app.stop()
        await checksub_app.stop()
        await main_app.shutdown()
        await traffic_app.shutdown()
        await checksub_app.shutdown()
        await api_runner.cleanup()


def main():
    asyncio.run(run_all_bots())


if __name__ == "__main__":
    main()
