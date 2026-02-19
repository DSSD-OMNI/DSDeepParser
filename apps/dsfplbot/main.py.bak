#!/usr/bin/env python3
import os
import sys
import asyncio
import signal
import logging
import datetime
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
logger.info("=== BOT STARTING (healthcheck-first version) ===")

async def healthcheck(request):
    return web.Response(text="OK")

async def run_healthcheck():
    port = int(os.getenv("PORT", 8080))
    app = web.Application()
    app.router.add_get("/", healthcheck)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Healthcheck server started on port {port}")
    await asyncio.Event().wait()

async def run_healthcheck_and_bot():
    healthcheck_task = asyncio.create_task(run_healthcheck())
    await asyncio.sleep(1)

    logger.info("Importing modules...")
    try:
        from config import TOKEN, FPL_LEAGUE_ID, ADMIN_USER_ID
        from database import init_db, import_legacy_csv, init_fpl_links_table, init_games_tables
        from handlers import (
            start, link_start, link_get_id, link_cancel,
            afterdl, aftertour,
            dssdtempo_start, dssdtempo_get_weeks, dssdtempo_cancel,
            dssdadvice, export_data
        )
        from fun import fun, fun_callback, dq, answer, gtd, predictions, scoreboard, daily_quiz_job
        from halloffame import halloffame, hof_callback
        from other import other, other_callback
        if os.getenv("DISABLE_PARSER") != "1":
            from fpl_parser.main import FPLUltimateParser
        logger.info("All modules imported successfully")
    except Exception as e:
        logger.exception("Module import failed, but healthcheck keeps running")
        await asyncio.Event().wait()
        return

    from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ConversationHandler, MessageHandler, filters
    from telegram.request import HTTPXRequest

    parser_task = None
    application = None
    WEEKS = 1
    LINK_FPL = 2

    async def post_init(app):
        nonlocal parser_task
        await init_db()
        await init_fpl_links_table()
        await init_games_tables()
        try:
            await import_legacy_csv("FPL League History.csv")
        except Exception as e:
            logger.error(f"CSV import failed: {e}")
        if os.getenv("DISABLE_PARSER") != "1" and 'FPLUltimateParser' in globals():
            try:
                parser = FPLUltimateParser(config_path="fpl_parser/config.json")
                parser_task = asyncio.create_task(parser.run_24_7())
                logger.info("Parser started")
            except Exception as e:
                logger.error(f"Failed to start parser: {e}")
        else:
            logger.info("Parser disabled by env var or not available")

    async def shutdown_handler(sig, frame):
        logger.info("Shutting down...")
        if parser_task:
            parser_task.cancel()
            try:
                await parser_task
            except asyncio.CancelledError:
                pass
        if application:
            await application.stop()
            await application.shutdown()

    request = HTTPXRequest(connect_timeout=30, read_timeout=30)
    application = Application.builder().token(TOKEN).request(request).post_init(post_init).build()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown_handler(sig, None)))

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("afterdl", afterdl))
    application.add_handler(CommandHandler("aftertour", aftertour))
    application.add_handler(CommandHandler("dssdadvice", dssdadvice))
    application.add_handler(CommandHandler("fun", fun))
    application.add_handler(CommandHandler("halloffame", halloffame))
    application.add_handler(CommandHandler("other", other))
    application.add_handler(CommandHandler("export_data", export_data))
    application.add_handler(CommandHandler("dq", dq))
    application.add_handler(CommandHandler("answer", answer))
    application.add_handler(CommandHandler("gtd", gtd))
    application.add_handler(CommandHandler("predictions", predictions))
    application.add_handler(CommandHandler("scoreboard", scoreboard))

    dssdtempo_conv = ConversationHandler(
        entry_points=[CommandHandler("dssdtempo", dssdtempo_start)],
        states={WEEKS: [MessageHandler(filters.TEXT & ~filters.COMMAND, dssdtempo_get_weeks)]},
        fallbacks=[CommandHandler("cancel", dssdtempo_cancel)]
    )
    application.add_handler(dssdtempo_conv)

    link_conv = ConversationHandler(
        entry_points=[CommandHandler("link", link_start)],
        states={LINK_FPL: [MessageHandler(filters.TEXT & ~filters.COMMAND, link_get_id)]},
        fallbacks=[CommandHandler("cancel", link_cancel)]
    )
    application.add_handler(link_conv)

    application.add_handler(CallbackQueryHandler(fun_callback, pattern="^fun_"))
    application.add_handler(CallbackQueryHandler(hof_callback, pattern="^hof_"))
    application.add_handler(CallbackQueryHandler(other_callback, pattern="^other_|^notif_toggle|^remind_toggle"))

    job_queue = application.job_queue
    if job_queue:
        job_queue.run_daily(daily_quiz_job, time=datetime.time(hour=12, minute=0), days=tuple(range(7)))

    await application.initialize()
    await application.start()
    logger.info("Bot started")

    await application.updater.start_polling()
    logger.info("Polling started")

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(run_healthcheck_and_bot())
    except Exception as e:
        logger.exception("Fatal error in main")
        sys.exit(1)
