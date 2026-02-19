#!/usr/bin/env python3
import os
import logging
from telegram.ext import Application, CommandHandler, CallbackQueryHandler
from handlers import start, button_handler, error_watcher
from config import TELEGRAM_TOKEN, ADMIN_IDS, setup_logging

def main():
    setup_logging()
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    job_queue = app.job_queue
    job_queue.run_repeating(error_watcher, interval=21600, first=10)
    
    logging.info("Бот запущен и готов к работе.")
    app.run_polling()

if __name__ == "__main__":
    main()
