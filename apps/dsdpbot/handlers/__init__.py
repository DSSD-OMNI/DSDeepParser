import asyncio
import logging
import subprocess
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from config import ADMIN_IDS, PARSER_ROOT

logger = logging.getLogger(__name__)

async def is_admin(update: Update) -> bool:
    return update.effective_user.id in ADMIN_IDS

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_admin(update):
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    keyboard = [
        [InlineKeyboardButton("📊 Статус парсера", callback_data="status")],
        [InlineKeyboardButton("🚀 Запустить ETL", callback_data="run_etl")],
        [InlineKeyboardButton("📈 Статистика БД", callback_data="db_stats")],
        [InlineKeyboardButton("🔄 Обновить Elo", callback_data="run_elo")],
        [InlineKeyboardButton("🔍 Последние ошибки", callback_data="errors")],
        [InlineKeyboardButton("💾 Создать бэкап", callback_data="backup")],
    ]
    await update.message.reply_text(
        "👋 Добро пожаловать в DSDPBot – панель управления DSDeepParser.\n"
        "Выберите действие:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not await is_admin(update):
        await query.edit_message_text("⛔ Доступ запрещён.")
        return

    if query.data == "status":
        await show_status(query)
    elif query.data == "run_etl":
        await run_etl(query)
    elif query.data == "db_stats":
        await db_stats(query)
    elif query.data == "run_elo":
        await run_elo(query)
    elif query.data == "errors":
        await show_errors(query)
    elif query.data == "backup":
        await create_backup(query)

async def show_status(query):
    try:
        logs = subprocess.check_output(
            ["railway", "logs", "-s", "DSDeepParser", "-n", "20"],
            text=True, timeout=10
        )
        msg = f"📡 Последние 20 строк логов DSDeepParser:\n<pre>{logs[-1500:]}</pre>"
    except Exception as e:
        msg = f"❌ Не удалось получить логи: {e}"
    await query.edit_message_text(msg, parse_mode="HTML")

async def run_etl(query):
    await query.edit_message_text("🔄 Запускаю ETL (это может занять несколько минут)...")
    asyncio.create_task(_run_etl_task(query))

async def _run_etl_task(query):
    try:
        import sys
        sys.path.insert(0, PARSER_ROOT)
        from packages.core.etl import run_etl_for_current_gw
        run_etl_for_current_gw()
        await query.edit_message_text("✅ ETL завершён успешно.")
    except Exception as e:
        await query.edit_message_text(f"❌ Ошибка ETL: {e}")

async def db_stats(query):
    try:
        cmd = "railway ssh -s DSDeepParser 'sqlite3 /app/data/fpl_data.db \"SELECT COUNT(*) FROM league_standings_1125782;\"'"
        count_league = subprocess.check_output(cmd, shell=True, text=True, timeout=10).strip()
        cmd = "railway ssh -s DSDeepParser 'sqlite3 /app/data/fpl_data.db \"SELECT COUNT(*) FROM features;\"'"
        count_features = subprocess.check_output(cmd, shell=True, text=True, timeout=10).strip()
        cmd = "railway ssh -s DSDeepParser 'sqlite3 /app/data/fpl_data.db \"SELECT COUNT(*) FROM lri_scores;\"'"
        count_lri = subprocess.check_output(cmd, shell=True, text=True, timeout=10).strip()
        msg = (
            f"📊 **Статистика базы данных**\n\n"
            f"• league_standings_1125782: **{count_league}** записей\n"
            f"• features: **{count_features}** записей\n"
            f"• lri_scores: **{count_lri}** записей"
        )
    except Exception as e:
        msg = f"❌ Ошибка получения статистики: {e}"
    await query.edit_message_text(msg, parse_mode="Markdown")

async def run_elo(query):
    await query.edit_message_text("🔄 Обновляю Elo-рейтинги...")
    asyncio.create_task(_run_elo_task(query))

async def _run_elo_task(query):
    try:
        import sys
        sys.path.insert(0, PARSER_ROOT)
        from apps.dsdeepparser.sources.elo import update_team_elo
        await update_team_elo()
        await query.edit_message_text("✅ Elo успешно обновлён.")
    except Exception as e:
        await query.edit_message_text(f"❌ Ошибка обновления Elo: {e}")

async def show_errors(query):
    try:
        logs = subprocess.check_output(
            ["railway", "logs", "-s", "DSDeepParser", "-n", "100"],
            text=True, timeout=10
        )
        errors = [line for line in logs.split('\n') if 'ERROR' in line or 'Traceback' in line]
        if errors:
            msg = "🚨 **Последние ошибки**\n" + "\n".join(errors[-10:])
        else:
            msg = "✅ Ошибок не обнаружено."
    except Exception as e:
        msg = f"❌ Ошибка получения логов: {e}"
    await query.edit_message_text(msg, parse_mode="Markdown")

async def create_backup(query):
    await query.edit_message_text("💾 Создаю резервную копию базы данных...")
    asyncio.create_task(_backup_task(query))

async def _backup_task(query):
    try:
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"/app/data/backup_{timestamp}.sql"
        cmd = f"railway ssh -s DSDeepParser 'sqlite3 /app/data/fpl_data.db \".dump\" > {backup_file}'"
        subprocess.run(cmd, shell=True, check=True, timeout=30)
        await query.edit_message_text(f"✅ Бэкап сохранён на сервере: `{backup_file}`", parse_mode="Markdown")
    except Exception as e:
        await query.edit_message_text(f"❌ Ошибка создания бэкапа: {e}")

async def error_watcher(context: ContextTypes.DEFAULT_TYPE):
    try:
        logs = subprocess.check_output(
            ["railway", "logs", "-s", "DSDeepParser", "-n", "100"],
            text=True, timeout=10
        )
        errors = [line for line in logs.split('\n') if 'ERROR' in line or 'Traceback' in line]
        if errors:
            for admin_id in ADMIN_IDS:
                await context.bot.send_message(
                    admin_id,
                    f"🚨 В парсере обнаружены ошибки!\n{errors[-5]}"
                )
    except Exception:
        pass
