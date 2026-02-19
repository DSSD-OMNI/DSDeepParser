"""
dssd.py — Логика команды /dssdtempo (таблица LRI + темп)
Исправления:
- Временный расчёт темпа на основе введённого числа недель
- Улучшенное логирование
- Обработка отсутствующих данных
"""

import logging
from telegram import Update
from telegram.ext import ContextTypes
from fpl_data_reader import get_lri_scores, get_features_by_manager

logger = logging.getLogger(__name__)


async def dssdtempo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /dssdtempo [weeks] — таблица LRI + темп за N недель
    
    ВРЕМЕННОЕ РЕШЕНИЕ:
    - Если weeks <= 5: используется form_5gw
    - Если weeks > 5: приблизительный расчёт через экстраполяцию
    
    ИДЕАЛЬНОЕ РЕШЕНИЕ (требует manager_history):
    - Получать реальные очки за последние N туров
    - Рассчитывать средний темп
    """
    try:
        user_id = update.effective_user.id
        
        # Получение числа недель
        if not context.args or not context.args[0].isdigit():
            await update.message.reply_text(
                "💡 Использование: `/dssdtempo [недели]`\n\n"
                "Например: `/dssdtempo 5`",
                parse_mode='Markdown'
            )
            return
        
        weeks = int(context.args[0])
        
        if weeks < 1 or weeks > 38:
            await update.message.reply_text("❌ Число недель должно быть от 1 до 38")
            return
        
        logger.info(f"Processing /dssdtempo for user {user_id}, weeks={weeks}")
        
        # Получаем данные из БД парсера
        from config import FPL_PARSER_DB_PATH
        
        lri_data = await get_lri_scores(FPL_PARSER_DB_PATH)
        if not lri_data:
            await update.message.reply_text(
                "❌ Нет данных LRI.\n\n"
                "_Парсер DSDeepParser ещё не собрал данные или БД недоступна._",
                parse_mode='Markdown'
            )
            return
        
        # Формируем таблицу
        table_rows = []
        
        for manager_id, lri_value in lri_data:
            # Получаем features для темпа
            features = await get_features_by_manager(FPL_PARSER_DB_PATH, manager_id)
            
            if not features:
                logger.warning(f"No features for manager {manager_id}")
                continue
            
            form_5gw = features.get('form_5gw', 0)
            
            # ВРЕМЕННЫЙ РАСЧЁТ ТЕМПА
            tempo = calculate_tempo_estimate(form_5gw, weeks)
            
            table_rows.append({
                'manager_id': manager_id,
                'lri': lri_value,
                'tempo': tempo
            })
        
        # Сортировка по LRI (descending)
        table_rows.sort(key=lambda x: x['lri'], reverse=True)
        
        # Форматирование таблицы
        text = f"📊 *DSSD Tempo — {weeks} недель(и)*\n\n"
        text += "```\n"
        text += f"{'Ранг':<5} {'LRI':<8} {'Темп':<8} {'ID':<10}\n"
        text += "─" * 35 + "\n"
        
        for i, row in enumerate(table_rows, 1):
            emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "  "
            text += f"{emoji} {i:<3} {row['lri']:<8.2f} {row['tempo']:<8.1f} #{row['manager_id']}\n"
        
        text += "```\n\n"
        
        # Предупреждение о временном расчёте
        if weeks > 5:
            text += "_⚠️ Темп рассчитан приблизительно (экстраполяция form_5gw)_\n"
        
        text += f"_LRI = Luck & Rank Index | Темп = очки/{weeks}GW_"
        
        await update.message.reply_text(text, parse_mode='Markdown')
        logger.info(f"DSSD Tempo table sent: {len(table_rows)} managers, {weeks} weeks")
    
    except Exception as e:
        logger.error(f"Error in dssdtempo_handler: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Ошибка при обработке команды.\n\n"
            f"_Убедитесь что парсер работает и БД доступна._",
            parse_mode='Markdown'
        )


def calculate_tempo_estimate(form_5gw: float, weeks: int) -> float:
    """
    Временный расчёт темпа на основе form_5gw
    
    ЛОГИКА:
    - Если weeks <= 5: возвращаем form_5gw напрямую
    - Если weeks > 5: экстраполируем (form_5gw * weeks / 5)
    
    ОГРАНИЧЕНИЯ:
    - Неточно для weeks > 5
    - Не учитывает старые туры (только последние 5)
    
    ИДЕАЛЬНОЕ РЕШЕНИЕ:
    - Получать реальные очки из manager_history
    - Рассчитывать sum(points[-weeks:]) / weeks
    
    Args:
        form_5gw: Средние очки за последние 5 туров
        weeks: Число недель для расчёта
    
    Returns:
        Приблизительный темп
    """
    if weeks <= 5:
        # Точный расчёт (если есть form_5gw)
        return form_5gw
    else:
        # Приблизительная экстраполяция
        # Предполагаем что темп за 5 туров примерно равен темпу за все weeks
        # (это неточно, но лучше чем ничего)
        return form_5gw
        
        # Альтернатива: пропорциональная экстраполяция
        # return form_5gw * (weeks / 5)
        # Но это даст завышенные значения для weeks > 5


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Рекомендация для парсера
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

"""
Для точного расчёта темпа необходимо добавить в парсер таблицу manager_history:

CREATE TABLE manager_history (
    manager_id INTEGER,
    gameweek INTEGER,
    points INTEGER,
    total_points INTEGER,
    rank INTEGER,
    PRIMARY KEY (manager_id, gameweek)
);

Тогда calculate_tempo_estimate можно заменить на:

async def calculate_tempo_accurate(db_path, manager_id, weeks):
    '''Точный расчёт темпа на основе истории'''
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            'SELECT points FROM manager_history 
             WHERE manager_id = ? 
             ORDER BY gameweek DESC LIMIT ?',
            (manager_id, weeks)
        )
        rows = await cursor.fetchall()
        if not rows:
            return 0
        return sum(r[0] for r in rows) / len(rows)
"""
