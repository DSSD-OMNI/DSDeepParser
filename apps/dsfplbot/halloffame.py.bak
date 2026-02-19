"""
halloffame.py — Hall of Fame (история чемпионов лиги)
Исправления:
- Правильный путь к CSV
- Обработка ошибок
- Логирование
"""

import os
import csv
import logging
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

# Путь к CSV относительно текущего файла
CSV_PATH = os.path.join(os.path.dirname(__file__), 'FPL League History.csv')


async def halloffame_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /halloffame — показать историю чемпионов лиги
    Читает из CSV файла
    """
    try:
        logger.info(f"Hall of Fame requested, CSV path: {CSV_PATH}")
        
        # Проверка существования файла
        if not os.path.exists(CSV_PATH):
            await update.message.reply_text(
                "❌ Файл с историей лиги не найден.\n\n"
                f"_Ожидаемый путь: {CSV_PATH}_",
                parse_mode='Markdown'
            )
            logger.error(f"CSV file not found: {CSV_PATH}")
            return
        
        # Чтение CSV
        champions = []
        with open(CSV_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                champions.append({
                    'season': row.get('Season', 'N/A'),
                    'winner': row.get('Winner', 'N/A'),
                    'points': row.get('Points', 'N/A')
                })
        
        if not champions:
            await update.message.reply_text("❌ История лиги пуста")
            return
        
        # Форматирование таблицы
        text = "🏆 *Hall of Fame — les mutants*\n\n"
        text += "```\n"
        text += f"{'Сезон':<12} {'Чемпион':<20} {'Очки':<8}\n"
        text += "─" * 45 + "\n"
        
        for champ in champions:
            text += f"{champ['season']:<12} {champ['winner']:<20} {champ['points']:<8}\n"
        
        text += "```"
        
        await update.message.reply_text(text, parse_mode='Markdown')
        logger.info(f"Hall of Fame sent: {len(champions)} seasons")
    
    except Exception as e:
        logger.error(f"Error in halloffame_handler: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ Ошибка при чтении истории лиги.\n\n"
            f"_Проверьте формат CSV файла_",
            parse_mode='Markdown'
        )


async def halloffame_add_season(season: str, winner: str, points: int):
    """
    Добавление нового сезона в Hall of Fame
    Используется администратором
    """
    try:
        # Проверка существования файла
        file_exists = os.path.exists(CSV_PATH)
        
        with open(CSV_PATH, 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            # Если файл новый — пишем header
            if not file_exists:
                writer.writerow(['Season', 'Winner', 'Points'])
            
            # Добавляем строку
            writer.writerow([season, winner, points])
        
        logger.info(f"Added to Hall of Fame: {season}, {winner}, {points}")
        return True
    
    except Exception as e:
        logger.error(f"Error adding to Hall of Fame: {e}", exc_info=True)
        return False
