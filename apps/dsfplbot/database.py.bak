"""
database.py — Работа с собственной БД бота (/data/dsfpl.db)
Исправления:
- Таблица user_fpl (не fpl_links)
- Функция ensure_user_fpl_table
- Асинхронные функции с proper error handling
"""

import aiosqlite
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


async def init_db(db_path: str):
    """Инициализация собственной БД бота"""
    try:
        async with aiosqlite.connect(db_path) as db:
            # Таблица scores для игр (/fun)
            await db.execute('''
                CREATE TABLE IF NOT EXISTS scores (
                    user_id INTEGER,
                    game_type TEXT,
                    score INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, game_type)
                )
            ''')
            await db.commit()
            logger.info(f"Database initialized: {db_path}")
    except Exception as e:
        logger.error(f"Error initializing database: {e}", exc_info=True)
        raise


async def ensure_user_fpl_table(db_path: str):
    """
    Создание таблицы user_fpl для привязки Telegram ID к FPL ID
    Вызывается из main.py в post_init
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS user_fpl (
                    telegram_id INTEGER PRIMARY KEY,
                    fpl_id INTEGER NOT NULL,
                    linked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            await db.commit()
            logger.info("user_fpl table ensured")
    except Exception as e:
        logger.error(f"Error creating user_fpl table: {e}", exc_info=True)
        raise


async def save_user_fpl_id(db_path: str, telegram_id: int, fpl_id: int):
    """
    Сохранение привязки Telegram ID → FPL ID
    ИСПРАВЛЕНО: пишет в user_fpl (не fpl_links)
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                'INSERT OR REPLACE INTO user_fpl (telegram_id, fpl_id, linked_at) VALUES (?, ?, ?)',
                (telegram_id, fpl_id, datetime.now())
            )
            await db.commit()
            logger.info(f"Saved FPL link: telegram_id={telegram_id}, fpl_id={fpl_id}")
    except Exception as e:
        logger.error(f"Error saving FPL ID: {e}", exc_info=True)
        raise


async def get_user_fpl_id(db_path: str, telegram_id: int) -> int | None:
    """Получение FPL ID по Telegram ID"""
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                'SELECT fpl_id FROM user_fpl WHERE telegram_id = ?',
                (telegram_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None
    except Exception as e:
        logger.error(f"Error getting FPL ID: {e}", exc_info=True)
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Функции для /fun (scores)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def add_score(db_path: str, user_id: int, game_type: str, points: int):
    """Добавление/обновление очков игры"""
    try:
        async with aiosqlite.connect(db_path) as db:
            # Получаем текущий счёт
            async with db.execute(
                'SELECT score FROM scores WHERE user_id = ? AND game_type = ?',
                (user_id, game_type)
            ) as cursor:
                row = await cursor.fetchone()
                current_score = row[0] if row else 0
            
            # Обновляем
            new_score = current_score + points
            await db.execute(
                'INSERT OR REPLACE INTO scores (user_id, game_type, score, timestamp) VALUES (?, ?, ?, ?)',
                (user_id, game_type, new_score, datetime.now())
            )
            await db.commit()
            logger.info(f"Added score: user={user_id}, game={game_type}, points={points}, new_total={new_score}")
            return new_score
    except Exception as e:
        logger.error(f"Error adding score: {e}", exc_info=True)
        return None


async def get_scores(db_path: str, game_type: str) -> list[tuple[int, int]]:
    """
    Получение таблицы очков для игры
    Returns: [(user_id, score), ...]
    """
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                'SELECT user_id, score FROM scores WHERE game_type = ? ORDER BY score DESC',
                (game_type,)
            ) as cursor:
                rows = await cursor.fetchall()
                return rows
    except Exception as e:
        logger.error(f"Error getting scores: {e}", exc_info=True)
        return []


async def get_user_score(db_path: str, user_id: int, game_type: str) -> int:
    """Получение очков пользователя в игре"""
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute(
                'SELECT score FROM scores WHERE user_id = ? AND game_type = ?',
                (user_id, game_type)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
    except Exception as e:
        logger.error(f"Error getting user score: {e}", exc_info=True)
        return 0


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Заглушки для совместимости (если используются в main.py)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def import_legacy_csv(db_path: str, csv_path: str):
    """
    Заглушка для импорта legacy CSV
    Если не используется — удалить импорт из main.py
    """
    logger.info(f"import_legacy_csv called (stub): {csv_path}")
    pass
