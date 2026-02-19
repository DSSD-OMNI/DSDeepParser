"""
fun.py — Игровые функции и развлечения
Исправления:
- Добавлена заглушка daily_quiz_job
- DoubleQuiz проверен
- GTD и predictions — заглушки
"""

import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import random

logger = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DoubleQuiz — викторина с вопросами
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DOUBLE_QUIZ_QUESTIONS = [
    {
        "question": "Кто выиграл Премьер-Лигу 2015/16?",
        "options": ["Leicester", "Chelsea", "Arsenal", "Man City"],
        "correct": 0
    },
    {
        "question": "Сколько голов Салах забил в сезоне 2017/18?",
        "options": ["28", "32", "35", "38"],
        "correct": 1
    },
    {
        "question": "Кто лучший бомбардир в истории АПЛ?",
        "options": ["Shearer", "Rooney", "Cole", "Henry"],
        "correct": 0
    },
    {
        "question": "В каком году основана Премьер-Лига?",
        "options": ["1988", "1990", "1992", "1994"],
        "correct": 2
    },
    {
        "question": "Какая команда дольше всех держалась в топе без вылета?",
        "options": ["Arsenal", "Man Utd", "Liverpool", "Chelsea"],
        "correct": 0
    }
]


async def doublequiz_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик команды /dq — начать викторину
    Выдаёт случайный вопрос с кнопками
    """
    try:
        user_id = update.effective_user.id
        
        # Выбираем случайный вопрос
        question_data = random.choice(DOUBLE_QUIZ_QUESTIONS)
        question_idx = DOUBLE_QUIZ_QUESTIONS.index(question_data)
        
        # Сохраняем в context для проверки ответа
        context.user_data['dq_question_idx'] = question_idx
        context.user_data['dq_correct'] = question_data['correct']
        
        # Создаём кнопки
        keyboard = [
            [InlineKeyboardButton(opt, callback_data=f"dq_answer_{i}")]
            for i, opt in enumerate(question_data['options'])
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"🎯 *DoubleQuiz*\n\n{question_data['question']}",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        logger.info(f"DoubleQuiz started for user {user_id}, question {question_idx}")
    
    except Exception as e:
        logger.error(f"Error in doublequiz_handler: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при загрузке вопроса")


async def doublequiz_answer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик ответа на вопрос DoubleQuiz
    Callback data: dq_answer_0, dq_answer_1, etc.
    """
    try:
        query = update.callback_query
        await query.answer()
        
        user_id = update.effective_user.id
        selected_idx = int(query.data.split('_')[-1])
        correct_idx = context.user_data.get('dq_correct')
        
        if correct_idx is None:
            await query.edit_message_text("❌ Вопрос не найден. Начните заново с /dq")
            return
        
        # Проверка ответа
        if selected_idx == correct_idx:
            points = 10
            result_text = "✅ *Правильно!* +10 очков"
            
            # Начисляем очки
            from database import add_score
            from config import DB_PATH
            await add_score(DB_PATH, user_id, 'doublequiz', points)
        else:
            result_text = f"❌ *Неправильно!* Правильный ответ: {DOUBLE_QUIZ_QUESTIONS[context.user_data['dq_question_idx']]['options'][correct_idx]}"
        
        await query.edit_message_text(result_text, parse_mode='Markdown')
        
        # Очищаем context
        context.user_data.pop('dq_question_idx', None)
        context.user_data.pop('dq_correct', None)
        
        logger.info(f"DoubleQuiz answer: user={user_id}, selected={selected_idx}, correct={correct_idx}")
    
    except Exception as e:
        logger.error(f"Error in doublequiz_answer_callback: {e}", exc_info=True)
        await query.edit_message_text("❌ Ошибка при проверке ответа")


async def scoreboard_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Показать таблицу очков для всех игр
    """
    try:
        from database import get_scores
        from config import DB_PATH
        
        # Получаем очки для DoubleQuiz
        dq_scores = await get_scores(DB_PATH, 'doublequiz')
        
        if not dq_scores:
            await update.message.reply_text("📊 Таблица пуста. Сыграйте в /dq!")
            return
        
        # Форматируем таблицу
        text = "📊 *Scoreboard — DoubleQuiz*\n\n"
        for i, (user_id, score) in enumerate(dq_scores, 1):
            try:
                user = await context.bot.get_chat(user_id)
                name = user.first_name or f"User {user_id}"
            except:
                name = f"User {user_id}"
            
            emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            text += f"{emoji} {name}: *{score}* pts\n"
        
        await update.message.reply_text(text, parse_mode='Markdown')
        logger.info(f"Scoreboard shown, {len(dq_scores)} players")
    
    except Exception as e:
        logger.error(f"Error in scoreboard_handler: {e}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при загрузке таблицы")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Заглушки для других игр
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def gtd_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Guess The Difference — в разработке"""
    await update.message.reply_text("🎮 Guess The Difference — скоро!\n\n_Игра в разработке_", parse_mode='Markdown')
    logger.info("GTD called (stub)")


async def predictions_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Predictions game — в разработке"""
    await update.message.reply_text("🔮 Predictions — скоро!\n\n_Игра в разработке_", parse_mode='Markdown')
    logger.info("Predictions game called (stub)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Заглушка для daily_quiz_job (если используется в main.py)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def daily_quiz_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Job для ежедневной викторины
    Если не используется — удалить из main.py
    """
    logger.info("daily_quiz_job called (stub)")
    pass
