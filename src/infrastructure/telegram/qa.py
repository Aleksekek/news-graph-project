"""
Логика вопросов-ответов для Telegram бота.
"""

import logging
from typing import Dict, List

from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from src.database.repositories.article_repository import ArticleRepository
from src.processing.llm.deepseek import DeepSeekAnalyzer

logger = logging.getLogger(__name__)


class QAHandlers:
    """Обработчики вопросов."""

    def __init__(self, article_repo: ArticleRepository, llm: DeepSeekAnalyzer):
        self.article_repo = article_repo
        self.llm = llm

    async def ask_prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Запрос вопроса."""
        query = update.callback_query
        await query.edit_message_text(
            "🤖 *Задать вопрос*\n\n"
            "Напишите ваш вопрос о текущей новостной повестке:\n\n"
            "Примеры:\n"
            "• Что происходит с рублём?\n"
            "• Какие главные новости за сегодня?"
        )
        await query.answer()
        return 1  # ASK_QUESTION_STATE

    async def ask_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /ask."""
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите вопрос.\nПример: `/ask Что происходит с рублём?`", parse_mode="Markdown"
            )
            return

        question = " ".join(context.args)
        await self._answer_question(update.message, question)

    async def handle_question(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка вопроса из диалога."""
        question = update.message.text.strip()
        await self._answer_question(update.message, question)
        return ConversationHandler.END

    async def _answer_question(self, message, question: str):
        """Ответ на вопрос."""
        status_msg = await message.reply_text("🤔 Анализирую новости, подождите секунду...")

        try:
            articles = await self.article_repo.get_unprocessed(limit=30)

            if not articles:
                await status_msg.edit_text("📭 Нет свежих новостей для анализа")
                return

            prompt = self._build_qa_prompt(question, articles)
            answer = await self.llm.raw_request(prompt)

            response = f"❓ *Вопрос:* {question}\n\n📚 *Ответ:*\n\n{answer}"

            await status_msg.delete()
            await message.reply_text(response, parse_mode="Markdown")

        except Exception as e:
            logger.error(f"Ошибка ответа на вопрос: {e}")
            await status_msg.edit_text("❌ Не удалось проанализировать новости")

    def _build_qa_prompt(self, question: str, articles: List[Dict]) -> str:
        """Построение промпта для LLM."""
        posts_text = []
        for a in articles[:20]:
            title = a.get("raw_title", "")[:100]
            text = a.get("raw_text", "")[:300]
            posts_text.append(f"• {title}\n   {text}...\n")

        return f"""
Ты — аналитик новостного агрегатора. Ответь на вопрос пользователя, используя только новости ниже.

ВОПРОС: {question}

СВЕЖИЕ НОВОСТИ:
{chr(10).join(posts_text)}

Ответь кратко и по делу (3-5 предложений). Основано ТОЛЬКО на предоставленных новостях.
"""
