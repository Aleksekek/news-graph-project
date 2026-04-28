"""
Логика поиска для Telegram бота.
"""

import logging

from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from src.database.repositories.article_repository import ArticleRepository
from src.utils.datetime_utils import format_for_display

logger = logging.getLogger(__name__)


class SearchHandlers:
    """Обработчики поиска."""

    def __init__(self, article_repo: ArticleRepository):
        self.article_repo = article_repo

    async def search_prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Запрос поискового запроса."""
        query = update.callback_query
        await query.edit_message_text(
            "🔍 *Поиск новостей*\n\n" "Введите поисковый запрос:\n\n" "Пример: `криптовалюта`"
        )
        await query.answer()
        return 2  # SEARCH_STATE

    async def search_popular(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка популярного запроса."""
        query = update.callback_query
        keyword = query.data.split(":")[1]
        await query.edit_message_text(f"🔍 Ищу новости по запросу '{keyword}'...")

        await self._do_search(query.message, keyword)
        await query.answer()
        return ConversationHandler.END

    async def handle_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка введённого поискового запроса."""
        keyword = update.message.text.strip()
        await update.message.reply_text(f"🔍 Ищу новости по запросу '{keyword}'...")

        await self._do_search(update.message, keyword)
        return ConversationHandler.END

    async def news_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /news - поиск новостей."""
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите поисковый запрос.\nПример: `/news криптовалюта`", parse_mode="Markdown"
            )
            return

        query = " ".join(context.args)
        await update.message.reply_text(
            f"🔍 Ищу новости по запросу: *{query}*...", parse_mode="Markdown"
        )

        await self._do_search(update.message, query)

    async def _do_search(self, message, query: str):
        """Выполнение поиска."""
        try:
            articles = await self.article_repo.search(query, limit=10, with_urls=True)

            if not articles:
                await message.reply_text(f"📭 Новостей по запросу '{query}' не найдено.")
                return

            result_text = f"🔍 <b>Результаты поиска по запросу '{query}':</b>\n\n"

            for i, article in enumerate(articles[:10], 1):
                title = article.get("raw_title", "Без заголовка")[:70]
                title = title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

                published = article.get("published_at")
                time_str = (
                    format_for_display(published, include_time=True)
                    if published
                    else "Дата неизвестна"
                )
                url = article.get("url", "")

                result_text += f"{i}. <b>{title}</b>...\n"
                result_text += f"   🕐 {time_str}\n"
                if url:
                    result_text += f"   🔗 <a href='{url}'>{url}</a>\n\n"

                if len(result_text) > 3500:
                    result_text += f"...\n<b>Показаны первые {i} результатов</b>"
                    break

            await message.reply_text(result_text, parse_mode="HTML")

        except Exception as e:
            logger.error(f"Ошибка поиска: {e}")
            await message.reply_text("❌ Ошибка при поиске новостей")
