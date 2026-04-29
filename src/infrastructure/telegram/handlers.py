"""
Обработчики команд для Telegram бота.
"""

import logging

from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from src.database.repositories.article_repository import ArticleRepository
from src.database.repositories.summary_repository import SummaryRepository
from src.processing.llm.deepseek import DeepSeekAnalyzer
from src.processing.summarization.formatter import SummaryFormatter

from .menus import (
    get_back_button,
    get_main_menu,
    get_search_menu,
    get_stats_menu,
    get_subscribe_menu,
    get_summaries_menu,
)
from .statistics import format_hourly_stats, get_hourly_stats

logger = logging.getLogger(__name__)

# Состояния для ConversationHandler
ASK_QUESTION_STATE = 1
SEARCH_STATE = 2
CUSTOM_BRIEF_STATE = 3


class Handlers:
    """Класс-контейнер для всех обработчиков."""

    def __init__(
        self,
        article_repo: ArticleRepository,
        summary_repo: SummaryRepository,
        llm: DeepSeekAnalyzer,
        formatter: SummaryFormatter,
        subscribers: dict,
    ):
        self.article_repo = article_repo
        self.summary_repo = summary_repo
        self.llm = llm
        self.formatter = formatter
        self.subscribers = subscribers

    # ==================== ОСНОВНЫЕ КОМАНДЫ ====================

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Главное меню."""
        user = update.effective_user
        welcome_text = """
👋 Привет, {user.first_name}!

Я — новостной бот News Graph Project.

📊 Что умею:
• 📰 Сводки новостей за период
• 🔍 Поиск по новостям
• 🤖 Отвечать на вопросы
• 📈 Статистика и тренды
• 🔔 Ежедневные дайджесты

Выберите действие в меню 👇
        """
        await update.message.reply_text(welcome_text, reply_markup=get_main_menu())

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Справка."""
        help_text = """
📚 *Список команд*

*Основные:*
/start - Главное меню
/help - Помощь
/brief [часы] - Сводка за N часов
/daily - Сводка за вчера
/ask [вопрос] - Ответ на вопрос

*Новости:*
/news [запрос] - Поиск новостей
/stats - Статистика

*Подписка:*
/subscribe - Подписаться на дайджест
/unsubscribe - Отписаться

*Технические:*
/health - Техническая информация
        """
        await update.message.reply_text(help_text, parse_mode="Markdown")

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена текущего действия."""
        await update.message.reply_text("❌ Действие отменено.")
        return ConversationHandler.END

    # ==================== МЕНЮ ====================

    async def main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать главное меню."""
        query = update.callback_query
        await query.edit_message_text(
            "🏠 *Главное меню*\n\nВыберите действие:",
            reply_markup=get_main_menu(),
            parse_mode="Markdown",
        )
        await query.answer()

    async def show_summaries_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню сводок."""
        query = update.callback_query
        await query.edit_message_text(
            "📰 *Сводки новостей*\n\nВыберите период:",
            reply_markup=get_summaries_menu(),
            parse_mode="Markdown",
        )
        await query.answer()

    async def show_search_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню поиска."""
        query = update.callback_query
        await query.edit_message_text(
            "🔍 *Поиск новостей*\n\nВыберите популярный запрос или введите свой:",
            reply_markup=get_search_menu(),
            parse_mode="Markdown",
        )
        await query.answer()
        return SEARCH_STATE

    async def show_stats_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню статистики."""
        query = update.callback_query
        await query.edit_message_text(
            "📊 *Статистика*\n\nВыберите тип:",
            reply_markup=get_stats_menu(),
            parse_mode="Markdown",
        )
        await query.answer()

    async def show_help_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню помощи."""
        query = update.callback_query
        help_text = """
📚 *Помощь*

*Основные команды:*
/start - Главное меню
/help - Помощь
/brief [часы] - Сводка за N часов
/daily - Сводка за вчера
/ask [вопрос] - Ответ на вопрос

*Новости:*
/news [запрос] - Поиск новостей
/stats - Статистика

*Подписка:*
/subscribe - Подписаться
/unsubscribe - Отписаться

*Технические:*
/health - Техническая информация
        """
        await query.edit_message_text(
            help_text,
            reply_markup=get_back_button(),
            parse_mode="Markdown",
        )
        await query.answer()

    # ==================== СТАТИСТИКА ====================

    async def _build_stats_text(self) -> str:
        """Формирует текст статистики."""
        stats = await self.article_repo.get_stats()
        sources_stats = await self.article_repo.get_sources_stats()

        total = stats.get("total", 0)
        raw = stats.get("raw", 0)
        processed = stats.get("processed", 0)

        stats_text = "📊 *Статистика*\n\n"
        stats_text += f"• Всего новостей: *{total}*\n"
        stats_text += f"• Необработано: {raw}\n"
        stats_text += f"• Обработано: {processed}\n\n"

        stats_text += "📰 *По источникам:*\n"
        for source_name, count in sources_stats[:5]:
            stats_text += f"• {source_name}: *{count}*\n"

        stats_text += f"\n👥 *Подписчиков:* {len(self.subscribers)}"
        return stats_text

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /stats."""
        try:
            stats_text = await self._build_stats_text()
            await update.message.reply_text(stats_text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Ошибка stats_command: {e}")
            await update.message.reply_text("❌ Ошибка получения статистики")

    async def stats_overall(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Общая статистика из меню."""
        query = update.callback_query
        await query.edit_message_text("📊 Собираю статистику...")

        try:
            stats_text = await self._build_stats_text()
            await query.edit_message_text(
                stats_text,
                reply_markup=get_back_button("menu_stats"),
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error(f"Ошибка stats_overall: {e}")
            await query.edit_message_text("❌ Ошибка получения статистики")

        await query.answer()

    async def stats_hourly(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Почасовая статистика (последние 24 часа, плавающее окно)."""
        query = update.callback_query
        await query.edit_message_text("📊 Собираю почасовую статистику...")

        try:
            logger.info("stats_hourly: вызываю get_hourly_stats")
            hourly_stats = await get_hourly_stats(self.article_repo)
            logger.info(f"stats_hourly: получил {len(hourly_stats)} записей")

            if not hourly_stats:
                logger.warning("stats_hourly: hourly_stats is empty")
                await query.edit_message_text(
                    "📊 За последние 24 часа нет данных о публикациях.",
                    reply_markup=get_back_button("menu_stats"),
                )
                await query.answer()
                return

            logger.info("stats_hourly: формирую ответ")
            response = format_hourly_stats(hourly_stats)
            logger.info(f"stats_hourly: ответ сформирован, длина={len(response)}")

            await query.edit_message_text(
                response,
                reply_markup=get_back_button("menu_stats"),
                parse_mode="Markdown",
            )

        except Exception as e:
            logger.error(f"Ошибка stats_hourly: {e}", exc_info=True)
            await query.edit_message_text("❌ Ошибка получения статистики")

        await query.answer()

    # ==================== ПОДПИСКА ====================
    async def show_subscribe_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню подписки."""
        query = update.callback_query
        chat_id = query.message.chat.id
        is_subscribed = chat_id in self.subscribers

        if is_subscribed:
            text = "🔔 *Вы подписаны на рассылку*\n\n✅ Ежедневный дайджест в 20:00 МСК."
        else:
            text = "🔕 *Вы не подписаны*\n\nПодпишитесь на ежедневный дайджест в 20:00 МСК."

        await query.edit_message_text(
            text,
            reply_markup=get_subscribe_menu(is_subscribed),
            parse_mode="Markdown",
        )
        await query.answer()

    async def subscribe_from_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подписка из меню."""
        query = update.callback_query
        chat_id = query.message.chat.id

        if chat_id in self.subscribers:
            await query.answer("✅ Вы уже подписаны!", show_alert=True)
        else:
            self.subscribers[chat_id] = {
                "time": "20:00",
                "timezone": "Europe/Moscow",
                "last_sent": None,
            }
            await query.edit_message_text(
                "✅ Вы подписались на ежедневную рассылку!\n\n"
                "Каждый день в 20:00 (МСК) вы будете получать дайджест.",
                reply_markup=get_back_button("menu_subscribe"),
            )
        await query.answer()

    async def unsubscribe_from_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отписка из меню."""
        query = update.callback_query
        chat_id = query.message.chat.id

        if chat_id in self.subscribers:
            del self.subscribers[chat_id]
            await query.edit_message_text(
                "❌ Вы отписались от рассылки.\n\n"
                "Чтобы подписаться снова, нажмите «Подписаться» в меню.",
                reply_markup=get_back_button("menu_subscribe"),
            )
        else:
            await query.answer("❌ Вы не были подписаны", show_alert=True)
        await query.answer()

    async def subscribe_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подписка через команду."""
        chat_id = update.effective_chat.id

        if chat_id in self.subscribers:
            await update.message.reply_text("✅ Вы уже подписаны на рассылку!")
        else:
            self.subscribers[chat_id] = {
                "time": "20:00",
                "timezone": "Europe/Moscow",
                "last_sent": None,
            }
            await update.message.reply_text(
                "✅ Вы подписались на ежедневную рассылку в 20:00 (МСК)!\n"
                "Каждый день в это время вы будете получать дайджест новостей."
            )

    async def unsubscribe_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отписка через команду."""
        chat_id = update.effective_chat.id

        if chat_id in self.subscribers:
            del self.subscribers[chat_id]
            await update.message.reply_text("✅ Вы отписались от рассылки новостей.")
        else:
            await update.message.reply_text("❌ Вы не были подписаны на рассылку.")
