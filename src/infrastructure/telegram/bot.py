"""
Telegram бот для News Graph Project.
"""

import logging
import warnings

from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest
from telegram.warnings import PTBUserWarning

from src.config.settings import settings
from src.database.repositories.article_repository import ArticleRepository
from src.database.repositories.summary_repository import SummaryRepository
from src.infrastructure.telegram.briefs import BriefHandlers
from src.infrastructure.telegram.handlers import Handlers
from src.infrastructure.telegram.qa import QAHandlers
from src.infrastructure.telegram.search import SearchHandlers
from src.processing.llm.deepseek import DeepSeekAnalyzer
from src.processing.summarization.formatter import SummaryFormatter
from src.utils.datetime_utils import format_for_display, now_msk

warnings.filterwarnings("ignore", category=PTBUserWarning)

logger = logging.getLogger(__name__)


class NewsTelegramBot:
    """Новостной телеграм бот."""

    def __init__(self, token: str, proxy_url: str | None = None):
        self.token = token
        self.proxy_url = proxy_url
        self.subscribers: dict[int, dict] = {}

        # Репозитории
        self.article_repo = ArticleRepository()
        self.summary_repo = SummaryRepository()

        # Сервисы
        self.llm = DeepSeekAnalyzer()
        self.formatter = SummaryFormatter()

        # Обработчики
        self.handlers = Handlers(
            self.article_repo, self.summary_repo, self.llm, self.formatter, self.subscribers
        )
        self.search_handlers = SearchHandlers(self.article_repo)
        self.qa_handlers = QAHandlers(self.article_repo, self.llm)
        self.brief_handlers = BriefHandlers(self.summary_repo)

        self.application = self._build_application()
        self._setup_handlers()

    def _build_application(self) -> Application:
        """Создание приложения."""
        if self.proxy_url:
            proxy = self._normalize_proxy_url(self.proxy_url)
            request = HTTPXRequest(
                proxy=proxy,
                connect_timeout=30.0,
                read_timeout=30.0,
                write_timeout=30.0,
                httpx_kwargs={"verify": False},
            )
            return Application.builder().token(self.token).request(request).build()
        return Application.builder().token(self.token).build()

    def _normalize_proxy_url(self, proxy_url: str) -> str:
        """Нормализация URL прокси."""
        if proxy_url.startswith(("http://", "https://", "socks5://")):
            return proxy_url
        parts = proxy_url.split(":")
        if len(parts) == 4:
            host, port, user, password = parts
            return f"socks5://{user}:{password}@{host}:{port}"
        return proxy_url

    def _setup_handlers(self):
        """Настройка всех обработчиков."""
        # Команды
        self.application.add_handler(CommandHandler("start", self.handlers.start))
        self.application.add_handler(CommandHandler("help", self.handlers.help_command))
        self.application.add_handler(CommandHandler("brief", self.brief_handlers.brief_command))
        self.application.add_handler(CommandHandler("daily", self.brief_handlers.daily_command))
        self.application.add_handler(CommandHandler("news", self.search_handlers.news_command))
        self.application.add_handler(CommandHandler("stats", self.handlers.stats_command))
        self.application.add_handler(CommandHandler("ask", self.qa_handlers.ask_command))
        self.application.add_handler(CommandHandler("subscribe", self.handlers.subscribe_command))
        self.application.add_handler(
            CommandHandler("unsubscribe", self.handlers.unsubscribe_command)
        )
        self.application.add_handler(CommandHandler("health", self.health_command))

        # Conversation для вопросов
        ask_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.qa_handlers.ask_prompt, pattern="menu_ask")],
            states={
                1: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND, self.qa_handlers.handle_question
                    )
                ],
            },
            fallbacks=[CommandHandler("cancel", self.handlers.cancel)],
        )
        self.application.add_handler(ask_conv)

        # Conversation для поиска
        search_conv = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(self.search_handlers.search_prompt, pattern="menu_search"),
                CallbackQueryHandler(
                    self.search_handlers.search_popular, pattern=r"^search_popular:"
                ),
            ],
            states={
                2: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND, self.search_handlers.handle_search
                    )
                ],
            },
            fallbacks=[CommandHandler("cancel", self.handlers.cancel)],
        )
        self.application.add_handler(search_conv)

        # Conversation для произвольного диапазона сводок
        brief_conv = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(
                    self.brief_handlers.brief_custom_prompt, pattern="brief_custom"
                )
            ],
            states={
                3: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND, self.brief_handlers.handle_custom_brief
                    )
                ],
            },
            fallbacks=[CommandHandler("cancel", self.handlers.cancel)],
        )
        self.application.add_handler(brief_conv)

        # Кнопки меню
        self.application.add_handler(
            CallbackQueryHandler(self.handlers.main_menu, pattern="main_menu")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.handlers.show_summaries_menu, pattern="menu_summaries")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.handlers.show_search_menu, pattern="menu_search")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.handlers.show_stats_menu, pattern="menu_stats")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.handlers.show_subscribe_menu, pattern="menu_subscribe")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.handlers.show_help_menu, pattern="menu_help")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.qa_handlers.ask_prompt, pattern="menu_ask")
        )

        # Сводки
        self.application.add_handler(
            CallbackQueryHandler(self.brief_handlers.brief_6h, pattern="brief_6h")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.brief_handlers.daily_command, pattern="summary_yesterday")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.brief_handlers.brief_custom_prompt, pattern="brief_custom")
        )

        # Поиск
        self.application.add_handler(
            CallbackQueryHandler(self.search_handlers.search_popular, pattern=r"^search_popular:")
        )

        # Статистика
        self.application.add_handler(
            CallbackQueryHandler(self.handlers.stats_overall, pattern="stats_overall")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.handlers.stats_hourly, pattern="stats_hourly")
        )

        # Подписка
        self.application.add_handler(
            CallbackQueryHandler(self.handlers.subscribe_from_menu, pattern="subscribe_daily")
        )
        self.application.add_handler(
            CallbackQueryHandler(
                self.handlers.unsubscribe_from_menu, pattern="subscribe_unsubscribe"
            )
        )

    async def health_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Техническая информация."""
        import platform

        health_text = f"""
⚙️ *Техническая информация*

🤖 *Бот:*
• Статус: ✅ Активен
• Прокси: {'✅' if self.proxy_url else '❌'}
• Подписчики: {len(self.subscribers)}

💻 *Система:*
• Python: {platform.python_version()}
• Время сервера: {format_for_display(now_msk())}
        """
        await update.message.reply_text(health_text, parse_mode="Markdown")

    def run(self):
        """Запуск бота."""
        logger.info("🚀 Запуск Telegram бота...")
        self.application.run_polling(drop_pending_updates=True)


def main():
    """Точка входа."""
    token = settings.TELEGRAM_BOT_TOKEN
    proxy_url = settings.PROXY_URL

    if not token:
        logger.error("❌ TELEGRAM_BOT_TOKEN не установлен")
        return

    bot = NewsTelegramBot(token, proxy_url)
    bot.run()


if __name__ == "__main__":
    main()
