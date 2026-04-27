"""
Telegram бот для News Graph Project.
"""

import logging
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
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
from src.processing.llm.deepseek import DeepSeekAnalyzer
from src.processing.summarization.formatter import SummaryFormatter
from src.utils.datetime_utils import format_for_display, now_msk
from src.utils.telegram_helpers import safe_markdown_text

warnings.filterwarnings("ignore", category=PTBUserWarning)

logger = logging.getLogger(__name__)

# Состояния для ConversationHandler
ASK_QUESTION_STATE = 1
SEARCH_STATE = 2
CUSTOM_BRIEF_STATE = 3


class NewsTelegramBot:
    """Новостной телеграм бот."""

    def __init__(self, token: str, proxy_url: Optional[str] = None):
        self.token = token
        self.proxy_url = proxy_url
        self.subscribers: Dict[int, Dict] = {}

        self.article_repo = ArticleRepository()
        self.summary_repo = SummaryRepository()
        self.llm = DeepSeekAnalyzer()
        self.formatter = SummaryFormatter()

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
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("brief", self.brief_command))
        self.application.add_handler(CommandHandler("daily", self.daily_command))
        self.application.add_handler(CommandHandler("news", self.news_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
        self.application.add_handler(CommandHandler("ask", self.ask_command))
        self.application.add_handler(CommandHandler("subscribe", self.subscribe_command))
        self.application.add_handler(CommandHandler("unsubscribe", self.unsubscribe_command))
        self.application.add_handler(CommandHandler("health", self.health_command))

        # Conversation для вопросов
        ask_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.ask_prompt, pattern="menu_ask")],
            states={
                ASK_QUESTION_STATE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_question)
                ],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
        )
        self.application.add_handler(ask_conv)

        # Conversation для поиска
        search_conv = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(self.search_prompt, pattern="menu_search"),
                CallbackQueryHandler(self.search_popular, pattern=r"^search_popular:"),
            ],
            states={
                SEARCH_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_search)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
        )
        self.application.add_handler(search_conv)

        # Conversation для произвольного диапазона сводок
        brief_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.brief_custom_prompt, pattern="brief_custom")],
            states={
                CUSTOM_BRIEF_STATE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_custom_brief)
                ],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
        )
        self.application.add_handler(brief_conv)

        # Кнопки меню
        self.application.add_handler(CallbackQueryHandler(self.main_menu, pattern="main_menu"))
        self.application.add_handler(
            CallbackQueryHandler(self.show_summaries_menu, pattern="menu_summaries")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.show_search_menu, pattern="menu_search")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.show_stats_menu, pattern="menu_stats")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.show_subscribe_menu, pattern="menu_subscribe")
        )
        self.application.add_handler(CallbackQueryHandler(self.show_help_menu, pattern="menu_help"))
        self.application.add_handler(CallbackQueryHandler(self.ask_prompt, pattern="menu_ask"))

        # Сводки
        self.application.add_handler(CallbackQueryHandler(self.brief_6h, pattern="brief_6h"))
        self.application.add_handler(
            CallbackQueryHandler(self.daily_command, pattern="summary_yesterday")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.brief_custom_prompt, pattern="brief_custom")
        )

        # Поиск (популярные запросы)
        self.application.add_handler(
            CallbackQueryHandler(self.search_popular, pattern=r"^search_popular:")
        )

        # Статистика
        self.application.add_handler(
            CallbackQueryHandler(self.stats_overall, pattern="stats_overall")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.stats_hourly, pattern="stats_hourly")
        )

        # Подписка
        self.application.add_handler(
            CallbackQueryHandler(self.subscribe_from_menu, pattern="subscribe_daily")
        )
        self.application.add_handler(
            CallbackQueryHandler(self.unsubscribe_from_menu, pattern="subscribe_unsubscribe")
        )

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена текущего действия."""
        await update.message.reply_text("❌ Действие отменено.")
        return ConversationHandler.END

    # ==================== ОСНОВНЫЕ КОМАНДЫ ====================

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Главное меню."""
        user = update.effective_user

        welcome_text = f"""
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

        keyboard = [
            [InlineKeyboardButton("📰 Сводки", callback_data="menu_summaries")],
            [InlineKeyboardButton("🔍 Поиск", callback_data="menu_search")],
            [InlineKeyboardButton("🤖 Задать вопрос", callback_data="menu_ask")],
            [InlineKeyboardButton("📊 Статистика", callback_data="menu_stats")],
            [InlineKeyboardButton("🔔 Подписка", callback_data="menu_subscribe")],
            [InlineKeyboardButton("⚙️ Помощь", callback_data="menu_help")],
        ]

        await update.message.reply_text(welcome_text, reply_markup=InlineKeyboardMarkup(keyboard))

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

    # ==================== МЕНЮ ====================

    async def main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать главное меню."""
        query = update.callback_query
        await query.edit_message_text(
            "🏠 *Главное меню*\n\nВыберите действие:",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("📰 Сводки", callback_data="menu_summaries")],
                    [InlineKeyboardButton("🔍 Поиск", callback_data="menu_search")],
                    [InlineKeyboardButton("🤖 Задать вопрос", callback_data="menu_ask")],
                    [InlineKeyboardButton("📊 Статистика", callback_data="menu_stats")],
                    [InlineKeyboardButton("🔔 Подписка", callback_data="menu_subscribe")],
                    [InlineKeyboardButton("⚙️ Помощь", callback_data="menu_help")],
                ]
            ),
            parse_mode="Markdown",
        )
        await query.answer()

    async def show_summaries_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню сводок."""
        query = update.callback_query
        text = "📰 *Сводки новостей*\n\nВыберите период:"
        keyboard = [
            [InlineKeyboardButton("🕐 За 6 часов", callback_data="brief_6h")],
            [InlineKeyboardButton("📅 За вчера", callback_data="summary_yesterday")],
            [InlineKeyboardButton("✏️ Свой диапазон (в часах)", callback_data="brief_custom")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
        ]
        await query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
        await query.answer()

    async def show_search_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню поиска."""
        query = update.callback_query
        text = "🔍 *Поиск новостей*\n\nВыберите популярный запрос или введите свой:"
        keyboard = [
            [InlineKeyboardButton("💰 Нефть", callback_data="search_popular:нефть")],
            [InlineKeyboardButton("💵 Рубль", callback_data="search_popular:рубль")],
            [InlineKeyboardButton("🏛 Путин", callback_data="search_popular:Путин")],
            [InlineKeyboardButton("🤖 ИИ", callback_data="search_popular:ИИ")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
        ]
        await query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
        await query.answer()

        # Возвращаем SEARCH_STATE для обработки текста
        return SEARCH_STATE

    async def show_stats_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню статистики."""
        query = update.callback_query
        text = "📊 *Статистика*\n\nВыберите тип:"
        keyboard = [
            [InlineKeyboardButton("📈 Общая статистика", callback_data="stats_overall")],
            [InlineKeyboardButton("🕐 Почасовая активность", callback_data="stats_hourly")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
        ]
        await query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
        await query.answer()

    async def show_subscribe_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Меню подписки."""
        query = update.callback_query
        chat_id = query.message.chat.id
        is_subscribed = chat_id in self.subscribers

        if is_subscribed:
            text = "🔔 *Вы подписаны на рассылку*\n\n✅ Ежедневный дайджест в 20:00 МСК."
            keyboard = [
                [InlineKeyboardButton("❌ Отписаться", callback_data="subscribe_unsubscribe")]
            ]
        else:
            text = "🔕 *Вы не подписаны*\n\nПодпишитесь на ежедневный дайджест в 20:00 МСК."
            keyboard = [[InlineKeyboardButton("✅ Подписаться", callback_data="subscribe_daily")]]
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="main_menu")])

        await query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
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
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="main_menu")]]
        await query.edit_message_text(
            help_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )
        await query.answer()

    # ==================== СВОДКИ ====================

    async def brief_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сводка за N часов."""
        try:
            hours = 6
            if context and context.args:
                try:
                    hours = int(context.args[0])
                    hours = max(1, min(168, hours))
                except ValueError:
                    hours = 6

            from zoneinfo import ZoneInfo

            msk_tz = ZoneInfo("Europe/Moscow")

            # Текущее время в МСК
            now_msk_time = datetime.now(msk_tz)
            period_start = now_msk_time - timedelta(hours=hours)

            summaries = await self.summary_repo.get_for_period(period_start, now_msk_time, "hour")

            if not summaries:
                await update.message.reply_text(f"📭 Нет суммаризаций за последние {hours} часов.")
                return

            response = f"📊 *Сводка за последние {hours} часов*\n\n"

            for s in summaries[-12:]:
                # Преобразуем period_start в МСК, если он из БД naive
                period_start_val = s["period_start"]
                if period_start_val.tzinfo is None:
                    period_start_val = period_start_val.replace(tzinfo=msk_tz)
                else:
                    # Если с часовым поясом, конвертируем в МСК
                    period_start_val = period_start_val.astimezone(msk_tz)

                time_str = period_start_val.strftime("%H:%M")
                content = s["content"]
                if isinstance(content, dict):
                    summary = content.get("summary", "Нет данных")
                    response += f"🕐 *{time_str}*: {summary}\n\n"

            await update.message.reply_text(response, parse_mode="Markdown")

        except Exception as e:
            logger.error(f"Ошибка brief_command: {e}")
            await update.message.reply_text("❌ Ошибка получения сводки")

    async def brief_6h(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сводка за 6 часов."""
        query = update.callback_query
        await query.edit_message_text("📊 Генерирую сводку за 6 часов...")

        # Создаём mock update
        class MockUpdate:
            def __init__(self, query):
                self.effective_chat = query.message.chat
                self.message = type("obj", (object,), {"reply_text": query.edit_message_text})
                self.args = ["6"]

        await self.brief_command(MockUpdate(query), None)
        await query.answer()

    async def brief_custom_prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Запрос произвольного диапазона."""
        query = update.callback_query
        await query.edit_message_text(
            "✏️ *Свой диапазон*\n\n" "Введите количество часов (от 1 до 168):\n\n" "Пример: `24`"
        )
        await query.answer()
        return CUSTOM_BRIEF_STATE

    async def handle_custom_brief(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка произвольного диапазона."""
        try:
            hours = int(update.message.text.strip())
            hours = max(1, min(168, hours))

            class MockUpdate:
                def __init__(self, msg, hours):
                    self.effective_chat = msg.chat
                    self.message = type("obj", (object,), {"reply_text": msg.reply_text})
                    self.args = [str(hours)]

            await self.brief_command(MockUpdate(update.message, hours), None)

        except ValueError:
            await update.message.reply_text("❌ Пожалуйста, введите число (1-168)")

        return ConversationHandler.END

    async def daily_command(self, update, context=None):
        """Дневная сводка за вчера."""
        try:
            yesterday = (now_msk() - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            today = yesterday + timedelta(days=1)

            summaries = await self.summary_repo.get_for_period(yesterday, today, "day")

            if not summaries:
                msg = "📭 Дневная суммаризация за вчера ещё не готова.\nОбычно она появляется к 10 утра."
                # Всегда используем reply_text, он работает и для сообщений, и для callback_query
                if hasattr(update, "message") and update.message:
                    await update.message.reply_text(msg)
                elif hasattr(update, "edit_message_text"):
                    await update.edit_message_text(msg)
                else:
                    # Если это callback_query
                    await update.callback_query.edit_message_text(msg)
                return

            s = summaries[0]
            content = s["content"]

            if isinstance(content, dict):
                response = f"📅 *Сводка за {yesterday.strftime('%d.%m.%Y')}*\n\n"
                response += f"📌 *Главные темы:*\n"
                for topic in content.get("topics", [])[:5]:
                    response += f"• {topic}\n"
                response += f"\n📝 *Суть дня:*\n{content.get('summary', 'Нет данных')}\n"
                if content.get("trend"):
                    response += f"\n📈 *Тренд:* {content.get('trend')}\n"

                if hasattr(update, "message") and update.message:
                    await update.message.reply_text(response, parse_mode="Markdown")
                elif hasattr(update, "edit_message_text"):
                    await update.edit_message_text(response, parse_mode="Markdown")
                else:
                    await update.callback_query.edit_message_text(response, parse_mode="Markdown")
            else:
                if hasattr(update, "message") and update.message:
                    await update.message.reply_text("❌ Не удалось разобрать суммаризацию")
                else:
                    await update.callback_query.edit_message_text(
                        "❌ Не удалось разобрать суммаризацию"
                    )

        except Exception as e:
            logger.error(f"Ошибка daily_command: {e}")
            if hasattr(update, "message") and update.message:
                await update.message.reply_text("❌ Ошибка получения дневной сводки")
            elif hasattr(update, "edit_message_text"):
                await update.edit_message_text("❌ Ошибка получения дневной сводки")
            else:
                await update.callback_query.edit_message_text("❌ Ошибка получения дневной сводки")

    # ==================== ПОИСК ====================

    async def search_prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Запрос поискового запроса."""
        query = update.callback_query
        await query.edit_message_text(
            "🔍 *Поиск новостей*\n\n" "Введите поисковый запрос:\n\n" "Пример: `криптовалюта`"
        )
        await query.answer()
        return SEARCH_STATE

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
                # Экранируем HTML-символы в заголовке
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

    # ==================== ВОПРОСЫ ====================

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
        return ASK_QUESTION_STATE

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

    # ==================== СТАТИСТИКА ====================

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /stats."""
        try:
            stats = await self.article_repo.get_stats()
            sources_stats = await self.article_repo.get_sources_stats()

            total = stats.get("total", 0)
            raw = stats.get("raw", 0)
            processed = stats.get("processed", 0)

            stats_text = f"📊 *Статистика*\n\n"
            stats_text += f"• Всего новостей: *{total}*\n"
            stats_text += f"• Необработано: {raw}\n"
            stats_text += f"• Обработано: {processed}\n\n"

            stats_text += "📰 *По источникам:*\n"
            for source_name, count in sources_stats[:5]:
                stats_text += f"• {source_name}: *{count}*\n"

            stats_text += f"\n👥 *Подписчиков:* {len(self.subscribers)}"

            await update.message.reply_text(stats_text, parse_mode="Markdown")

        except Exception as e:
            logger.error(f"Ошибка stats_command: {e}")
            await update.message.reply_text("❌ Ошибка получения статистики")

    async def stats_overall(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Общая статистика из меню."""
        query = update.callback_query
        await query.edit_message_text("📊 Собираю статистику...")

        class MockUpdate:
            def __init__(self, query):
                self.effective_chat = query.message.chat
                self.message = type("obj", (object,), {"reply_text": query.edit_message_text})
                self.args = []

        await self.stats_command(MockUpdate(query), None)
        await query.answer()

    async def stats_hourly(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Почасовая статистика."""
        query = update.callback_query
        await query.edit_message_text("📊 Собираю почасовую статистику...")

        try:
            stats = await self.article_repo.get_daily_stats(days=1)

            if not stats:
                await query.edit_message_text("❌ Нет данных для статистики")
                return

            # Находим максимальное значение
            max_count = max(count for _, count in stats[:24]) if stats else 0

            # Максимальная длина бара
            MAX_BAR_LENGTH = 10

            response = "🕐 *Активность по часам (последние 24 ч)*\n\n"
            response += f"📈 Максимум: {max_count} публикаций\n\n"

            for hour, count in stats[:24]:
                if max_count > 0:
                    # Пропорциональная длина бара
                    bar_length = int((count / max_count) * MAX_BAR_LENGTH)
                    bar = "█" * bar_length if bar_length > 0 else "░"
                    # Добавляем пустые символы для выравнивания
                    bar = bar.ljust(MAX_BAR_LENGTH, "░")
                else:
                    bar = "░" * MAX_BAR_LENGTH

                # Форматируем число с разделителями для тысяч
                formatted_count = f"{count:,}".replace(",", " ")
                response += f"• {hour:02d}:00 {bar} {formatted_count}\n"

            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="menu_stats")]]
            await query.edit_message_text(
                response, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
            )

        except Exception as e:
            logger.error(f"Ошибка stats_hourly: {e}")
            await query.edit_message_text("❌ Ошибка получения статистики")

        await query.answer()

    # ==================== ПОДПИСКА ====================

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
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="menu_subscribe")]]
                ),
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
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Назад", callback_data="menu_subscribe")]]
                ),
            )
        else:
            await query.answer("❌ Вы не были подписаны", show_alert=True)
        await query.answer()

    # ==================== ЗДОРОВЬЕ ====================

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

    # ==================== ЗАПУСК ====================

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
