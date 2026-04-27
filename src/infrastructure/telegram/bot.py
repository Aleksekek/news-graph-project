"""
Telegram бот для News Graph Project
Поддерживает интерактивное меню, суммаризации, поиск и ответы на вопросы
"""

import asyncio
import logging
import os
import re
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

from src.config.settings import settings
from src.domain.processing.llm_analyzer import DeepSeekAnalyzer
from src.domain.processing.summary_generator import SummaryGenerator
from src.domain.storage.database import ArticleRepository
from src.domain.storage.summarization_repository import SummarizationRepository
from src.utils.telegram_helpers import safe_markdown_text

# ============================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


# ============================================================
# ОСНОВНОЙ КЛАСС БОТА
# ============================================================


class NewsTelegramBot:
    """Новостной телеграм бот с интерактивным меню"""

    # --------------------------------------------------------
    # ИНИЦИАЛИЗАЦИЯ
    # --------------------------------------------------------

    def __init__(self, token: str, proxy_url: Optional[str] = None):
        self.token = token
        self.proxy_url = proxy_url
        self.subscribers: Dict[int, Dict] = {}
        self.summary_generator = SummaryGenerator()
        self.llm_analyzer = DeepSeekAnalyzer()

        # Настройка прокси
        if proxy_url:
            logger.info("✅ Бот будет использовать прокси")
            os.environ["HTTP_PROXY"] = proxy_url
            os.environ["HTTPS_PROXY"] = proxy_url
            os.environ["ALL_PROXY"] = proxy_url

            request = HTTPXRequest(
                proxy=proxy_url,
                connect_timeout=30.0,
                read_timeout=30.0,
                write_timeout=30.0,
                httpx_kwargs={"verify": False},
            )
            self.application = (
                Application.builder().token(token).request(request).build()
            )
        else:
            logger.info("✅ Бот будет использовать прямое подключение")
            self.application = Application.builder().token(token).build()

    # --------------------------------------------------------
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # --------------------------------------------------------

    async def _safe_send_message(
        self, update: Update, text: str, parse_mode: str = "MarkdownV2"
    ):
        """Безопасная отправка сообщения с Markdown"""
        try:
            await update.message.reply_text(text, parse_mode=parse_mode)
        except Exception as e:
            logger.warning(f"Ошибка Markdown, отправка без разметки: {e}")
            clean_text = re.sub(r"[*_`]", "", text)
            await update.message.reply_text(clean_text)

    async def _show_main_menu(self, query):
        """Показывает главное меню"""
        text = "🏠 *Главное меню*\n\nВыберите действие:"
        keyboard = [
            [
                InlineKeyboardButton("📰 Сводки", callback_data="menu_summaries"),
                InlineKeyboardButton("🔍 Поиск", callback_data="menu_search"),
            ],
            [
                InlineKeyboardButton("🤖 Задать вопрос", callback_data="menu_ask"),
                InlineKeyboardButton("📊 Статистика", callback_data="menu_stats"),
            ],
            [
                InlineKeyboardButton("🔔 Подписка", callback_data="menu_subscribe"),
                InlineKeyboardButton("⚙️ Помощь", callback_data="menu_help"),
            ],
        ]
        await query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )

    async def _show_summaries_menu(self, query):
        """Меню выбора сводок"""
        text = "📰 *Сводки новостей*\n\nКакую сводку показать? 👇"
        keyboard = [
            [
                InlineKeyboardButton("🕐 За 6 часов", callback_data="brief_6h"),
                InlineKeyboardButton("🕑 За 12 часов", callback_data="brief_12h"),
            ],
            [
                InlineKeyboardButton("📅 За 24 часа", callback_data="brief_24h"),
                InlineKeyboardButton("📆 За вчера", callback_data="summary_yesterday"),
            ],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
        ]
        await query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )

    async def _quick_brief(self, query, hours: int):
        """Быстрая сводка за N часов"""
        await query.edit_message_text(f"📊 Генерирую сводку за {hours} часов...")

        class MockUpdate:
            def __init__(self, query):
                self.effective_chat = query.message.chat
                self.message = type(
                    "obj", (object,), {"reply_text": query.edit_message_text}
                )
                self.callback_query = query

        mock_update = MockUpdate(query)
        await self.brief_command(mock_update, None, hours=hours)

    async def _show_search_menu(self, query):
        """Меню поиска"""
        text = "🔍 *Поиск новостей*\n\nВыберите популярный запрос или введите свой:"
        keyboard = [
            [
                InlineKeyboardButton("💰 Нефть", callback_data="search_popular:нефть"),
                InlineKeyboardButton("💵 Рубль", callback_data="search_popular:рубль"),
            ],
            [
                InlineKeyboardButton("🏛 Путин", callback_data="search_popular:Путин"),
                InlineKeyboardButton("🤖 ИИ", callback_data="search_popular:ИИ"),
            ],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
        ]
        await query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )

    async def _quick_search(self, query, keyword: str, context):
        """Быстрый поиск по популярному запросу"""
        await query.edit_message_text(f"🔍 Ищу новости по запросу '{keyword}'...")

        class MockUpdate:
            def __init__(self, query):
                self.effective_chat = query.message.chat
                self.message = type(
                    "obj", (object,), {"reply_text": query.edit_message_text}
                )
                self.args = [keyword]

        mock_update = MockUpdate(query)
        await self.news_command(mock_update, context)

    async def _show_stats_menu(self, query):
        """Меню статистики"""
        text = "📊 *Статистика*\n\nВыберите тип статистики:"
        keyboard = [
            [
                InlineKeyboardButton(
                    "📈 Общая статистика", callback_data="stats_overall"
                )
            ],
            [
                InlineKeyboardButton(
                    "🕐 Почасовая активность", callback_data="stats_hourly"
                )
            ],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
        ]
        await query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )

    async def _hourly_stats(self, query, context):
        """Почасовая статистика активности"""
        await query.edit_message_text("📊 Собираю почасовую статистику...")

        try:
            repo = ArticleRepository()
            stats = await repo.get_hourly_stats(hours=24)

            if not stats:
                await query.edit_message_text("❌ Нет данных для статистики")
                return

            response = "🕐 *Активность по часам (последние 24 ч)*\n\n"
            for hour, count in stats[-12:]:
                bar = "█" * min(count // 5, 20)
                response += f"• {hour:02d}:00 {bar} {count}\n"

            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="menu_stats")]]
            await query.edit_message_text(
                response,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown",
            )

        except Exception as e:
            logger.error(f"Ошибка почасовой статистики: {e}")
            await query.edit_message_text("❌ Ошибка получения статистики")

    async def _show_subscribe_menu(self, query):
        """Меню подписки"""
        chat_id = query.message.chat.id
        is_subscribed = chat_id in self.subscribers

        if is_subscribed:
            text = "🔔 *Вы подписаны на рассылку*\n\n✅ Ежедневный дайджест приходит в 20:00."
            keyboard = [
                [
                    InlineKeyboardButton(
                        "❌ Отписаться", callback_data="subscribe_unsubscribe"
                    )
                ],
                [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
            ]
        else:
            text = "🔕 *Вы не подписаны*\n\nПодпишитесь, чтобы получать ежедневный дайджест в 20:00."
            keyboard = [
                [
                    InlineKeyboardButton(
                        "✅ Подписаться", callback_data="subscribe_daily"
                    )
                ],
                [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
            ]

        await query.edit_message_text(
            text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
        )

    async def _show_help_menu(self, query):
        """Меню помощи"""
        help_text = """
📚 *Помощь*

*Основные команды:*
• /start - Главное меню
• /brief [часы] - Сводка за N часов
• /daily - Сводка за вчера
• /ask [вопрос] - Ответ на вопрос

*Новости:*
• /summary [дни] - Сводка с ссылками
• /news [запрос] - Поиск новостей

*Статистика:*
• /stats - Общая статистика

*Подписка:*
• /subscribe - Подписаться на дайджест
• /unsubscribe - Отписаться

*Технические:*
• /health - Техническая информация

❓ По вопросам и предложениям: @aleksekek
        """
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="main_menu")]]
        await query.edit_message_text(
            help_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )

    async def _ask_question_prompt(self, query):
        """Запросить вопрос у пользователя"""
        text = (
            "🤖 *Задать вопрос*\n\n"
            "Напишите ваш вопрос о текущей новостной повестке.\n\n"
            "Примеры:\n"
            "• Что происходит с рублём?\n"
            "• Какие главные новости за сегодня?\n"
            "• Что пишут про нефть?"
        )
        await query.edit_message_text(text, parse_mode="Markdown")

    def _build_qa_prompt(
        self, question: str, articles: List[Dict], summaries: List[Dict]
    ) -> str:
        """Строит промпт для ответа на вопрос"""
        # Формируем текстовое представление постов
        posts_text = []
        for a in articles[:20]:
            title = a.get("title", "")[:100]
            text = a.get("text", "")[:300]
            source = a.get("source_name", "unknown")
            posts_text.append(f"[{source}] {title}\n   {text}...\n")

        # Формируем текстовое представление суммаризаций
        summaries_text = []
        for s in summaries[-6:]:
            period_start = s["period_start"]
            content = s["content"]
            if isinstance(content, dict):
                summary = content.get("summary", "")[:200]
                summaries_text.append(f"{period_start.strftime('%H:%M')}: {summary}")

        return f"""
Ты — аналитик новостного агрегатора. Ответь на вопрос пользователя, используя только новости за последние 24 часа.

ВОПРОС: {question}

КЛЮЧЕВЫЕ СОБЫТИЯ (из суммаризаций):
{chr(10).join(summaries_text)}

СВЕЖИЕ НОВОСТИ:
{chr(10).join(posts_text)}

Ответь кратко и по делу (3-5 предложений). Основано ТОЛЬКО на предоставленных новостях.
"""

    # --------------------------------------------------------
    # ОСНОВНЫЕ КОМАНДЫ
    # --------------------------------------------------------

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start — главное меню"""
        user = update.effective_user

        welcome_text = f"""
👋 Привет, {user.first_name}!

Я — новостной бот проекта News Graph Project.

📊 Умею:
• 📰 Показывать свежие новости и сводки
• 🤖 Отвечать на вопросы по новостям
• 📈 Анализировать тренды и темы
• 🔔 Присылать ежедневные дайджесты

Выберите действие в меню ниже 👇
        """

        keyboard = [
            [
                InlineKeyboardButton("📰 Сводки", callback_data="menu_summaries"),
                InlineKeyboardButton("🔍 Поиск", callback_data="menu_search"),
            ],
            [
                InlineKeyboardButton("🤖 Задать вопрос", callback_data="menu_ask"),
                InlineKeyboardButton("📊 Статистика", callback_data="menu_stats"),
            ],
            [
                InlineKeyboardButton("🔔 Подписка", callback_data="menu_subscribe"),
                InlineKeyboardButton("⚙️ Помощь", callback_data="menu_help"),
            ],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(welcome_text, reply_markup=reply_markup)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        help_text = """
📚 *Список команд*

*Основные:*
/start - Главное меню
/help - Помощь
/brief [часы] - Сводка за N часов
/daily - Сводка за вчера
/ask [вопрос] - Ответ на вопрос

*Новости:*
/summary [дни] - Сводка с ссылками
/news [запрос] - Поиск новостей
/stats - Статистика

*Подписка:*
/subscribe - Подписаться
/unsubscribe - Отписаться

*Технические:*
/health - Техническая информация
        """
        await update.message.reply_text(help_text, parse_mode="Markdown")

    async def subscribe_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """Подписка на рассылку"""
        chat_id = update.effective_chat.id

        if chat_id in self.subscribers:
            await update.message.reply_text("✅ Вы уже подписаны на рассылку!")
        else:
            self.subscribers[chat_id] = {
                "time": "20:00",
                "timezone": "Europe/Moscow",
                "format": "digest",
                "last_sent": None,
            }
            await update.message.reply_text(
                "✅ Вы подписались на ежедневную рассылку в 20:00 (МСК)!\n"
                "Каждый день в это время вы будете получать дайджест новостей."
            )

    async def unsubscribe_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        """Отписка от рассылки"""
        chat_id = update.effective_chat.id

        if chat_id in self.subscribers:
            del self.subscribers[chat_id]
            await update.message.reply_text("✅ Вы отписались от рассылки новостей.")
        else:
            await update.message.reply_text("❌ Вы не были подписаны на рассылку.")

    async def summary_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Генерация сводки с безопасным Markdown"""
        try:
            days = 1
            if context.args and len(context.args) > 0:
                try:
                    days = int(context.args[0])
                    if days < 1 or days > 30:
                        days = 1
                except ValueError:
                    days = 1

            await update.message.reply_text(
                f"📊 Генерирую сводку за {days} день(дней)..."
            )

            articles = await self.summary_generator.get_articles_with_links(days=days)

            if not articles:
                await update.message.reply_text(
                    "📭 За указанный период новостей не найдено."
                )
                return

            summary = self.summary_generator.format_summary_with_links(articles, days)

            if len(summary) > 4000:
                parts = [summary[i : i + 4000] for i in range(0, len(summary), 4000)]
                for i, part in enumerate(parts, 1):
                    await self._safe_send_message(
                        update, f"📰 Часть {i}/{len(parts)}:\n\n{part}"
                    )
            else:
                await self._safe_send_message(
                    update, f"📰 *Сводка за {days} день(дней):*\n\n{summary}"
                )

        except Exception as e:
            logger.error(f"Ошибка генерации сводки: {e}")
            await update.message.reply_text("❌ Произошла ошибка при генерации сводки.")

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Статистика базы данных"""
        try:
            repo = ArticleRepository()

            total_count = await repo.get_articles_count()
            sources_stats = await repo.get_sources_stats()
            daily_stats = await repo.get_daily_stats(days=7)

            max_count = max([count for _, count in daily_stats]) if daily_stats else 1
            bar_width = 10

            stats_text = "📊 *Статистика базы данных*\n\n"
            stats_text += f"• Всего новостей: *{total_count}*\n"

            stats_text += "\n📰 *По источникам:*\n"
            for source_name, count in sources_stats:
                stats_text += f"• {source_name}: *{count}*\n"

            stats_text += "\n📅 *За последние 7 дней:*\n"
            for date, count in daily_stats:
                date_str = date.strftime("%d.%m")
                bar_length = int((count / max_count) * bar_width)
                bar = "█" * bar_length if bar_length > 0 else "░"
                stats_text += f"• {date_str}: {bar} *{count}*\n"

            stats_text += f"\n👥 *Подписчики:* {len(self.subscribers)}"

            await update.message.reply_text(stats_text, parse_mode="Markdown")

        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            await update.message.reply_text("❌ Ошибка получения статистики.")

    async def news_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Поиск новостей по ключевым словам с ссылками"""
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите поисковый запрос.\nПример: `/news криптовалюта`",
                parse_mode="Markdown",
            )
            return

        query = " ".join(context.args)
        await update.message.reply_text(
            f"🔍 Ищу новости по запросу: *{query}*...", parse_mode="Markdown"
        )

        try:
            repo = ArticleRepository()
            articles = await repo.search_articles_with_links(query, limit=10)

            if not articles:
                await update.message.reply_text(
                    f"📭 Новостей по запросу '{query}' не найдено."
                )
                return

            result_text = f"🔍 *Результаты поиска по запросу '{query}':*\n\n"

            for i, article in enumerate(articles, 1):
                title = safe_markdown_text(
                    article.get("raw_title", "Без заголовка")[:80]
                )
                published = article.get("published_at")
                time_str = (
                    published.strftime("%d.%m %H:%M")
                    if published
                    else "Дата неизвестна"
                )
                url = article.get("url", "")

                result_text += f"{i}. *{title}*...\n"
                result_text += f"   🕐 {time_str}\n"
                if url:
                    result_text += f"   🔗 {url}\n\n"

                if len(result_text) > 3500:
                    result_text += f"...\n*Показаны первые {i} результатов*"
                    break

            await self._safe_send_message(update, result_text)

        except Exception as e:
            logger.error(f"Ошибка поиска новостей: {e}")
            await update.message.reply_text("❌ Ошибка при поиске новостей.")

    async def brief_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, hours: int = None
    ):
        """Показать суммаризацию за последние N часов"""
        try:
            if hours is None:
                hours = 24
                if context.args and len(context.args) > 0:
                    try:
                        hours = int(context.args[0])
                        if hours < 1 or hours > 168:
                            hours = 24
                    except ValueError:
                        hours = 24

            period_start = datetime.now() - timedelta(hours=hours)

            summaries = await SummarizationRepository.get_summaries_for_period(
                period_start, datetime.now(), "hour"
            )

            if not summaries:
                await update.message.reply_text(
                    f"📭 Нет суммаризаций за последние {hours} часов.\n"
                    "Попробуйте позже, данные ещё собираются."
                )
                return

            response = f"📊 *Сводка за последние {hours} часов*\n\n"

            for s in summaries[-12:]:
                period_start = s["period_start"]
                content = s["content"]
                if isinstance(content, dict):
                    time_str = period_start.strftime("%H:%M")
                    summary = content.get("summary", "Нет данных")
                    response += f"🕐 *{time_str}*: {summary}\n\n"

            if len(summaries) > 12:
                response += f"\n📌 *Формат:* показаны последние 12 часов. "
                response += f"Всего часовых суммаризаций за период: {len(summaries)}"

            await self._safe_send_message(update, response)

        except Exception as e:
            logger.error(f"Ошибка команды /brief: {e}")
            await update.message.reply_text("❌ Ошибка получения сводки")

    async def daily_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать дневную суммаризацию за вчера"""
        try:
            yesterday = (datetime.now() - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            today = yesterday + timedelta(days=1)

            summaries = await SummarizationRepository.get_summaries_for_period(
                yesterday, today, "day"
            )

            if not summaries:
                await update.message.reply_text(
                    "📭 Дневная суммаризация за вчера ещё не готова.\n"
                    "Обычно она появляется к 10 утра."
                )
                return

            s = summaries[0]
            content = s["content"]

            if isinstance(content, dict):
                response = f"📅 *Сводка за {yesterday.strftime('%d.%m.%Y')}*\n\n"
                response += f"📌 *Главные темы:*\n"
                for topic in content.get("topics", []):
                    response += f"• {topic}\n"

                response += (
                    f"\n📝 *Суть дня:*\n{content.get('summary', 'Нет данных')}\n"
                )

                if content.get("trend"):
                    response += f"\n📈 *Тренд:* {content.get('trend')}\n"

                if content.get("important_events"):
                    response += f"\n⚡ *Важные события:*\n"
                    for event in content.get("important_events", []):
                        response += f"• {event}\n"

                await self._safe_send_message(update, response)
            else:
                await update.message.reply_text("❌ Не удалось разобрать суммаризацию")

        except Exception as e:
            logger.error(f"Ошибка команды /daily: {e}")
            await update.message.reply_text("❌ Ошибка получения дневной сводки")

    async def ask_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Открытый вопрос к новостям"""
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите вопрос.\nПример: `/ask Что происходит с рублём?`",
                parse_mode="Markdown",
            )
            return

        question = " ".join(context.args)
        status_msg = await update.message.reply_text(
            "🤔 Анализирую новости, подождите секунду..."
        )

        try:
            last_hours = 24
            period_start = datetime.now() - timedelta(hours=last_hours)

            articles = (
                await SummarizationRepository.get_smart_articles_for_summarization(
                    period_start, datetime.now(), total_limit=50
                )
            )

            summaries = await SummarizationRepository.get_summaries_for_period(
                period_start, datetime.now(), "hour"
            )
            summaries = summaries[-12:]

            prompt = self._build_qa_prompt(question, articles, summaries)
            result = await self.llm_analyzer._raw_request(prompt)

            response = f"❓ *Вопрос:* {question}\n\n"
            response += (
                f"📚 *Ответ на основе новостей за последние {last_hours} часов:*\n\n"
            )
            response += result

            await status_msg.delete()
            await self._safe_send_message(update, response)

        except Exception as e:
            logger.error(f"Ошибка команды /ask: {e}")
            await status_msg.edit_text(
                "❌ Не удалось проанализировать новости. Попробуйте позже."
            )

    async def health_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Техническая информация"""
        import platform

        admin_id = os.getenv("ADMIN_CHAT_ID")
        is_admin = admin_id and str(update.effective_user.id) == admin_id

        health_text = f"""
⚙️ *Техническая информация*

🤖 *Бот:*
• Статус: ✅ Активен
• Прокси: {'✅ используется' if self.proxy_url else '❌ не используется'}
• Подписчики: {len(self.subscribers)}

💻 *Система:*
• Python: {platform.python_version()}
• Время сервера: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """

        if is_admin:
            import psutil

            health_text += f"""
📊 *Системные ресурсы:*
• CPU: {psutil.cpu_percent(interval=1)}%
• RAM: {psutil.virtual_memory().percent}%
            """

        await update.message.reply_text(health_text, parse_mode="Markdown")

    # --------------------------------------------------------
    # РАССЫЛКА
    # --------------------------------------------------------

    async def send_daily_digest(self, context: ContextTypes.DEFAULT_TYPE):
        """Отправка ежедневного дайджеста всем подписчикам"""
        logger.info("📨 Отправка ежедневного дайджеста...")

        try:
            summary = await self.summary_generator.generate_daily_summary(days=1)

            if not summary:
                logger.warning("Не удалось сгенерировать дайджест")
                return

            for chat_id in list(self.subscribers.keys()):
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"📰 *Ежедневный дайджест новостей*\n\n{summary}",
                        parse_mode="Markdown",
                    )
                    self.subscribers[chat_id]["last_sent"] = datetime.now()
                    logger.info(f"✅ Дайджест отправлен chat_id: {chat_id}")
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки chat_id {chat_id}: {e}")
                    if "bot was blocked" in str(e):
                        del self.subscribers[chat_id]
                        logger.info(f"Удален заблокировавший пользователь: {chat_id}")

        except Exception as e:
            logger.error(f"Ошибка отправки дайджеста: {e}")

    # --------------------------------------------------------
    # ОБРАБОТЧИКИ
    # --------------------------------------------------------

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик нажатий на кнопки"""
        query = update.callback_query
        await query.answer()
        data = query.data

        # Главное меню
        if data == "main_menu":
            await self._show_main_menu(query)

        # Меню сводок
        elif data == "menu_summaries":
            await self._show_summaries_menu(query)
        elif data == "summary_yesterday":
            await self.daily_command(query, context)
        elif data == "brief_6h":
            await self._quick_brief(query, 6)
        elif data == "brief_12h":
            await self._quick_brief(query, 12)
        elif data == "brief_24h":
            await self._quick_brief(query, 24)

        # Меню поиска
        elif data == "menu_search":
            await self._show_search_menu(query)
        elif data.startswith("search_popular:"):
            keyword = data.split(":")[1]
            await self._quick_search(query, keyword, context)

        # Меню статистики
        elif data == "menu_stats":
            await self._show_stats_menu(query)
        elif data == "stats_overall":
            await self.stats_command(query, context)
        elif data == "stats_hourly":
            await self._hourly_stats(query, context)

        # Меню подписки
        elif data == "menu_subscribe":
            await self._show_subscribe_menu(query)
        elif data == "subscribe_daily":
            await self.subscribe_command(query, context)
        elif data == "subscribe_unsubscribe":
            await self.unsubscribe_command(query, context)

        # Меню помощи
        elif data == "menu_help":
            await self._show_help_menu(query)

        # Задать вопрос
        elif data == "menu_ask":
            await self._ask_question_prompt(query)

        # Обратная совместимость
        elif data == "stats":
            await self.stats_command(query, context)
        elif data == "summary_today":
            await self.summary_command(query, context)
        elif data == "subscribe":
            await self.subscribe_command(query, context)

    def setup_handlers(self):
        """Настройка обработчиков команд"""
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(
            CommandHandler("subscribe", self.subscribe_command)
        )
        self.application.add_handler(
            CommandHandler("unsubscribe", self.unsubscribe_command)
        )
        self.application.add_handler(CommandHandler("summary", self.summary_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
        self.application.add_handler(CommandHandler("news", self.news_command))
        self.application.add_handler(CommandHandler("health", self.health_command))
        self.application.add_handler(CommandHandler("brief", self.brief_command))
        self.application.add_handler(CommandHandler("daily", self.daily_command))
        self.application.add_handler(CommandHandler("ask", self.ask_command))
        self.application.add_handler(CallbackQueryHandler(self.button_handler))

    def setup_scheduler(self):
        """Настройка планировщика задач"""
        if self.subscribers:
            self.application.job_queue.run_daily(
                self.send_daily_digest,
                time=time(hour=20, minute=0),
                days=(0, 1, 2, 3, 4, 5, 6),
            )
            logger.info("⏰ Планировщик дайджестов настроен на 20:00")

    def run(self):
        """Запуск бота"""
        self.setup_handlers()
        self.setup_scheduler()

        logger.info("🚀 Запуск телеграм-бота...")
        if self.proxy_url:
            logger.info("📡 Прокси настроен")

        self.application.run_polling(drop_pending_updates=True, allowed_updates=None)


# ============================================================
# ТОЧКА ВХОДА
# ============================================================


def main():
    """Точка входа"""
    from dotenv import load_dotenv

    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    proxy_url = os.getenv("PROXY_URL")

    if not token:
        logger.error("❌ TELEGRAM_BOT_TOKEN не установлен")
        return

    if proxy_url and not proxy_url.startswith("socks"):
        parts = proxy_url.split(":")
        if len(parts) == 4:
            host, port, user, password = parts
            proxy_url = f"socks5://{user}:{password}@{host}:{port}"
            logger.info("✅ Прокси преобразован в стандартный формат")

    bot = NewsTelegramBot(token, proxy_url)

    try:
        bot.run()
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
