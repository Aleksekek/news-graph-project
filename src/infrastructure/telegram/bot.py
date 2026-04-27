"""
Telegram бот для News Graph Project.
Использует новые репозитории и сервисы.
"""

import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
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
from src.database.repositories.article_repository import ArticleRepository
from src.database.repositories.summary_repository import SummaryRepository
from src.processing.llm.deepseek import DeepSeekAnalyzer
from src.processing.summarization.formatter import SummaryFormatter
from src.utils.datetime_utils import format_for_display, now_msk
from src.utils.telegram_helpers import safe_markdown_text

logger = logging.getLogger(__name__)


class NewsTelegramBot:
    """Новостной телеграм бот с интерактивным меню."""

    def __init__(self, token: str, proxy_url: Optional[str] = None):
        self.token = token
        self.proxy_url = proxy_url
        self.subscribers: Dict[int, Dict] = {}

        # Репозитории и сервисы
        self.article_repo = ArticleRepository()
        self.summary_repo = SummaryRepository()
        self.llm = DeepSeekAnalyzer()
        self.formatter = SummaryFormatter()

        # Настройка прокси
        self.application = self._build_application()

    def _build_application(self) -> Application:
        """Создание приложения с прокси если нужно."""
        if self.proxy_url:
            logger.info("✅ Бот будет использовать прокси")
            # Преобразуем прокси если нужно
            proxy = self._normalize_proxy_url(self.proxy_url)

            request = HTTPXRequest(
                proxy=proxy,
                connect_timeout=30.0,
                read_timeout=30.0,
                write_timeout=30.0,
                httpx_kwargs={"verify": False},
            )
            return Application.builder().token(self.token).request(request).build()
        else:
            logger.info("✅ Бот будет использовать прямое подключение")
            return Application.builder().token(self.token).build()

    def _normalize_proxy_url(self, proxy_url: str) -> str:
        """Нормализация URL прокси."""
        if proxy_url.startswith(("http://", "https://", "socks5://")):
            return proxy_url

        # Формат: ip:port:login:password
        parts = proxy_url.split(":")
        if len(parts) == 4:
            host, port, user, password = parts
            return f"socks5://{user}:{password}@{host}:{port}"

        return proxy_url

    async def _safe_send_message(self, update: Update, text: str, parse_mode: str = "MarkdownV2"):
        """Безопасная отправка сообщения с Markdown."""
        try:
            await update.message.reply_text(text, parse_mode=parse_mode)
        except Exception as e:
            logger.warning(f"Ошибка Markdown, отправка без разметки: {e}")
            clean_text = re.sub(r"[*_`]", "", text)
            await update.message.reply_text(clean_text)

    # ==================== КОМАНДЫ ====================

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start."""
        user = update.effective_user

        welcome_text = f"""
👋 Привет, {user.first_name}!

Я — новостной бот проекта News Graph Project.

📊 Умею:
• 📰 Показывать свежие новости
• 🤖 Отвечать на вопросы по новостям
• 📈 Анализировать тренды
• 🔔 Присылать ежедневные дайджесты

Используйте /help для списка команд.
        """

        await update.message.reply_text(welcome_text)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help."""
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

    async def brief_command(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        hours: Optional[int] = None,
    ):
        """Сводка за последние N часов."""
        try:
            if hours is None and context.args:
                try:
                    hours = int(context.args[0])
                    hours = max(1, min(168, hours))  # от 1 до 168 часов
                except ValueError:
                    hours = 6

            if hours is None:
                hours = 6

            period_start = now_msk() - timedelta(hours=hours)

            summaries = await self.summary_repo.get_for_period(period_start, now_msk(), "hour")

            if not summaries:
                await update.message.reply_text(
                    f"📭 Нет суммаризаций за последние {hours} часов.\n" "Попробуйте позже."
                )
                return

            response = f"📊 *Сводка за последние {hours} часов*\n\n"

            for s in summaries[-12:]:
                time_str = s["period_start"].strftime("%H:%M")
                content = s["content"]
                if isinstance(content, dict):
                    summary = content.get("summary", "Нет данных")
                    response += f"🕐 *{time_str}*: {summary}\n\n"

            await self._safe_send_message(update, response)

        except Exception as e:
            logger.error(f"Ошибка brief_command: {e}")
            await update.message.reply_text("❌ Ошибка получения сводки")

    async def daily_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Дневная сводка за вчера."""
        try:
            yesterday = (now_msk() - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            today = yesterday + timedelta(days=1)

            summaries = await self.summary_repo.get_for_period(yesterday, today, "day")

            if not summaries:
                await update.message.reply_text("📭 Дневная суммаризация за вчера ещё не готова.")
                return

            s = summaries[0]
            content = s["content"]

            if isinstance(content, dict):
                response = (
                    f"📅 *Сводка за {yesterday.strftime('%d.%m.%Y')}*\n\n" f"📌 *Главные темы:*\n"
                )
                for topic in content.get("topics", [])[:5]:
                    response += f"• {topic}\n"

                response += f"\n📝 *Суть дня:*\n{content.get('summary', 'Нет данных')}\n"

                if content.get("trend"):
                    response += f"\n📈 *Тренд:* {content.get('trend')}\n"

                await self._safe_send_message(update, response)
            else:
                await update.message.reply_text("❌ Не удалось разобрать суммаризацию")

        except Exception as e:
            logger.error(f"Ошибка daily_command: {e}")
            await update.message.reply_text("❌ Ошибка получения дневной сводки")

    async def news_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Поиск новостей по ключевым словам."""
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
            articles = await self.article_repo.search(query, limit=10, with_urls=True)

            if not articles:
                await update.message.reply_text(f"📭 Новостей по запросу '{query}' не найдено.")
                return

            result_text = f"🔍 *Результаты поиска по запросу '{query}':*\n\n"

            for i, article in enumerate(articles, 1):
                title = safe_markdown_text(article.get("raw_title", "Без заголовка")[:70])
                published = article.get("published_at")
                time_str = (
                    format_for_display(published, include_time=True)
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
            logger.error(f"Ошибка news_command: {e}")
            await update.message.reply_text("❌ Ошибка при поиске новостей")

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Статистика базы данных."""
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

    async def ask_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Ответ на вопрос на основе новостей."""
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите вопрос.\nПример: `/ask Что происходит с рублём?`",
                parse_mode="Markdown",
            )
            return

        question = " ".join(context.args)
        status_msg = await update.message.reply_text("🤔 Анализирую новости, подождите секунду...")

        try:
            # Получаем последние статьи для контекста
            articles = await self.article_repo.get_unprocessed(limit=30)

            if not articles:
                await status_msg.edit_text("📭 Нет свежих новостей для анализа")
                return

            # Формируем промпт
            prompt = self._build_qa_prompt(question, articles)

            # Запрашиваем LLM
            answer = await self.llm.raw_request(prompt)

            response = f"❓ *Вопрос:* {question}\n\n"
            response += f"📚 *Ответ:*\n\n{answer}"

            await status_msg.delete()
            await self._safe_send_message(update, response)

        except Exception as e:
            logger.error(f"Ошибка ask_command: {e}")
            await status_msg.edit_text("❌ Не удалось проанализировать новости")

    async def subscribe_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подписка на рассылку."""
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
        """Отписка от рассылки."""
        chat_id = update.effective_chat.id

        if chat_id in self.subscribers:
            del self.subscribers[chat_id]
            await update.message.reply_text("✅ Вы отписались от рассылки новостей.")
        else:
            await update.message.reply_text("❌ Вы не были подписаны на рассылку.")

    async def health_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Техническая информация."""
        import platform

        admin_id = settings.ADMIN_CHAT_ID
        is_admin = admin_id and str(update.effective_user.id) == admin_id

        health_text = f"""
⚙️ *Техническая информация*

🤖 *Бот:*
• Статус: ✅ Активен
• Прокси: {'✅ используется' if self.proxy_url else '❌ не используется'}
• Подписчики: {len(self.subscribers)}

💻 *Система:*
• Python: {platform.python_version()}
• Время сервера: {format_for_display(now_msk())}

📊 *БД:*
• Всего статей: {await self._get_total_count()}
        """

        await update.message.reply_text(health_text, parse_mode="Markdown")

    async def _get_total_count(self) -> int:
        """Получение общего количества статей."""
        try:
            stats = await self.article_repo.get_stats()
            return stats.get("total", 0)
        except:
            return 0

    def _build_qa_prompt(self, question: str, articles: List[Dict]) -> str:
        """Строит промпт для ответа на вопрос."""
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

    async def send_daily_digest(self, context: ContextTypes.DEFAULT_TYPE):
        """Отправка ежедневного дайджеста подписчикам."""
        logger.info("📨 Отправка ежедневного дайджеста...")

        try:
            yesterday = (now_msk() - timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

            summaries = await self.summary_repo.get_for_period(
                yesterday, yesterday + timedelta(days=1), "day"
            )

            if not summaries:
                logger.warning("Дайджест не готов")
                return

            summary_text = self.formatter.format_daily_digest(summaries[0])

            for chat_id in list(self.subscribers.keys()):
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"📰 *Ежедневный дайджест*\n\n{summary_text}",
                        parse_mode="Markdown",
                    )
                    logger.info(f"✅ Дайджест отправлен {chat_id}")
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки {chat_id}: {e}")
                    if "bot was blocked" in str(e):
                        del self.subscribers[chat_id]

        except Exception as e:
            logger.error(f"Ошибка отправки дайджеста: {e}")

    def setup_handlers(self):
        """Настройка обработчиков команд."""
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

    def run(self):
        """Запуск бота."""
        self.setup_handlers()

        logger.info("🚀 Запуск телеграм-бота...")
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
