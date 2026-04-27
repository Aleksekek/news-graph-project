import asyncio
import logging
import os
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

# Импорты из вашего проекта
from src.config.settings import settings
from src.domain.processing.summary_generator import SummaryGenerator
from src.domain.storage.database import ArticleRepository

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class NewsTelegramBot:
    def __init__(self, token: str, proxy_url: Optional[str] = None):
        self.token = token
        self.proxy_url = proxy_url
        self.subscribers: Dict[int, Dict] = {}  # chat_id -> настройки
        self.summary_generator = SummaryGenerator()

        # Настройка прокси через переменные окружения и HTTPXRequest
        if proxy_url:
            logger.info("✅ Бот будет использовать прокси")

            # Устанавливаем переменные окружения для всех HTTP-запросов
            os.environ["HTTP_PROXY"] = proxy_url
            os.environ["HTTPS_PROXY"] = proxy_url
            os.environ["ALL_PROXY"] = proxy_url

            # Создаем request с прокси
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

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        user = update.effective_user
        chat_id = update.effective_chat.id

        welcome_text = f"""
👋 Привет, {user.first_name}!

Я — новостной бот проекта News Graph Project.

📊 Что я умею:
• Присылать ежедневные дайджесты новостей
• Искать новости по ключевым словам
• Показывать статистику по базе данных

🎛 Доступные команды:
/help - Показать это сообщение
/subscribe - Подписаться на рассылку
/unsubscribe - Отписаться от рассылки
/summary [дней] - Сводка за N дней
/stats - Статистика базы данных
/news [запрос] - Поиск новостей
/health - Техническая информация
        """

        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data="stats")],
            [InlineKeyboardButton("📰 Сводка за день", callback_data="summary_today")],
            [InlineKeyboardButton("🔔 Подписаться", callback_data="subscribe")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(welcome_text, reply_markup=reply_markup)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        help_text = """
📚 *Список команд:*

*Основные:*
/start - Начало работы
/help - Помощь и список команд
/subscribe - Подписаться на рассылку
/unsubscribe - Отписаться от рассылки

*Новости:*
/summary [дней] - Сводка за N дней (по умолчанию 1)
/news [запрос] - Поиск новостей по ключевым словам
/stats - Статистика базы данных

*Технические:*
/health - Техническая информация о боте и сервере
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
        """Генерация сводки за период"""
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

            summary = await self.summary_generator.generate_daily_summary(days=days)

            if summary:
                # Используем HTML вместо Markdown — он более надёжный
                if len(summary) > 4000:
                    parts = [
                        summary[i : i + 4000] for i in range(0, len(summary), 4000)
                    ]
                    for i, part in enumerate(parts, 1):
                        # Убираем parse_mode для длинных сообщений
                        await update.message.reply_text(
                            f"📰 Часть {i}/{len(parts)}:\n\n{part}"
                        )
                else:
                    # Пробуем с parse_mode, но если ошибка — отправляем без
                    try:
                        await update.message.reply_text(
                            f"📰 *Сводка за {days} день(дней):*\n\n{summary}",
                            parse_mode="Markdown",
                        )
                    except Exception as e:
                        if "Can't parse entities" in str(e):
                            # Отправляем без разметки
                            await update.message.reply_text(
                                f"📰 Сводка за {days} день(дней):\n\n{summary}"
                            )
                        else:
                            raise
            else:
                await update.message.reply_text(
                    "❌ Не удалось сгенерировать сводку. Попробуйте позже."
                )

        except Exception as e:
            logger.error(f"Ошибка генерации сводки: {e}")
            await update.message.reply_text("❌ Произошла ошибка при генерации сводки.")

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Статистика базы данных с красивыми полосками"""
        try:
            repo = ArticleRepository()

            total_count = await repo.get_articles_count()
            sources_stats = await repo.get_sources_stats()
            daily_stats = await repo.get_daily_stats(days=7)

            # Находим максимальное значение для масштабирования
            max_count = max([count for _, count in daily_stats]) if daily_stats else 1
            bar_width = 10  # ширина полоски в символах

            stats_text = "📊 *Статистика базы данных*\n\n"
            stats_text += f"• Всего новостей: *{total_count}*\n"

            stats_text += "\n📰 *По источникам:*\n"
            for source_name, count in sources_stats:
                stats_text += f"• {source_name}: *{count}*\n"

            stats_text += "\n📅 *За последние 7 дней:*\n"
            for date, count in daily_stats:
                date_str = date.strftime("%d.%m")
                # Масштабируем полоску: (count / max_count) * bar_width
                bar_length = int((count / max_count) * bar_width)
                bar = "█" * bar_length if bar_length > 0 else "░"
                stats_text += f"• {date_str}: {bar} *{count}*\n"

            stats_text += f"\n👥 *Подписчики:* {len(self.subscribers)}"

            await update.message.reply_text(stats_text, parse_mode="Markdown")

        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            await update.message.reply_text("❌ Ошибка получения статистики.")

    async def news_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Поиск новостей по ключевым словам"""
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
            articles = await repo.search_articles(query, limit=10)

            if not articles:
                await update.message.reply_text(
                    f"📭 Новостей по запросу '{query}' не найдено."
                )
                return

            result_text = f"🔍 *Результаты поиска по запросу '{query}':*\n\n"

            for i, article in enumerate(articles, 1):
                title = article.get("raw_title", "Без заголовка")[:80]
                published = article.get("published_at")
                if published:
                    time_str = published.strftime("%d.%m %H:%M")
                else:
                    time_str = "Дата неизвестна"
                result_text += f"{i}. *{title}*...\n"
                result_text += f"   🕐 {time_str}\n\n"

                if len(result_text) > 3000:
                    result_text += "...\n*Слишком много результатов, показаны первые 5*"
                    break

            await update.message.reply_text(result_text, parse_mode="Markdown")

        except Exception as e:
            logger.error(f"Ошибка поиска новостей: {e}")
            await update.message.reply_text("❌ Ошибка при поиске новостей.")

    async def health_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Техническая информация"""
        import platform

        # Проверяем админские права (простой список)
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
            # Добавляем дополнительную информацию для админа
            import psutil

            health_text += f"""
📊 *Системные ресурсы:*
• CPU: {psutil.cpu_percent(interval=1)}%
• RAM: {psutil.virtual_memory().percent}%
            """

        await update.message.reply_text(health_text, parse_mode="Markdown")

    async def send_daily_digest(self, context: ContextTypes.DEFAULT_TYPE):
        """Отправка ежедневного дайджеста всем подписчикам"""
        logger.info("📨 Отправка ежедневного дайджеста...")

        try:
            summary = await self.summary_generator.generate_daily_summary(days=1)

            if not summary:
                logger.warning("Не удалось сгенерировать дайджест")
                return

            # Отправляем всем подписчикам
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

    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик нажатий на кнопки"""
        query = update.callback_query
        await query.answer()

        data = query.data

        if data == "stats":
            await self.stats_command(update, context)
        elif data == "summary_today":
            await self.summary_command(update, context)
        elif data == "subscribe":
            await self.subscribe_command(update, context)

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

        # Обработчики инлайн-кнопок
        self.application.add_handler(CallbackQueryHandler(self.button_handler))

    def setup_scheduler(self):
        """Настройка планировщика задач"""
        # Ежедневная рассылка в 20:00
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
            logger.info(f"📡 Прокси настроен")

        # Запускаем polling
        self.application.run_polling(drop_pending_updates=True, allowed_updates=None)


def main():
    """Точка входа"""
    from dotenv import load_dotenv

    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    proxy_url = os.getenv("PROXY_URL")

    if not token:
        logger.error("❌ TELEGRAM_BOT_TOKEN не установлен")
        return

    # Преобразуем прокси если нужно
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
