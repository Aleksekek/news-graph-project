"""
Логика сводок для Telegram бота.
"""

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from src.database.repositories.summary_repository import SummaryRepository
from src.utils.datetime_utils import format_for_display, now_msk

logger = logging.getLogger(__name__)

MSK_TZ = ZoneInfo("Europe/Moscow")


class BriefHandlers:
    """Обработчики сводок."""

    def __init__(self, summary_repo: SummaryRepository):
        self.summary_repo = summary_repo

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

            now_msk_time = now_msk()
            period_start = now_msk_time - timedelta(hours=hours)

            summaries = await self.summary_repo.get_for_period(period_start, now_msk_time, "hour")

            if not summaries:
                await update.message.reply_text(f"📭 Нет суммаризаций за последние {hours} часов.")
                return

            response = f"📊 *Сводка за последние {hours} часов*\n\n"

            for s in summaries[-12:]:
                period_start_val = s["period_start"]
                # Убеждаемся, что время в MSK
                if period_start_val.tzinfo is None:
                    period_start_val = period_start_val.replace(tzinfo=MSK_TZ)
                else:
                    period_start_val = period_start_val.astimezone(MSK_TZ)

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
        return 3  # CUSTOM_BRIEF_STATE

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
                if hasattr(update, "message") and update.message:
                    await update.message.reply_text(msg)
                elif hasattr(update, "edit_message_text"):
                    await update.edit_message_text(msg)
                else:
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
                error_msg = "❌ Не удалось разобрать суммаризацию"
                if hasattr(update, "message") and update.message:
                    await update.message.reply_text(error_msg)
                else:
                    await update.callback_query.edit_message_text(error_msg)

        except Exception as e:
            logger.error(f"Ошибка daily_command: {e}")
            error_msg = "❌ Ошибка получения дневной сводки"
            if hasattr(update, "message") and update.message:
                await update.message.reply_text(error_msg)
            elif hasattr(update, "edit_message_text"):
                await update.edit_message_text(error_msg)
            else:
                await update.callback_query.edit_message_text(error_msg)
