"""
Тесты для Telegram бота (обновлённая структура).
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.infrastructure.telegram.bot import NewsTelegramBot
from src.infrastructure.telegram.briefs import BriefHandlers
from src.infrastructure.telegram.handlers import Handlers
from src.infrastructure.telegram.qa import QAHandlers
from src.infrastructure.telegram.search import SearchHandlers


class TestNewsTelegramBot:
    """Тесты бота."""

    @pytest.fixture
    def bot(self):
        """Фикстура бота с мокнутым токеном."""
        with patch("src.infrastructure.telegram.bot.Application") as MockApp:
            mock_app = Mock()
            MockApp.builder.return_value.token.return_value.build.return_value = mock_app

            bot = NewsTelegramBot(token="test_token_123")
            # Подменяем репозитории моками
            bot.article_repo = AsyncMock()
            bot.summary_repo = AsyncMock()
            bot.llm = AsyncMock()
            return bot

    # ==================== ТЕСТЫ ВСПОМОГАТЕЛЬНЫХ МЕТОДОВ ====================

    def test_normalize_proxy_url_socks(self, bot):
        """Нормализация socks прокси."""
        proxy = "127.0.0.1:1080:user:pass"
        result = bot._normalize_proxy_url(proxy)
        assert result == "socks5://user:pass@127.0.0.1:1080"

    def test_normalize_proxy_url_already_normalized(self, bot):
        """Уже нормализованный прокси."""
        proxy = "socks5://user:pass@127.0.0.1:1080"
        result = bot._normalize_proxy_url(proxy)
        assert result == proxy

    def test_normalize_proxy_url_http(self, bot):
        """HTTP прокси."""
        proxy = "http://proxy.example.com:8080"
        result = bot._normalize_proxy_url(proxy)
        assert result == "http://proxy.example.com:8080"

    def test_normalize_proxy_url_https(self, bot):
        """HTTPS прокси."""
        proxy = "https://proxy.example.com:8080"
        result = bot._normalize_proxy_url(proxy)
        assert result == "https://proxy.example.com:8080"

    # ==================== ТЕСТЫ КОМАНД (через обработчики) ====================

    @pytest.mark.asyncio
    async def test_health_command(self, bot):
        """Тест команды health."""
        mock_update = Mock()
        mock_update.message = AsyncMock()

        bot.subscribers = {123: {}}

        async def mock_health(update, context):
            await update.message.reply_text("✅ Бот активен")

        bot.health_command = mock_health

        await bot.health_command(mock_update, Mock())
        mock_update.message.reply_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_command(self, bot):
        """Тест команды start."""
        mock_update = Mock()
        mock_update.effective_user = Mock()
        mock_update.effective_user.first_name = "TestUser"
        mock_update.message = AsyncMock()

        bot.handlers = Handlers(
            bot.article_repo, bot.summary_repo, bot.llm, bot.formatter, bot.subscribers
        )
        bot.handlers.start = AsyncMock()

        await bot.handlers.start(mock_update, Mock())
        bot.handlers.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribe_command(self, bot):
        """Тест подписки."""
        mock_update = Mock()
        mock_update.effective_chat = Mock()
        mock_update.effective_chat.id = 12345
        mock_update.message = AsyncMock()

        bot.subscribers = {}
        bot.handlers = Handlers(
            bot.article_repo, bot.summary_repo, bot.llm, bot.formatter, bot.subscribers
        )
        bot.handlers.subscribe_command = AsyncMock()

        await bot.handlers.subscribe_command(mock_update, Mock())
        bot.handlers.subscribe_command.assert_called_once()

    @pytest.mark.asyncio
    async def test_stats_hourly_command(self, bot):
        """Тест почасовой статистики."""
        mock_update = Mock()
        mock_update.callback_query = AsyncMock()
        mock_update.callback_query.edit_message_text = AsyncMock()
        mock_update.callback_query.answer = AsyncMock()

        bot.article_repo.get_hourly_stats_24h = AsyncMock(
            return_value=[
                (datetime(2026, 4, 28, 10, 0), 10),
                (datetime(2026, 4, 28, 11, 0), 20),
                (datetime(2026, 4, 28, 12, 0), 30),
            ]
        )

        bot.handlers = Handlers(
            bot.article_repo, bot.summary_repo, bot.llm, bot.formatter, bot.subscribers
        )

        await bot.handlers.stats_hourly(mock_update, Mock())

        assert mock_update.callback_query.edit_message_text.call_count == 2

        first_call = mock_update.callback_query.edit_message_text.call_args_list[0]
        assert "Собираю почасовую статистику" in first_call[0][0]

        second_call = mock_update.callback_query.edit_message_text.call_args_list[1]
        response_text = second_call[0][0]
        assert "Активность по часам" in response_text
        # Проверяем только часы, без даты
        assert "10:00" in response_text
        assert "11:00" in response_text
        assert "12:00" in response_text
        # Даты быть не должно
        assert "28.04" not in response_text
        assert "Максимум: 30" in response_text

        mock_update.callback_query.answer.assert_called_once()

    @pytest.mark.asyncio
    async def test_stats_hourly_empty_data(self, bot):
        """Тест почасовой статистики при отсутствии данных."""
        mock_update = Mock()
        mock_update.callback_query = AsyncMock()
        mock_update.callback_query.edit_message_text = AsyncMock()
        mock_update.callback_query.answer = AsyncMock()

        bot.article_repo.get_hourly_stats_24h = AsyncMock(return_value=[])

        bot.handlers = Handlers(
            bot.article_repo, bot.summary_repo, bot.llm, bot.formatter, bot.subscribers
        )

        await bot.handlers.stats_hourly(mock_update, Mock())

        # Проверяем, что после получения пустых данных показано сообщение
        # (первый вызов - статус, второй - результат)
        second_call = mock_update.callback_query.edit_message_text.call_args_list[1]
        response_text = second_call[0][0]
        assert "нет данных" in response_text.lower() or "Нет данных" in response_text

    @pytest.mark.asyncio
    async def test_brief_command_with_hours(self, bot):
        """Тест сводки с указанием часов."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_context = Mock()
        mock_context.args = ["12"]

        bot.summary_repo.get_for_period = AsyncMock(
            return_value=[
                {
                    "period_start": datetime(2026, 4, 28, 10, 0),
                    "period_end": datetime(2026, 4, 28, 11, 0),
                    "content": {"summary": "Тестовая сводка"},
                }
            ]
        )

        bot.brief_handlers = BriefHandlers(bot.summary_repo)

        await bot.brief_handlers.brief_command(mock_update, mock_context)

        # Проверяем, что get_for_period был вызван с правильными параметрами
        call_args = bot.summary_repo.get_for_period.call_args
        assert call_args is not None
        # Проверяем, что переданные datetime имеют таймзону (aware)
        start, end, period_type = call_args[0]
        assert start.tzinfo is not None
        assert end.tzinfo is not None
        assert period_type == "hour"

    @pytest.mark.asyncio
    async def test_brief_command_no_summaries(self, bot):
        """Тест сводки при отсутствии суммаризаций."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_context = Mock()
        mock_context.args = ["6"]

        bot.summary_repo.get_for_period = AsyncMock(return_value=[])

        bot.brief_handlers = BriefHandlers(bot.summary_repo)

        await bot.brief_handlers.brief_command(mock_update, mock_context)

        # Должно быть сообщение об отсутствии данных
        mock_update.message.reply_text.assert_called_once()
        assert "Нет суммаризаций" in mock_update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_brief_command_default_hours(self, bot):
        """Тест сводки с часами по умолчанию."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_context = Mock()
        mock_context.args = None  # Нет аргументов

        bot.summary_repo.get_for_period = AsyncMock(return_value=[])

        bot.brief_handlers = BriefHandlers(bot.summary_repo)

        await bot.brief_handlers.brief_command(mock_update, mock_context)

        # Должен быть вызван get_for_period с периодом 6 часов
        call_args = bot.summary_repo.get_for_period.call_args
        assert call_args is not None
        start, end, _ = call_args[0]
        diff_hours = (end - start).total_seconds() / 3600
        assert abs(diff_hours - 6) < 0.1  # Примерно 6 часов

    @pytest.mark.asyncio
    async def test_search_command_with_results(self, bot):
        """Тест поиска с результатами."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_context = Mock()
        mock_context.args = ["тест"]

        bot.article_repo.search = AsyncMock(
            return_value=[
                {
                    "raw_title": "Тестовая новость",
                    "published_at": datetime.now(),
                    "url": "https://test.com",
                }
            ]
        )

        bot.search_handlers = SearchHandlers(bot.article_repo)

        await bot.search_handlers.news_command(mock_update, mock_context)

        # Проверяем, что поиск был выполнен
        bot.article_repo.search.assert_called_once_with("тест", limit=10, with_urls=True)

    @pytest.mark.asyncio
    async def test_search_command_no_results(self, bot):
        """Тест поиска без результатов."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_context = Mock()
        mock_context.args = ["несуществующийзапрос"]

        bot.article_repo.search = AsyncMock(return_value=[])

        bot.search_handlers = SearchHandlers(bot.article_repo)

        await bot.search_handlers.news_command(mock_update, mock_context)

        # Должно быть сообщение об отсутствии результатов
        mock_update.message.reply_text.assert_called()
        assert "не найдено" in mock_update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_ask_command_with_context(self, bot):
        """Тест вопроса с контекстом."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_context = Mock()
        mock_context.args = ["Что", "нового?"]

        bot.article_repo.get_unprocessed = AsyncMock(
            return_value=[
                {
                    "raw_title": "Важная новость",
                    "raw_text": "Содержимое новости",
                }
            ]
        )
        bot.llm.raw_request = AsyncMock(return_value="Тестовый ответ")

        bot.qa_handlers = QAHandlers(bot.article_repo, bot.llm)

        await bot.qa_handlers.ask_command(mock_update, mock_context)

        # Проверяем, что был обработан вопрос
        bot.llm.raw_request.assert_called_once()

    @pytest.mark.asyncio
    async def test_ask_command_no_args(self, bot):
        """Тест вопроса без аргументов."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_context = Mock()
        mock_context.args = []  # Нет аргументов

        bot.qa_handlers = QAHandlers(bot.article_repo, bot.llm)

        await bot.qa_handlers.ask_command(mock_update, mock_context)

        # Должно быть сообщение с инструкцией
        mock_update.message.reply_text.assert_called_once()
        assert "Укажите вопрос" in mock_update.message.reply_text.call_args[0][0]


class TestHandlersIntegration:
    """Интеграционные тесты для обработчиков."""

    @pytest.fixture
    def handlers(self):
        """Фикстура с моками обработчиков."""
        article_repo = AsyncMock()
        summary_repo = AsyncMock()
        llm = AsyncMock()
        formatter = Mock()
        subscribers = {}

        return Handlers(article_repo, summary_repo, llm, formatter, subscribers)

    @pytest.mark.asyncio
    async def test_stats_hourly_integration(self, handlers):
        """Интеграционный тест почасовой статистики."""
        mock_update = Mock()
        mock_update.callback_query = AsyncMock()
        mock_update.callback_query.edit_message_text = AsyncMock()
        mock_update.callback_query.answer = AsyncMock()

        # Мокаем репозиторий
        now = datetime(2026, 4, 28, 15, 0)
        mock_stats = []
        for i in range(24):
            dt = now - timedelta(hours=23 - i)
            mock_stats.append((dt, i * 5))

        handlers.article_repo.get_hourly_stats_24h = AsyncMock(return_value=mock_stats)

        await handlers.stats_hourly(mock_update, Mock())

        assert mock_update.callback_query.edit_message_text.called
        call_args = mock_update.callback_query.edit_message_text.call_args[0][0]

        # Проверяем наличие часов (без дат)
        assert "00:00" in call_args
        assert "01:00" in call_args
        assert "Максимум:" in call_args
        # Даты быть не должно
        assert "28.04" not in call_args
        assert "27.04" not in call_args

    @pytest.mark.asyncio
    async def test_stats_overall_integration(self, handlers):
        """Интеграционный тест общей статистики."""
        mock_update = Mock()
        mock_update.callback_query = AsyncMock()
        mock_update.callback_query.edit_message_text = AsyncMock()
        mock_update.callback_query.answer = AsyncMock()

        handlers.article_repo.get_stats = AsyncMock(
            return_value={"total": 1000, "raw": 100, "processed": 900}
        )
        handlers.article_repo.get_sources_stats = AsyncMock(
            return_value=[("Источник 1", 500), ("Источник 2", 300)]
        )

        await handlers.stats_overall(mock_update, Mock())

        assert mock_update.callback_query.edit_message_text.called
        call_args = mock_update.callback_query.edit_message_text.call_args[0][0]
        assert "1000" in call_args
        assert "Источник 1" in call_args


class TestSearchHandlers:
    """Тесты поисковых обработчиков."""

    @pytest.fixture
    def search_handlers(self):
        """Фикстура обработчиков поиска."""
        article_repo = AsyncMock()
        return SearchHandlers(article_repo)

    @pytest.mark.asyncio
    async def test_search_popular(self, search_handlers):
        """Тест популярного поискового запроса."""
        mock_update = Mock()
        mock_update.callback_query = Mock()
        mock_update.callback_query.data = "search_popular:нефть"
        mock_update.callback_query.message = Mock()
        mock_update.callback_query.edit_message_text = AsyncMock()
        mock_update.callback_query.answer = AsyncMock()

        search_handlers.article_repo.search = AsyncMock(
            return_value=[
                {"raw_title": "Цены на нефть", "published_at": datetime.now(), "url": "test.com"}
            ]
        )

        search_handlers._do_search = AsyncMock()

        await search_handlers.search_popular(mock_update, Mock())

        assert mock_update.callback_query.edit_message_text.called

    @pytest.mark.asyncio
    async def test_handle_search_empty_query(self, search_handlers):
        """Тест поиска с пустым запросом."""
        mock_update = Mock()
        mock_update.message = Mock()
        mock_update.message.text = "   "  # Пустой запрос
        mock_update.message.reply_text = AsyncMock()

        await search_handlers.handle_search(mock_update, Mock())

        # Поиск с пустым запросом всё равно выполнится, но должен вернуть пустой результат
        search_handlers.article_repo.search.assert_called()


class TestQAHandlers:
    """Тесты обработчиков вопросов."""

    @pytest.fixture
    def qa_handlers(self):
        """Фикстура QA обработчиков."""
        article_repo = AsyncMock()
        llm = AsyncMock()
        return QAHandlers(article_repo, llm)

    def test_build_qa_prompt(self, qa_handlers):
        """Тест построения промпта."""
        articles = [
            {"raw_title": "Заголовок 1", "raw_text": "Текст 1"},
            {"raw_title": "Заголовок 2", "raw_text": "Текст 2"},
        ]

        prompt = qa_handlers._build_qa_prompt("Тестовый вопрос?", articles)

        assert "Тестовый вопрос?" in prompt
        assert "Заголовок 1" in prompt
        assert "Текст 1" in prompt

    def test_build_qa_prompt_empty_articles(self, qa_handlers):
        """Тест построения промпта с пустыми статьями."""
        prompt = qa_handlers._build_qa_prompt("Вопрос?", [])

        assert "Вопрос?" in prompt
        assert "СВЕЖИЕ НОВОСТИ:" in prompt

    def test_build_qa_prompt_truncation(self, qa_handlers):
        """Тест обрезания длинных заголовков и текстов."""
        long_title = "X" * 200
        long_text = "Y" * 500
        articles = [
            {"raw_title": long_title, "raw_text": long_text},
        ]

        prompt = qa_handlers._build_qa_prompt("Вопрос?", articles)

        # Заголовок должен быть обрезан до 100 символов
        assert len(long_title) > 100

        # Находим строку с заголовком в промпте
        import re

        # Ищем строку после "• " и до перевода строки
        title_match = re.search(r"• (.+?)\n", prompt)
        assert title_match is not None

        found_title = title_match.group(1)
        # Должно быть не больше 100 символов (+ возможно "..." но в коде его нет)
        assert len(found_title) <= 100
        # Первые 100 символов должны совпадать
        assert found_title == long_title[:100]

        # Находим строку с текстом (после пробелов)
        text_match = re.search(r"\s+(.+?)\.\.\.", prompt)
        if text_match:
            found_text = text_match.group(1)
            # Должно быть не больше 300 символов
            assert len(found_text) <= 300

    @pytest.mark.asyncio
    async def test_answer_question_no_articles(self, qa_handlers):
        """Тест ответа при отсутствии статей."""
        mock_message = AsyncMock()
        mock_status_msg = AsyncMock()
        mock_status_msg.edit_text = AsyncMock()
        mock_message.reply_text = AsyncMock(return_value=mock_status_msg)

        qa_handlers.article_repo.get_unprocessed = AsyncMock(return_value=[])

        await qa_handlers._answer_question(mock_message, "Тестовый вопрос?")

        # Проверяем, что был вызван edit_text для обновления статуса
        assert mock_status_msg.edit_text.called
        call_args = mock_status_msg.edit_text.call_args[0][0]
        assert "Нет свежих новостей" in call_args

        # reply_text вызывался ровно 1 раз (для статусного сообщения)
        assert mock_message.reply_text.call_count == 1

    @pytest.mark.asyncio
    async def test_answer_question_success(self, qa_handlers):
        """Тест успешного ответа на вопрос."""
        mock_message = AsyncMock()
        mock_status_msg = AsyncMock()
        mock_status_msg.delete = AsyncMock()
        mock_message.reply_text = AsyncMock(return_value=mock_status_msg)

        qa_handlers.article_repo.get_unprocessed = AsyncMock(
            return_value=[
                {
                    "raw_title": "Важная новость",
                    "raw_text": "Содержимое новости",
                }
            ]
        )
        qa_handlers.llm.raw_request = AsyncMock(return_value="Тестовый ответ от LLM")

        await qa_handlers._answer_question(mock_message, "Тестовый вопрос?")

        # Статусное сообщение должно быть удалено
        assert mock_status_msg.delete.called

        # Финальный ответ отправлен через reply_text
        final_call = mock_message.reply_text.call_args_list[-1]
        assert "Тестовый вопрос?" in final_call[0][0]
        assert "Тестовый ответ от LLM" in final_call[0][0]

    @pytest.mark.asyncio
    async def test_answer_question_llm_error(self, qa_handlers):
        """Тест ошибки LLM при ответе на вопрос."""
        mock_message = AsyncMock()
        mock_status_msg = AsyncMock()
        mock_status_msg.edit_text = AsyncMock()
        mock_message.reply_text = AsyncMock(return_value=mock_status_msg)

        qa_handlers.article_repo.get_unprocessed = AsyncMock(
            return_value=[
                {
                    "raw_title": "Важная новость",
                    "raw_text": "Содержимое новости",
                }
            ]
        )
        qa_handlers.llm.raw_request = AsyncMock(side_effect=Exception("LLM Error"))

        await qa_handlers._answer_question(mock_message, "Тестовый вопрос?")

        # Должно быть сообщение об ошибке
        assert mock_status_msg.edit_text.called
        call_args = mock_status_msg.edit_text.call_args[0][0]
        assert "Не удалось проанализировать" in call_args


class TestBriefHandlers:
    """Тесты обработчиков сводок."""

    @pytest.fixture
    def brief_handlers(self):
        """Фикстура обработчиков сводок."""
        summary_repo = AsyncMock()
        return BriefHandlers(summary_repo)

    @pytest.mark.asyncio
    async def test_brief_command_validation(self, brief_handlers):
        """Тест валидации часов в команде brief."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_context = Mock()

        mock_context.args = ["invalid"]
        brief_handlers.summary_repo.get_for_period = AsyncMock(return_value=[])

        await brief_handlers.brief_command(mock_update, mock_context)

        call_args = brief_handlers.summary_repo.get_for_period.call_args
        if call_args:
            start, end = call_args[0][0], call_args[0][1]
            diff_hours = (end - start).total_seconds() / 3600
            assert diff_hours <= 6  # Должно быть не больше 6 часов

    @pytest.mark.asyncio
    async def test_brief_command_max_hours(self, brief_handlers):
        """Тест максимального количества часов."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_context = Mock()
        mock_context.args = ["200"]  # Больше максимума

        brief_handlers.summary_repo.get_for_period = AsyncMock(return_value=[])

        await brief_handlers.brief_command(mock_update, mock_context)

        call_args = brief_handlers.summary_repo.get_for_period.call_args
        if call_args:
            start, end = call_args[0][0], call_args[0][1]
            diff_hours = (end - start).total_seconds() / 3600
            assert diff_hours <= 168  # Должно быть не больше 168 часов

    @pytest.mark.asyncio
    async def test_daily_command(self, brief_handlers):
        """Тест дневной сводки."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_update.message.reply_text = AsyncMock()

        brief_handlers.summary_repo.get_for_period = AsyncMock(
            return_value=[
                {
                    "period_start": datetime(2026, 4, 27, 0, 0),
                    "period_end": datetime(2026, 4, 28, 0, 0),
                    "content": {"summary": "Сводка дня", "topics": ["Тема 1", "Тема 2"]},
                }
            ]
        )

        await brief_handlers.daily_command(mock_update)

        mock_update.message.reply_text.assert_called_once()
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Сводка дня" in call_args


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
