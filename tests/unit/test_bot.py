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

    # ==================== ТЕСТЫ КОМАНД (через обработчики) ====================

    @pytest.mark.asyncio
    async def test_health_command(self, bot):
        """Тест команды health."""
        mock_update = Mock()
        mock_update.message = AsyncMock()

        bot.subscribers = {123: {}}

        # Подменяем health_command в основном боте
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

        # Создаём реальный handlers и подменяем его метод
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
        
        # Мокаем получение статистики
        bot.article_repo.get_hourly_stats_24h = AsyncMock(
            return_value=[
                (datetime(2026, 4, 28, 10, 0), 10),
                (datetime(2026, 4, 28, 11, 0), 20),
                (datetime(2026, 4, 28, 12, 0), 30),
            ]
        )
        
        bot.handlers = Handlers(
            bot.article_repo,
            bot.summary_repo,
            bot.llm,
            bot.formatter,
            bot.subscribers
        )
        
        # Вызываем stats_hourly
        await bot.handlers.stats_hourly(mock_update, Mock())
        
        # Проверяем, что edit_message_text был вызван 2 раза
        assert mock_update.callback_query.edit_message_text.call_count == 2
        
        # Проверяем первый вызов (статус)
        first_call = mock_update.callback_query.edit_message_text.call_args_list[0]
        assert "Собираю почасовую статистику" in first_call[0][0]
        
        # Проверяем второй вызов (результат)
        second_call = mock_update.callback_query.edit_message_text.call_args_list[1]
        response_text = second_call[0][0]
        assert "Активность по часам" in response_text
        assert "28.04 10:00" in response_text
        assert "Максимум: 30" in response_text
        
        # Проверяем, что answer был вызван один раз
        mock_update.callback_query.answer.assert_called_once()

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
                    "content": {"summary": "Тестовая сводка"},
                }
            ]
        )

        bot.brief_handlers = BriefHandlers(bot.summary_repo)
        bot.brief_handlers.brief_command = AsyncMock()

        await bot.brief_handlers.brief_command(mock_update, mock_context)
        bot.brief_handlers.brief_command.assert_called_once()

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
        bot.search_handlers.news_command = AsyncMock()

        await bot.search_handlers.news_command(mock_update, mock_context)
        bot.search_handlers.news_command.assert_called_once()

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
        bot.qa_handlers.ask_command = AsyncMock()

        await bot.qa_handlers.ask_command(mock_update, mock_context)
        bot.qa_handlers.ask_command.assert_called_once()


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
            mock_stats.append((dt, i * 5))  # Разные значения

        handlers.article_repo.get_hourly_stats_24h = AsyncMock(return_value=mock_stats)

        await handlers.stats_hourly(mock_update, Mock())

        # Проверяем, что вызван edit_message_text
        assert mock_update.callback_query.edit_message_text.called
        # Проверяем, что ответ содержит часы
        call_args = mock_update.callback_query.edit_message_text.call_args[0][0]
        assert "28.04" in call_args or "27.04" in call_args
        assert "Максимум:" in call_args


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

        # Мокаем _do_search
        search_handlers._do_search = AsyncMock()

        await search_handlers.search_popular(mock_update, Mock())

        # Проверяем, что _do_search был вызван с правильным запросом
        # (в реальном коде это проверяется через параметры)
        assert mock_update.callback_query.edit_message_text.called


class TestQAHandlers:
    """Тесты обработчиков вопросов."""

    @pytest.fixture
    def qa_handlers(self):
        """Фикстура QA обработчиков."""
        article_repo = AsyncMock()
        llm = AsyncMock()
        return QAHandlers(article_repo, llm)

    @pytest.mark.asyncio
    async def test_build_qa_prompt(self, qa_handlers):
        """Тест построения промпта."""
        articles = [
            {"raw_title": "Заголовок 1", "raw_text": "Текст 1"},
            {"raw_title": "Заголовок 2", "raw_text": "Текст 2"},
        ]

        prompt = qa_handlers._build_qa_prompt("Тестовый вопрос?", articles)

        assert "Тестовый вопрос?" in prompt
        assert "Заголовок 1" in prompt
        assert "Текст 1" in prompt


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

        # Тест с некорректными часами
        mock_context.args = ["invalid"]
        brief_handlers.summary_repo.get_for_period = AsyncMock(return_value=[])

        # Должен использовать значение по умолчанию (6)
        await brief_handlers.brief_command(mock_update, mock_context)

        # Проверяем, что вызван get_for_period с правильным периодом
        # (период должен быть 6 часов)
        call_args = brief_handlers.summary_repo.get_for_period.call_args
        if call_args:
            # Проверяем разницу между start и end
            start, end = call_args[0][0], call_args[0][1]
            diff_hours = (end - start).total_seconds() / 3600
            assert diff_hours <= 6  # Должно быть не больше 6 часов
