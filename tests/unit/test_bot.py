"""
Тесты для Telegram бота.
"""

from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.infrastructure.telegram.bot import NewsTelegramBot


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

    # ==================== ТЕСТЫ ХЕЛПЕРОВ ====================

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

    # ==================== ТЕСТЫ ПОСТРОЕНИЯ PROMPT ====================

    def test_build_qa_prompt(self, bot):
        """Построение промпта для вопросов."""
        articles = [
            {"raw_title": "Test Title 1", "raw_text": "Test content 1 here"},
            {"raw_title": "Test Title 2", "raw_text": "Test content 2 here"},
        ]
        prompt = bot._build_qa_prompt("What's new?", articles)

        assert "What's new?" in prompt
        assert "Test Title 1" in prompt
        assert "Test content 1" in prompt
        assert "Test Title 2" in prompt

    def test_build_qa_prompt_empty_articles(self, bot):
        """Построение промпта с пустыми статьями."""
        prompt = bot._build_qa_prompt("Test question?", [])

        assert "Test question?" in prompt
        assert "СВЕЖИЕ НОВОСТИ" in prompt

    # ==================== ТЕСТЫ КОМАНД ====================

    @pytest.mark.asyncio
    async def test_health_command(self, bot):
        """Тест команды health."""
        mock_update = Mock()
        mock_update.effective_user.id = "123"
        mock_update.message = AsyncMock()

        bot.subscribers = {123: {}}

        await bot.health_command(mock_update, Mock())

        mock_update.message.reply_text.assert_called_once()
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Активен" in call_args

    @pytest.mark.asyncio
    async def test_start_command(self, bot):
        """Тест команды start."""
        mock_update = Mock()
        mock_update.effective_user = Mock()
        mock_update.effective_user.first_name = "TestUser"
        mock_update.message = AsyncMock()

        await bot.start(mock_update, Mock())

        mock_update.message.reply_text.assert_called_once()
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "TestUser" in call_args
        assert "Привет" in call_args

    @pytest.mark.asyncio
    async def test_help_command(self, bot):
        """Тест команды help."""
        mock_update = Mock()
        mock_update.message = AsyncMock()

        await bot.help_command(mock_update, Mock())

        mock_update.message.reply_text.assert_called_once()
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Список команд" in call_args

    @pytest.mark.asyncio
    async def test_subscribe_command_new(self, bot):
        """Тест подписки нового пользователя."""
        mock_update = Mock()
        mock_update.effective_chat = Mock()
        mock_update.effective_chat.id = 12345
        mock_update.message = AsyncMock()

        bot.subscribers = {}

        await bot.subscribe_command(mock_update, Mock())

        assert 12345 in bot.subscribers
        mock_update.message.reply_text.assert_called_once()
        assert "подписались" in mock_update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_subscribe_command_already_subscribed(self, bot):
        """Тест подписки уже подписанного пользователя."""
        mock_update = Mock()
        mock_update.effective_chat = Mock()
        mock_update.effective_chat.id = 12345
        mock_update.message = AsyncMock()

        bot.subscribers = {12345: {}}

        await bot.subscribe_command(mock_update, Mock())

        mock_update.message.reply_text.assert_called_once()
        assert "уже подписаны" in mock_update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_unsubscribe_command(self, bot):
        """Тест отписки."""
        mock_update = Mock()
        mock_update.effective_chat = Mock()
        mock_update.effective_chat.id = 12345
        mock_update.message = AsyncMock()

        bot.subscribers = {12345: {}}

        await bot.unsubscribe_command(mock_update, Mock())

        assert 12345 not in bot.subscribers
        mock_update.message.reply_text.assert_called_once()
        assert "отписались" in mock_update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_unsubscribe_command_not_subscribed(self, bot):
        """Тест отписки не подписанного пользователя."""
        mock_update = Mock()
        mock_update.effective_chat = Mock()
        mock_update.effective_chat.id = 12345
        mock_update.message = AsyncMock()

        bot.subscribers = {}

        await bot.unsubscribe_command(mock_update, Mock())

        assert "не были подписаны" in mock_update.message.reply_text.call_args[0][0]

    @pytest.mark.asyncio
    async def test_brief_command_success(self, bot):
        """Тест команды brief с успешным ответом."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_update.message.reply_text = AsyncMock()
        mock_update.args = ["6"]  # Правильный способ передать args

        bot.summary_repo.get_for_period = AsyncMock(
            return_value=[
                {
                    "period_start": datetime(2026, 4, 27, 14, 0),
                    "content": {"summary": "Test summary 1"},
                },
                {
                    "period_start": datetime(2026, 4, 27, 15, 0),
                    "content": {"summary": "Test summary 2"},
                },
            ]
        )

        # Создаём context с args
        mock_context = Mock()
        mock_context.args = ["6"]

        await bot.brief_command(mock_update, mock_context)

        bot.summary_repo.get_for_period.assert_called_once()
        mock_update.message.reply_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_brief_command_no_data(self, bot):
        """Тест команды brief без данных."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_update.args = []

        mock_context = Mock()
        mock_context.args = []

        bot.summary_repo.get_for_period = AsyncMock(return_value=[])

        await bot.brief_command(mock_update, mock_context)

        mock_update.message.reply_text.assert_called_once()
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Нет суммаризаций" in call_args

    @pytest.mark.asyncio
    async def test_daily_command_success(self, bot):
        """Тест команды daily с успешным ответом."""
        mock_update = Mock()
        mock_update.message = AsyncMock()

        bot.summary_repo.get_for_period = AsyncMock(
            return_value=[
                {
                    "period_start": datetime(2026, 4, 26, 0, 0),
                    "content": {
                        "topics": ["Экономика", "Политика"],
                        "summary": "Тестовая сводка",
                        "trend": "Восходящий тренд",
                    },
                }
            ]
        )

        await bot.daily_command(mock_update, None)

        bot.summary_repo.get_for_period.assert_called_once()
        mock_update.message.reply_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_daily_command_no_data(self, bot):
        """Тест команды daily без данных."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_update.message.reply_text = AsyncMock()
        # Убираем атрибут edit_message_text, чтобы бот пошёл по ветке else
        if hasattr(mock_update, "edit_message_text"):
            del mock_update.edit_message_text

        bot.summary_repo.get_for_period = AsyncMock(return_value=[])

        await bot.daily_command(mock_update, None)

        mock_update.message.reply_text.assert_called_once()
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "ещё не готова" in call_args

    @pytest.mark.asyncio
    async def test_stats_command_success(self, bot):
        """Тест команды stats с успешным ответом."""
        mock_update = Mock()
        mock_update.message = AsyncMock()

        bot.article_repo.get_stats = AsyncMock(
            return_value={"total": 1000, "raw": 100, "processed": 900}
        )
        bot.article_repo.get_sources_stats = AsyncMock(
            return_value=[("lenta", 600), ("tinvest", 400)]
        )

        await bot.stats_command(mock_update, Mock())

        mock_update.message.reply_text.assert_called_once()
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "1000" in call_args
        assert "lenta" in call_args

    @pytest.mark.asyncio
    async def test_ask_command_with_args(self, bot):
        """Тест команды ask с аргументами."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_update.args = ["Что", "происходит", "с", "рублём?"]

        mock_context = Mock()
        mock_context.args = ["Что", "происходит", "с", "рублём?"]

        bot.article_repo.get_unprocessed = AsyncMock(
            return_value=[{"raw_title": "Test", "raw_text": "Content"}]
        )
        bot.llm.raw_request = AsyncMock(return_value="Тестовый ответ")

        # Создаём мок для status_msg
        mock_status = AsyncMock()
        mock_update.message.reply_text = AsyncMock(return_value=mock_status)
        mock_status.delete = AsyncMock()

        await bot.ask_command(mock_update, mock_context)

        bot.article_repo.get_unprocessed.assert_called_once()
        bot.llm.raw_request.assert_called_once()

    @pytest.mark.asyncio
    async def test_ask_command_no_args(self, bot):
        """Тест команды ask без аргументов."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_update.args = []

        mock_context = Mock()
        mock_context.args = []

        await bot.ask_command(mock_update, mock_context)

        mock_update.message.reply_text.assert_called_once()
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Укажите вопрос" in call_args

    @pytest.mark.asyncio
    async def test_news_command_with_args(self, bot):
        """Тест команды news с аргументами."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_update.args = ["криптовалюта"]

        mock_context = Mock()
        mock_context.args = ["криптовалюта"]

        bot.article_repo.search = AsyncMock(
            return_value=[
                {
                    "raw_title": "Bitcoin растёт",
                    "published_at": datetime.now(),
                    "url": "https://test.com/1",
                }
            ]
        )

        await bot.news_command(mock_update, mock_context)

        bot.article_repo.search.assert_called_once_with("криптовалюта", limit=10, with_urls=True)
        mock_update.message.reply_text.assert_called()

    @pytest.mark.asyncio
    async def test_news_command_no_args(self, bot):
        """Тест команды news без аргументов."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_update.args = []

        mock_context = Mock()
        mock_context.args = []

        await bot.news_command(mock_update, mock_context)

        mock_update.message.reply_text.assert_called_once()
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Укажите поисковый запрос" in call_args

    @pytest.mark.asyncio
    async def test_news_command_no_results(self, bot):
        """Тест команды news без результатов."""
        mock_update = Mock()
        mock_update.message = AsyncMock()
        mock_update.args = ["nothing"]

        mock_context = Mock()
        mock_context.args = ["nothing"]

        bot.article_repo.search = AsyncMock(return_value=[])

        await bot.news_command(mock_update, mock_context)

        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "не найдено" in call_args

    # ==================== ТЕСТЫ МЕНЮ ====================

    @pytest.mark.asyncio
    async def test_main_menu(self, bot):
        """Тест главного меню."""
        mock_query = Mock()
        mock_query.edit_message_text = AsyncMock()
        mock_query.answer = AsyncMock()

        await bot.main_menu(mock_query)

        mock_query.edit_message_text.assert_called_once()
        mock_query.answer.assert_called_once()

    @pytest.mark.asyncio
    async def test_show_summaries_menu(self, bot):
        """Тест меню сводок."""
        mock_query = Mock()
        mock_query.edit_message_text = AsyncMock()
        mock_query.answer = AsyncMock()

        await bot.show_summaries_menu(mock_query)

        mock_query.edit_message_text.assert_called_once()
        mock_query.answer.assert_called_once()

    @pytest.mark.asyncio
    async def test_show_search_menu(self, bot):
        """Тест меню поиска."""
        mock_query = Mock()
        mock_query.edit_message_text = AsyncMock()
        mock_query.answer = AsyncMock()

        await bot.show_search_menu(mock_query)

        mock_query.edit_message_text.assert_called_once()
        mock_query.answer.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribe_from_menu(self, bot):
        """Тест подписки из меню."""
        mock_query = Mock()
        mock_query.message.chat.id = 12345
        mock_query.edit_message_text = AsyncMock()
        mock_query.answer = AsyncMock()

        bot.subscribers = {}

        await bot.subscribe_from_menu(mock_query)

        assert 12345 in bot.subscribers
        mock_query.answer.assert_called_once()

    @pytest.mark.asyncio
    async def test_unsubscribe_from_menu(self, bot):
        """Тест отписки из меню."""
        mock_query = Mock()
        mock_query.message.chat.id = 12345
        mock_query.edit_message_text = AsyncMock()
        mock_query.answer = AsyncMock()

        bot.subscribers = {12345: {}}

        await bot.unsubscribe_from_menu(mock_query)

        assert 12345 not in bot.subscribers
        mock_query.answer.assert_called_once()
