"""
Тесты для Telegram бота (с моками).
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
            MockApp.builder.return_value.token.return_value.build.return_value = (
                mock_app
            )

            bot = NewsTelegramBot(token="test_token_123")
            return bot

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

    @pytest.mark.asyncio
    async def test_build_qa_prompt(self, bot):
        """Построение промпта для вопросов."""
        articles = [
            {"raw_title": "Test Title", "raw_text": "Test content here"},
        ]
        prompt = bot._build_qa_prompt("What's new?", articles)

        assert "What's new?" in prompt
        assert "Test Title" in prompt
        assert "Test content" in prompt

    @pytest.mark.asyncio
    async def test_health_command(self, bot):
        """Тест команды health."""
        mock_update = Mock()
        mock_update.effective_user.id = "123"
        mock_update.message = AsyncMock()

        with patch.object(bot, "_get_total_count", AsyncMock(return_value=100)):
            await bot.health_command(mock_update, Mock())

        mock_update.message.reply_text.assert_called_once()
        call_args = mock_update.message.reply_text.call_args[0][0]
        assert "Активен" in call_args
        assert "100" in call_args
