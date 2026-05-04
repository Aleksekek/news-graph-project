"""
Тесты LLMNERClient. Мокируем AsyncOpenAI — без реальных DeepSeek-вызовов.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture(autouse=True)
def deepseek_key(monkeypatch):
    """Подменяем ключ в settings (а не в env) — соответствует production-flow,
    где код берёт ключ через src.config.settings, а не через os.getenv."""
    from src.config.settings import settings
    monkeypatch.setattr(settings, "DEEPSEEK_API_KEY", "test-key")


@pytest.fixture
def llm_client():
    from src.processing.ner.llm_client import LLMNERClient

    client = LLMNERClient()
    return client


def _make_response(content: str):
    """Эмулирует структуру response от AsyncOpenAI."""
    msg = MagicMock()
    msg.content = content
    choice = MagicMock()
    choice.message = msg
    response = MagicMock()
    response.choices = [choice]
    return response


class TestParseResponse:
    """Парсер JSON-ответа DeepSeek."""

    def test_parses_basic_response(self, llm_client):
        content = '''[
            {"mention": "Путин", "canonical": "Владимир Путин", "type": "person", "importance": "subject"},
            {"mention": "Москва", "canonical": "Москва", "type": "location", "importance": "mention"}
        ]'''
        entities = llm_client._parse_response(content, "Путин в Москве")
        assert len(entities) == 2
        assert entities[0].normalized_name == "Владимир Путин"
        assert entities[0].entity_type == "person"
        assert entities[0].importance_score == 1.0  # subject
        assert entities[1].importance_score == 0.3  # mention

    def test_strips_markdown_fence(self, llm_client):
        content = '```json\n[{"mention": "X", "canonical": "X", "type": "person", "importance": "key"}]\n```'
        entities = llm_client._parse_response(content, "X")
        assert len(entities) == 1
        assert entities[0].importance_score == 0.7  # key

    def test_skips_invalid_type(self, llm_client):
        content = '[{"mention": "X", "canonical": "X", "type": "weird_type", "importance": "key"}]'
        entities = llm_client._parse_response(content, "X")
        assert entities == []

    def test_skips_missing_canonical(self, llm_client):
        content = '[{"mention": "X", "type": "person", "importance": "key"}]'
        entities = llm_client._parse_response(content, "X")
        assert entities == []

    def test_invalid_importance_falls_back_to_mention(self, llm_client):
        content = '[{"mention": "X", "canonical": "X", "type": "person", "importance": "bogus"}]'
        entities = llm_client._parse_response(content, "X")
        assert len(entities) == 1
        assert entities[0].importance_score == 0.3  # mention

    def test_missing_importance_defaults_to_mention(self, llm_client):
        content = '[{"mention": "X", "canonical": "X", "type": "person"}]'
        entities = llm_client._parse_response(content, "X")
        assert entities[0].importance_score == 0.3

    def test_missing_mention_defaults_to_canonical(self, llm_client):
        content = '[{"canonical": "Y", "type": "person", "importance": "subject"}]'
        entities = llm_client._parse_response(content, "Y appears here Y")
        assert len(entities) == 1
        assert entities[0].original_name == "Y"
        assert entities[0].count == 2  # дважды в тексте

    def test_dedups_same_canonical_type(self, llm_client):
        content = '''[
            {"mention": "Путин", "canonical": "Владимир Путин", "type": "person", "importance": "key"},
            {"mention": "Путина", "canonical": "Владимир Путин", "type": "person", "importance": "subject"}
        ]'''
        entities = llm_client._parse_response(content, "Путин и Путина")
        assert len(entities) == 1  # дубль свернут

    def test_count_reflects_text_occurrences(self, llm_client):
        content = '[{"mention": "Москва", "canonical": "Москва", "type": "location", "importance": "key"}]'
        entities = llm_client._parse_response(content, "Москва, Москва, Москва")
        assert entities[0].count == 3

    def test_context_snippet_extracted(self, llm_client):
        text = "Раз два три. Президент сегодня встретился. Ещё какой-то текст."
        content = '[{"mention": "Президент", "canonical": "Президент", "type": "organization", "importance": "key"}]'
        entities = llm_client._parse_response(content, text)
        assert entities[0].context_snippet is not None
        assert "Президент" in entities[0].context_snippet

    def test_empty_array(self, llm_client):
        entities = llm_client._parse_response("[]", "пустой текст")
        assert entities == []

    def test_invalid_json_returns_empty(self, llm_client):
        entities = llm_client._parse_response("not valid json", "any text")
        assert entities == []

    def test_non_array_response_returns_empty(self, llm_client):
        entities = llm_client._parse_response('{"key": "value"}', "any text")
        assert entities == []

    def test_event_type_supported(self, llm_client):
        content = '[{"mention": "ПМЭФ", "canonical": "ПМЭФ", "type": "event", "importance": "subject"}]'
        entities = llm_client._parse_response(content, "На ПМЭФ выступил...")
        assert len(entities) == 1
        assert entities[0].entity_type == "event"


class TestExtract:
    """End-to-end вызов extract() с моком HTTP."""

    @pytest.mark.asyncio
    async def test_extract_calls_deepseek_and_parses(self, llm_client):
        mock_response = _make_response(
            '[{"mention": "Сбербанк", "canonical": "Сбербанк", "type": "organization", "importance": "subject"}]'
        )
        llm_client.client.chat.completions.create = AsyncMock(return_value=mock_response)

        entities = await llm_client.extract("Заголовок", "Сбербанк объявил о ...")

        assert len(entities) == 1
        assert entities[0].normalized_name == "Сбербанк"
        llm_client.client.chat.completions.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_extract_empty_text_skips_call(self, llm_client):
        llm_client.client.chat.completions.create = AsyncMock()
        entities = await llm_client.extract("", "")
        assert entities == []
        llm_client.client.chat.completions.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_extract_returns_empty_on_api_error(self, llm_client):
        llm_client.client.chat.completions.create = AsyncMock(side_effect=Exception("network"))
        entities = await llm_client.extract("Title", "Body")
        assert entities == []

    @pytest.mark.asyncio
    async def test_extract_truncates_long_text(self, llm_client):
        from src.processing.ner.llm_client import _MAX_TEXT_CHARS

        very_long = "А" * (_MAX_TEXT_CHARS + 5000)
        mock_response = _make_response("[]")
        llm_client.client.chat.completions.create = AsyncMock(return_value=mock_response)

        await llm_client.extract("", very_long)

        sent_prompt = llm_client.client.chat.completions.create.call_args.kwargs["messages"][0][
            "content"
        ]
        # Промпт включает шаблон + текст; проверяем, что не превысил max+промпт-overhead
        assert len(sent_prompt) <= _MAX_TEXT_CHARS + 5000


class TestFactory:
    """Фабрика создаёт клиента согласно settings.NER_ENGINE."""

    def test_factory_returns_natasha_by_default(self, monkeypatch):
        # Дефолт настроек = "natasha"
        from src.processing.ner.factory import create_ner_client
        from src.processing.ner.natasha_client import NatashaClient

        client = create_ner_client()
        assert isinstance(client, NatashaClient)

    def test_factory_returns_llm_when_configured(self, monkeypatch):
        from src.config.settings import settings
        monkeypatch.setattr(settings, "NER_ENGINE", "llm")
        from src.processing.ner.factory import create_ner_client
        from src.processing.ner.llm_client import LLMNERClient

        client = create_ner_client()
        assert isinstance(client, LLMNERClient)

    def test_factory_raises_on_unknown_engine(self, monkeypatch):
        from src.config.settings import settings
        monkeypatch.setattr(settings, "NER_ENGINE", "fasttext")
        from src.processing.ner.factory import create_ner_client

        with pytest.raises(ValueError, match="Unknown NER_ENGINE"):
            create_ner_client()
