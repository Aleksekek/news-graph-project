"""
Тесты для src/processing/ner/natasha_client.py

Используем реальную Natasha (без моков) — модели быстрые (~1с инит),
фикстура module-scoped чтобы инициализировать один раз на весь модуль.
"""

import pytest

from src.processing.ner.natasha_client import NatashaClient


@pytest.fixture(scope="module")
def client() -> NatashaClient:
    """Единственный экземпляр NatashaClient на все тесты модуля."""
    return NatashaClient()


class TestNatashaClientExtraction:
    def test_extracts_person(self, client):
        entities = client.extract("Байден посетил Киев", "Президент США Джо Байден прибыл в Киев.")
        names = [e.normalized_name for e in entities]
        assert any("Байден" in n for n in names)

    def test_extracts_organization(self, client):
        entities = client.extract(
            "Сбербанк снизил ставку",
            "Сбербанк объявил о снижении ставки по вкладам.",
        )
        names = [e.normalized_name for e in entities]
        assert any("Сбербанк" in n for n in names)

    def test_extracts_location(self, client):
        entities = client.extract(
            "Новости из Москвы",
            "В Москве прошёл саммит лидеров государств.",
        )
        names = [e.normalized_name for e in entities]
        assert any("Москва" in n for n in names)

    def test_empty_text_returns_empty_list(self, client):
        entities = client.extract("", "")
        assert entities == []

    def test_entity_types_are_valid(self, client):
        entities = client.extract(
            "Путин встретился с Грефом в Кремле",
            "Президент России Владимир Путин встретился с главой Сбербанка Германом Грефом в Кремле.",
        )
        valid_types = {"person", "organization", "location"}
        for e in entities:
            assert e.entity_type in valid_types


class TestNatashaClientNormalization:
    def test_normalizes_grammatical_cases(self, client):
        """Разные падежи одной сущности → одна запись с суммарным count."""
        entities = client.extract(
            "Заседание Государственной думы",
            "Государственная дума приняла закон. "
            "Депутаты Государственной думы проголосовали. "
            "В стенах Государственной думы прошли дебаты.",
        )
        duma_entities = [e for e in entities if "Государственная дума" in e.normalized_name]
        assert len(duma_entities) == 1
        # Дума встречается и в заголовке, и в тексте 3 раза → count >= 3
        assert duma_entities[0].count >= 3

    def test_normalized_name_is_nominative(self, client):
        """normalized_name должен быть в именительном падеже."""
        entities = client.extract(
            "Встреча с Байденом",
            "Переговоры с президентом США Байденом прошли успешно.",
        )
        biden = next((e for e in entities if "Байден" in e.normalized_name), None)
        assert biden is not None
        # В именительном падеже окончание не -ом/-ым/-ем
        assert not biden.normalized_name.endswith("ом")
        assert not biden.normalized_name.endswith("ым")


class TestNatashaClientImportanceScore:
    def test_title_entity_gets_max_importance(self, client):
        """Сущность в заголовке → importance_score = 1.0"""
        entities = client.extract(
            "Сбербанк отчитался о прибыли",
            "Банк показал рекордные результаты за квартал.",
        )
        sber = next((e for e in entities if "Сбербанк" in e.normalized_name), None)
        assert sber is not None
        assert sber.importance_score == 1.0

    def test_body_entity_gets_lower_importance(self, client):
        """Сущность только в конце текста → importance_score < 1.0"""
        long_padding = "Экономическая ситуация остаётся стабильной. " * 10
        entities = client.extract(
            "Обзор рынка",
            long_padding + "Аналитики Газпромбанка дали прогноз.",
        )
        gpb = next((e for e in entities if "Газпромбанк" in e.normalized_name), None)
        assert gpb is not None
        assert gpb.importance_score < 1.0

    def test_importance_takes_max_across_mentions(self, client):
        """Сущность из заголовка получает importance=1.0, даже если в тексте ниже."""
        # Организации в заголовке Natasha определяет надёжнее, чем фамилии без имени
        entities = client.extract(
            "Сбербанк и ВТБ снизили ставки",
            "Аналитики прокомментировали решение. "
            "По мнению экспертов, Сбербанк и ВТБ реагируют на рыночную ситуацию. "
            "Ожидается, что другие банки последуют примеру Сбербанка.",
        )
        sber = next((e for e in entities if e.normalized_name == "Сбербанк"), None)
        assert sber is not None
        # Сбербанк в заголовке → importance = 1.0
        assert sber.importance_score == 1.0
        # И в тексте тоже встречается
        assert sber.count >= 2


class TestNatashaClientEdgeCases:
    def test_coreference_creates_separate_entities(self, client):
        """'Путин' и 'Владимир Путин' — два разных ключа (известное ограничение)."""
        entities = client.extract(
            "Путин встретился с Байденом",
            "Президент России Владимир Путин провёл переговоры. "
            "Путин выразил готовность к диалогу.",
        )
        names = [e.normalized_name for e in entities]
        # Обе формы присутствуют как отдельные сущности
        has_short = any(n == "Путин" for n in names)
        has_full = any("Владимир" in n for n in names)
        assert has_short and has_full, (
            "Кореференция ожидаемо создаёт две записи — это известное ограничение Natasha"
        )

    def test_context_snippet_not_none_for_found_entity(self, client):
        """context_snippet должен быть заполнен для найденных сущностей."""
        entities = client.extract(
            "Новости экономики",
            "Центральный банк России повысил ключевую ставку до 18 процентов.",
        )
        for e in entities:
            assert e.context_snippet is not None
            assert len(e.context_snippet) > 0

    def test_count_multiple_mentions(self, client):
        """Несколько упоминаний одной сущности → count > 1."""
        entities = client.extract(
            "Сбербанк и ВТБ",
            "Сбербанк снизил ставки. ВТБ также снизил ставки. "
            "Сбербанк и ВТБ лидируют на рынке. Сбербанк отчитался о прибыли.",
        )
        sber = next((e for e in entities if e.normalized_name == "Сбербанк"), None)
        assert sber is not None
        assert sber.count >= 3
