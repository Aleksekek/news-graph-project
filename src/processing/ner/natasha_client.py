"""
Обёртка над Natasha для извлечения именованных сущностей из русскоязычных новостей.
Модели инициализируются один раз при создании экземпляра.
"""

from natasha import Doc, MorphVocab, NewsEmbedding, NewsMorphTagger, NewsNERTagger, Segmenter

from src.core.models import ExtractedEntity

# Маппинг типов Natasha → наши типы
_TYPE_MAP = {
    "PER": "person",
    "LOC": "location",
    "ORG": "organization",
}

# Доля текста, которая считается «началом» (для importance_score)
_FIRST_PART_RATIO = 0.2


class NatashaClient:
    """NER-клиент на базе Natasha/Slovnet."""

    def __init__(self) -> None:
        emb = NewsEmbedding()
        self._segmenter = Segmenter()
        self._morph_vocab = MorphVocab()
        self._morph_tagger = NewsMorphTagger(emb)
        self._ner_tagger = NewsNERTagger(emb)

    def extract(self, title: str, text: str) -> list[ExtractedEntity]:
        """
        Извлекает сущности из заголовка и текста статьи.
        Заголовок учитывается при расчёте importance_score отдельно.
        """
        title_len = len(title)
        combined = f"{title}\n\n{text}" if text else title

        doc = Doc(combined)
        doc.segment(self._segmenter)
        doc.tag_morph(self._morph_tagger)
        for token in doc.tokens:
            token.lemmatize(self._morph_vocab)
        doc.tag_ner(self._ner_tagger)
        for span in doc.spans:
            span.normalize(self._morph_vocab)

        # Границы предложений для context_snippet
        sents = [(s.start, s.stop, s.text) for s in doc.sents]
        text_threshold = title_len + int(len(text) * _FIRST_PART_RATIO)

        # Группируем упоминания по (normalized_name, type)
        entity_map: dict[tuple[str, str], ExtractedEntity] = {}

        for span in doc.spans:
            if span.type not in _TYPE_MAP:
                continue

            entity_type = _TYPE_MAP[span.type]
            normalized = span.normal or span.text
            key = (normalized, entity_type)

            importance = self._importance(span.start, title_len, text_threshold)
            context = self._context_snippet(span.start, sents)

            if key not in entity_map:
                entity_map[key] = ExtractedEntity(
                    original_name=span.text,
                    normalized_name=normalized,
                    entity_type=entity_type,
                    count=1,
                    importance_score=importance,
                    context_snippet=context,
                )
            else:
                entry = entity_map[key]
                entry.count += 1
                entry.importance_score = max(entry.importance_score, importance)

        return list(entity_map.values())

    @staticmethod
    def _importance(start: int, title_len: int, text_threshold: int) -> float:
        if start < title_len:
            return 1.0
        if start < text_threshold:
            return 0.7
        return 0.3

    @staticmethod
    def _context_snippet(start: int, sents: list[tuple[int, int, str]]) -> str | None:
        for s_start, s_stop, s_text in sents:
            if s_start <= start < s_stop:
                return s_text[:300]
        return None
