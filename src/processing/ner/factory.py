"""
Фабрика NER-клиентов. Возвращает NatashaClient (sync) или LLMNERClient (async)
в зависимости от settings.NER_ENGINE.

Use case:
    from src.processing.ner.factory import create_ner_client
    ner = create_ner_client()
    result = ner.extract(title, text)
    if asyncio.iscoroutine(result):  # LLM-клиент async
        entities = await result
    else:                            # Natasha sync
        entities = result
"""

from src.config.settings import settings
from src.utils.logging import get_logger

logger = get_logger("ner.factory")


def create_ner_client():
    """Создаёт NER-клиент согласно settings.NER_ENGINE.

    Returns:
        NatashaClient (sync .extract) или LLMNERClient (async .extract).

    Raises:
        ValueError: при неизвестном значении NER_ENGINE.
        RuntimeError: если для LLM не задан DEEPSEEK_API_KEY.
    """
    engine = (settings.NER_ENGINE or "natasha").strip().lower()

    if engine == "llm":
        from src.processing.ner.llm_client import LLMNERClient

        logger.info("NER engine: LLM (DeepSeek)")
        return LLMNERClient()

    if engine == "natasha":
        from src.processing.ner.natasha_client import NatashaClient

        logger.info("NER engine: Natasha (local)")
        return NatashaClient()

    raise ValueError(
        f"Unknown NER_ENGINE: {engine!r}. Use 'natasha' or 'llm'."
    )
