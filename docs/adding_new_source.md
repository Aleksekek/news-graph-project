# Как добавить новый источник новостей

## Шаг 1: Определите тип источника

- **RSS** — просто (feedparser)
- **HTML** — средняя сложность (BeautifulSoup)
- **API** — зависит от API

## Шаг 2: Создайте парсер

```python
# src/parsers/newsource/parser.py

from src.parsers.base import BaseParser, ParseResult, ParserConfig
from src.core.models import ParsedItem

class NewsourceParser(BaseParser):
    def __init__(self, config: ParserConfig):
        super().__init__(config)
        self.base_url = "https://newsouce.com"
    
    async def parse(self, limit: int = 100, **filters) -> ParseResult:
        # 1. Получить список элементов
        # 2. Для каждого: self.to_parsed_item(raw_data)
        # 3. Вернуть ParseResult(items)
        pass
    
    async def parse_period(self, start_date, end_date, limit, **filters) -> ParseResult:
        # Архивный парсинг (опционально)
        pass
    
    def to_parsed_item(self, raw_data: dict) -> ParsedItem:
        return ParsedItem(
            source_id=self.source_id,
            source_name=self.source_name,
            original_id=self._generate_id(raw_data),
            url=raw_data["url"],
            title=raw_data["title"],
            content=raw_data["content"],
            published_at=self._parse_date(raw_data["date"]),
            author=raw_data.get("author"),
            metadata={...}
        )
```

## Шаг 3: Создайте конвертер

```python
# src/parsers/newsource/converter.py

from src.core.models import ArticleForDB, ParsedItem

class NewsourceConverter:
    def convert(self, item: ParsedItem) -> ArticleForDB:
        return ArticleForDB(
            source_id=item.source_id,
            original_id=item.original_id,
            url=item.url,
            raw_title=item.title[:500],
            raw_text=item.content[:10000],
            published_at=item.published_at,
            author=item.author,
            status="raw"
        )
```

## Шаг 4: Зарегистрируйте в фабриках

```python
# src/parsers/factory.py
from src.parsers.newsource.parser import NewsourceParser

_parsers_registry = {
    "lenta": LentaParser,
    "tinvest": TInvestParser,
    "interfax": InterfaxParser,
    "tass": TassParser,
    "rbc": RbcParser,
    "newsource": NewsourceParser,  # добавить
}

_default_configs["newsource"] = {
    "base_url": "https://newsouce.com",
    "request_delay": 1.0,
}
```

```python
# src/parsers/converter_factory.py
from src.parsers.newsource.converter import NewsourceConverter

_converters = {
    "lenta": LentaConverter,
    "tinvest": TInvestConverter,
    "interfax": InterfaxConverter,
    "tass": TassConverter,
    "rbc": RbcConverter,
    "newsource": NewsourceConverter,  # добавить
}
```

## Шаг 5: Добавим в constants.py

```python
# src/core/constants.py
SOURCE_IDS = {
    "tinvest": 1,
    "lenta": 2,
    "interfax": 3,
    "tass": 4,
    "rbc": 5,
    "newsource": 6,  # новый ID
}
```

## Шаг 6: Обновите расписание

Добавьте задачу в [src/config/schedules.py](../src/config/schedules.py) (дефолты) или опционально переопределите через `config/schedule_config.yaml`:

```python
# src/config/schedules.py
default_tasks = {
    ...
    # подберите минуту, не пересекающуюся с другими источниками (текущий шаг — 5 мин)
    "newsource": TaskConfig(
        name="Парсинг NewSource",
        cron="0,30 * * * *",
        enabled=True,
        kwargs={"limit": 30},
    ),
}
```

## Шаг 7: Напишите тесты

```python
# tests/integration/test_newsource_parser.py

import pytest
from src.parsers.factory import ParserFactory

@pytest.mark.asyncio
async def test_newsource_parser():
    parser = ParserFactory.create("newsource")
    async with parser:
        result = await parser.parse(limit=2)
    
    assert len(result.items) <= 2
    for item in result.items:
        assert item.title
        assert item.url.startswith("https://")
```

## Шаг 8: Проверьте

```bash
pytest tests/integration/test_newsource_parser.py -v
```

Готово! Новый источник работает. 🎉