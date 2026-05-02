-- Заполнение данных об источниках
INSERT INTO sources (name, type, external_id, url, meta_info)
VALUES (
  'Тинькофф Пульс (TInvest)',
  'api',
  'tinvest_pulse',
  'https://www.tinvest.ru/pulse/',
  '{"sector": "financial_social", "language": "ru", "note": "Парсер через API/скрапинг TInvest, тикер-ориентированный"}'
)
RETURNING id; -- id 1


INSERT INTO sources (name, type, external_id, url, meta_info)
VALUES (
  'Лента ру',
  'rss+parsing',
  'lenta_ru',
  'https://lenta.ru/',
  '{"sector": "politics", "language": "ru", "note": "Парсер через RSS + скрапинг по ссылкам из RSS"}'
)
RETURNING id; -- id 2

INSERT INTO sources (name, type, external_id, url, meta_info)
VALUES (
  'Интерфакс',
  'rss+parsing',
  'interfax_ru',
  'https://www.interfax.ru/',
  '{"sector": "general", "language": "ru", "note": "Парсер через RSS + скрапинг полного текста со страниц статей. Разделы: main, russia, business, world"}'
)
RETURNING id; -- id 3


INSERT INTO sources (name, type, external_id, url, meta_info)
VALUES (
  'ТАСС',
  'rss+parsing',
  'tass_ru',
  'https://tass.ru/',
  '{"sector": "general", "language": "ru", "note": "Парсер через RSS (tass.ru/rss/v2.xml) + скрапинг полного текста со страниц статей"}'
)
RETURNING id; -- id 4


INSERT INTO sources (name, type, external_id, url, meta_info)
VALUES (
  'РБК',
  'rss',
  'rbc_ru',
  'https://rbc.ru/',
  '{"sector": "business", "language": "ru", "note": "Парсер только через RSS (rssexport.rbc.ru/rbcnews/news/20/full.rss). Полный текст уже в фиде, скрапинг сайта не нужен"}'
)
RETURNING id; -- id 5
