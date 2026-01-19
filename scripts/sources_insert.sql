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