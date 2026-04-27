```text
news_graph_project/
├── .env.example                   
├── .gitignore                     
├── docker-compose.yml             
├── Dockerfile                     
├── pyproject.toml                 # Конфигурация
├── requirements.txt               # Зависимости
├── README.md                      
├── Makefile                       
├── test.py                        # Тесты
├── test_db_connections.py         # Тесты подключения к БД
├── test_full_integration.py       # Полные интеграционные тесты
│
├── .github/                       # Github Actions
│   ├── workflows/                 # Список воркфлоу
│   │   └── deploy.yml             # Основной воркфлоу сборки и деплоя
│
├── src/                           # Все исходники здесь
│   │
│   ├── main.py                    # Точка входа
│   │
│   ├── core/                      # Ядро - модели, константы, исключения
│   │   ├── models.py              # Все Pydantic/датаклассы вместе
│   │   ├── schemas.py             # Схемы валидации
│   │   ├── exceptions.py          # Все исключения проекта
│   │   └── constants.py           # Константы (SOURCE_IDS, и т.д.)
│   │
│   ├── config/                    # Конфигурация
│   │   ├── settings.py            # Основные настройки
│   │   ├── database.py            # Конфигурация БД
│   │   └── schedules.py           # Конфигурация расписания
│   │
│   ├── domain/                    # Бизнес-логика
│   │   │
│   │   ├── parsing/               # Вся логика парсинга
│   │   │   ├── base.py            # Базовые классы (BaseParser, BaseProcessor)
│   │   │   ├── factory.py         # Сборщик парсеров
│   │   │   ├── parsers/           # Конкретные парсеры
│   │   │   │   ├── lenta.py       # LentaParser + LentaArchiveParser
│   │   │   │   └── tinvest.py     # PulseParser
│   │   │   │
│   │   │   └── processors/        # Процессоры/адаптеры
│   │   │       ├── base.py        # BaseProcessor
│   │   │       ├── factory.py     # Сборщик процессоров
│   │   │       ├── lenta.py       # LentaProcessor
│   │   │       └── tinvest.py     # TInvestParserAdapter → TInvestProcessor
│   │   │
│   │   ├── processing/            # Логика обработки скачанных постов
│   │   │   ├── nlp_worker.py      # Базовая nlp обработка
│   │   │   └── summary_generator.py # Базовый суммаризатор для бота
│   │   │
│   │   ├── storage/               # Работа с хранилищами
│   │   │   ├── database.py        # DatabaseWriter → ArticleRepository
│   │   │   └── models.py          # SQLAlchemy модели
│   │   │ 
│   │   └── scheduling/            # Планирование задач
│   │       ├── scheduler.py       # TaskScheduler
│   │       └── runner.py          # scheduler_runner.py
│   │
│   ├── application/               # Оркестрация (use cases)
│   │   ├── use_cases/             # Сценарии использования
│   │   │   ├── parse_source.py    # Сценарий парсинга источника
│   │   │   └── process_articles.py # Сценарий обработки статей
│   │   │
│   │   └── cli/                   # CLI команды
│   │       ├── commands.py        # Все команды
│   │       └── utils.py           # Вспомогательные функции для CLI
│   │
│   ├── infrastructure/            # Внешние взаимодействия
│   │   ├── http/                  # HTTP клиенты
│   │   │   ├── client.py          # Базовый HTTP клиент
│   │   │   └── session.py         # Сессии с retry логикой
│   │   │
│   │   └── telegram/              # Телеграм бот
│   │       └── bot.py             # Основной бот
│   │
│   └── utils/                     # Вспомогательные утилиты
│       ├── logging.py             # Конфиг логирования
│       ├── data.py                # Общие data utils
│       ├── retry.py               # Логика повторных попыток
│       └── telegram_helpers.py    # Вспомогательные функции для обработки markdown
│
└── scripts/                       # Скрипты запуска
    ├── run_parser.py              # Универсальный скрипт запуска
    └── nlp_worker.py              
```