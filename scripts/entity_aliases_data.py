"""
Данные алиасов сущностей.
Формат: (alias_name, alias_type, canonical_name, canonical_type)
alias_type=None => применяется к любому типу сущности с таким именем.

Редактировать здесь, затем применить:
    python scripts/migrate_entity_aliases.py --seed-only
"""

SEED_ALIASES: list[tuple[str, str | None, str, str]] = [
    # ===== РОССИЯ =====
    ("РФ",                               "location",     "Россия",                               "location"),
    ("Российская Федерация",             "location",     "Россия",                               "location"),
    ("России",                           "location",     "Россия",                               "location"),
    ("России",                           "organization", "Россия",                               "organization"),

    # ===== ЦБ РФ =====
    ("Банк России",                      "organization", "ЦБ РФ",                                "organization"),
    ("ЦБ",                               "organization", "ЦБ РФ",                                "organization"),
    ("ЦБ России",                        "organization", "ЦБ РФ",                                "organization"),
    ("Центробанк",                       "organization", "ЦБ РФ",                                "organization"),
    ("Центральный банк",                 "organization", "ЦБ РФ",                                "organization"),
    ("Центральный банк России",          "organization", "ЦБ РФ",                                "organization"),

    # ===== ПЕРСОНЫ: короткое ↔ полное имя =====
    ("Путин",                            "person",       "Владимир Путин",                       "person"),
    ("Трамп",                            "person",       "Дональд Трамп",                        "person"),
    ("Зеленский",                        "person",       "Владимир Зеленский",                   "person"),
    ("Песков",                           "person",       "Дмитрий Песков",                       "person"),
    ("Набиуллина",                       "person",       "Эльвира Набиуллина",                   "person"),
    ("Мишустин",                         "person",       "Михаил Мишустин",                      "person"),
    ("Силуанов",                         "person",       "Антон Силуанов",                       "person"),
    ("Лавров",                           "person",       "Сергей Лавров",                        "person"),
    ("Медведев",                         "person",       "Дмитрий Медведев",                     "person"),
    ("Байден",                           "person",       "Джо Байден",                           "person"),
    ("Макрон",                           "person",       "Эмманюэль Макрон",                     "person"),
    ("Шольц",                            "person",       "Олаф Шольц",                           "person"),
    ("Костин",                           "person",       "Андрей Костин",                        "person"),

    # ===== ГОСУДАРСТВЕННАЯ ДУМА =====
    ("Госдума",                          "organization", "Государственная дума",                 "organization"),
    ("ГД",                               "organization", "Государственная дума",                 "organization"),
    ("Государственной думы",             "organization", "Государственная дума",                 "organization"),

    # ===== СОВЕТ ФЕДЕРАЦИИ =====
    ("СФ",                               "organization", "Совет Федерации",                      "organization"),

    # ===== ПРАВИТЕЛЬСТВО =====
    ("Правительство РФ",                 "organization", "Правительство России",                 "organization"),
    ("Правительства РФ",                 "organization", "Правительство России",                 "organization"),

    # ===== МИНИСТЕРСТВА =====
    ("Минфин",                           "organization", "Министерство финансов",                "organization"),
    ("Минфин России",                    "organization", "Министерство финансов",                "organization"),
    ("Минфина",                          "organization", "Министерство финансов",                "organization"),
    ("Минобороны",                       "organization", "Министерство обороны",                 "organization"),
    ("Минобороны России",                "organization", "Министерство обороны",                 "organization"),
    ("Минэкономразвития",                "organization", "Министерство экономического развития", "organization"),
    ("МЭР",                              "organization", "Министерство экономического развития", "organization"),

    # ===== ФСБ / МВД / ФНС / ЦИК =====
    ("Федеральная служба безопасности",  "organization", "ФСБ",                                  "organization"),
    ("Министерство внутренних дел",      "organization", "МВД",                                  "organization"),
    ("ФНС",                              "organization", "Федеральная налоговая служба",         "organization"),
    ("ЦИК",                              "organization", "Центральная избирательная комиссия",   "organization"),

    # ===== ВСУ / Армия Украины =====
    ("ВСУ",                              "organization", "Вооружённые силы Украины",             "organization"),
    ("Украина (ВСУ)",                    "organization", "Вооружённые силы Украины",             "organization"),
    ("Украина (ВСУ)",                    "location",     "Вооружённые силы Украины",             "organization"),
    ("Вооруженные силы Украина",         "organization", "Вооружённые силы Украины",             "organization"),
    ("Вооруженные силы Украина (ВСУ)",   "organization", "Вооружённые силы Украины",             "organization"),
    ("Вооружённые силы Украина",         "organization", "Вооружённые силы Украины",             "organization"),
    ("Вооружённые силы Украины",         "organization", "Вооружённые силы Украины",             "organization"),

    # ===== ЕВРОПЕЙСКИЙ СОЮЗ =====
    ("ЕС",                               "organization", "Европейский союз",                     "organization"),
    ("Евросоюз",                         "organization", "Европейский союз",                     "organization"),
    ("Европейский союз (ЕС)",            "organization", "Европейский союз",                     "organization"),
    ("ЕС",                               "location",     "Европейский союз",                     "organization"),
    ("Европейский союз",                 "location",     "Европейский союз",                     "organization"),
    ("Европейский союз (ЕС)",            "location",     "Европейский союз",                     "organization"),
    ("Евросоюз",                         "location",     "Европейский союз",                     "organization"),

    # ===== МОСКОВСКАЯ БИРЖА =====
    ("Мосбиржа",                         "organization", "Московская биржа",                     "organization"),
    ("Мосбиржи",                         "organization", "Московская биржа",                     "organization"),
    ("MOEX",                             "organization", "Московская биржа",                     "organization"),

    # ===== ТИКЕРЫ TINVEST → НАЗВАНИЯ КОМПАНИЙ =====
    # Сбер (+ исправление ошибочного типа location → organization)
    ("SBER",                             "organization", "Сбер",                                 "organization"),
    ("SBER",                             "location",     "Сбер",                                 "organization"),
    ("Сбербанк",                         "organization", "Сбер",                                 "organization"),
    ("Сбербанка",                        "organization", "Сбер",                                 "organization"),
    ("Сбер",                             "location",     "Сбер",                                 "organization"),

    ("VTBR",                             "organization", "ВТБ",                                  "organization"),
    ("GAZP",                             "organization", "Газпром",                              "organization"),
    ("LKOH",                             "organization", "Лукойл",                               "organization"),
    ("GMKN",                             "organization", "Норникель",                            "organization"),
    ("Норильский никель",                "organization", "Норникель",                            "organization"),
    ("NVTK",                             "organization", "Новатэк",                              "organization"),
    ("TATN",                             "organization", "Татнефть",                             "organization"),
    ("ROSN",                             "organization", "Роснефть",                             "organization"),
    ("ALRS",                             "organization", "АЛРОСА",                               "organization"),
    ("YDEX",                             "organization", "Яндекс",                               "organization"),
    ("Yandex",                           "organization", "Яндекс",                               "organization"),
    ("OZON",                             "organization", "Ozon",                                 "organization"),
    ("Ozon",                             "organization", "Ozon",                                 "organization"),
    ("AFKS",                             "organization", "АФК Система",                          "organization"),
    ("АФК «Система»",                    "organization", "АФК Система",                          "organization"),
    ("PHOR",                             "organization", "ФосАгро",                              "organization"),
    ("MGNT",                             "organization", "Магнит",                               "organization"),

    # ===== ГЕОЛОКАЦИИ =====
    ("США",                              "location",     "США",                                  "location"),
    ("Соединённые Штаты",                "location",     "США",                                  "location"),
    ("Соединенные Штаты",                "location",     "США",                                  "location"),
    ("Украины",                          "location",     "Украина",                              "location"),
    ("Москвы",                           "location",     "Москва",                               "location"),
    ("Китая",                            "location",     "Китай",                                "location"),
]
