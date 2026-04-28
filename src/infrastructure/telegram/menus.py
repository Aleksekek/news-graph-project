"""
Построение меню и клавиатур для Telegram бота.
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def get_main_menu() -> InlineKeyboardMarkup:
    """Главное меню."""
    keyboard = [
        [InlineKeyboardButton("📰 Сводки", callback_data="menu_summaries")],
        [InlineKeyboardButton("🔍 Поиск", callback_data="menu_search")],
        [InlineKeyboardButton("🤖 Задать вопрос", callback_data="menu_ask")],
        [InlineKeyboardButton("📊 Статистика", callback_data="menu_stats")],
        [InlineKeyboardButton("🔔 Подписка", callback_data="menu_subscribe")],
        [InlineKeyboardButton("⚙️ Помощь", callback_data="menu_help")],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_summaries_menu() -> InlineKeyboardMarkup:
    """Меню сводок."""
    keyboard = [
        [InlineKeyboardButton("🕐 За 6 часов", callback_data="brief_6h")],
        [InlineKeyboardButton("📅 За вчера", callback_data="summary_yesterday")],
        [InlineKeyboardButton("✏️ Свой диапазон (в часах)", callback_data="brief_custom")],
        [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_search_menu() -> InlineKeyboardMarkup:
    """Меню поиска."""
    keyboard = [
        [InlineKeyboardButton("💰 Нефть", callback_data="search_popular:нефть")],
        [InlineKeyboardButton("💵 Рубль", callback_data="search_popular:рубль")],
        [InlineKeyboardButton("🏛 Путин", callback_data="search_popular:Путин")],
        [InlineKeyboardButton("🤖 ИИ", callback_data="search_popular:ИИ")],
        [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_stats_menu() -> InlineKeyboardMarkup:
    """Меню статистики."""
    keyboard = [
        [InlineKeyboardButton("📈 Общая статистика", callback_data="stats_overall")],
        [InlineKeyboardButton("🕐 Почасовая активность", callback_data="stats_hourly")],
        [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(keyboard)


def get_back_button(callback_data: str = "main_menu") -> InlineKeyboardMarkup:
    """Кнопка "Назад"."""
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=callback_data)]])


def get_subscribe_menu(is_subscribed: bool) -> InlineKeyboardMarkup:
    """Меню подписки."""
    if is_subscribed:
        keyboard = [[InlineKeyboardButton("❌ Отписаться", callback_data="subscribe_unsubscribe")]]
    else:
        keyboard = [[InlineKeyboardButton("✅ Подписаться", callback_data="subscribe_daily")]]
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="menu_subscribe")])
    return InlineKeyboardMarkup(keyboard)
