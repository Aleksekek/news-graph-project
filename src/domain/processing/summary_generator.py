import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from src.domain.storage.database import ArticleRepository
from src.utils.telegram_helpers import safe_markdown_text

logger = logging.getLogger(__name__)


class SummaryGenerator:
    """Генератор текстовых сводок из новостей с ссылками"""

    def __init__(self):
        self.repo = ArticleRepository()

    async def get_articles_with_links(
        self, days: int = 1, limit: int = 30
    ) -> List[Dict]:
        """Получить статьи с URL за N дней"""
        try:
            # Используем существующий метод search_articles_with_links
            cutoff_date = datetime.now() - timedelta(days=days)
            # Нужно добавить фильтр по дате в существующий метод или создать новый
            articles = await self.repo.get_articles_by_days_with_links(days, limit)
            return articles
        except Exception as e:
            logger.error(f"Ошибка получения статей: {e}")
            return []

    def format_summary_with_links(self, articles: List[Dict], days: int) -> str:
        """Форматирует сводку с ссылками на статьи"""
        if not articles:
            return "📭 За указанный период новостей не найдено."

        # Категоризируем
        categorized = self._categorize_articles_with_links(articles)

        summary = f"📰 *Сводка новостей за {days} день(дней)*\n\n"
        summary += f"Всего новостей: *{len(articles)}*\n"
        summary += f"Период: *{(datetime.now() - timedelta(days=days)).strftime('%d.%m')} - {datetime.now().strftime('%d.%m')}*\n\n"

        for category, articles_list in categorized.items():
            if articles_list:
                # Категория жирным
                summary += f"*{category.upper()}*\n"

                for article in articles_list[:7]:
                    title = safe_markdown_text(
                        article.get("raw_title", "Без заголовка")[:70]
                    )
                    published = article.get("published_at")
                    time_str = published.strftime("%H:%M") if published else "--:--"
                    url = article.get("url", "")

                    summary += f"• [{time_str}] {title}...\n"
                    if url:
                        summary += f"  🔗 {url}\n"

                summary += "\n"

        summary += "\n📊 Используйте /news [запрос] для поиска конкретных новостей."
        return summary

    def _categorize_articles_with_links(self, articles: List[Dict]) -> Dict[str, List]:
        """Категоризация статей с сохранением ссылок"""
        categories = {
            "политика": [],
            "экономика": [],
            "технологии": [],
            "общество": [],
            "происшествия": [],
            "другое": [],
        }

        keywords = {
            "политика": [
                "путин",
                "правительство",
                "выборы",
                "минобороны",
                "мвд",
                "кремль",
            ],
            "экономика": [
                "рубль",
                "доллар",
                "нефть",
                "газ",
                "санкции",
                "биржа",
                "акции",
            ],
            "технологии": ["ии", "искусственный интеллект", "робот", "нейросеть", "ai"],
            "общество": ["образование", "медицина", "культура", "спорт"],
            "происшествия": ["пожар", "дтп", "наводнение", "авария"],
        }

        for article in articles:
            title = article.get("raw_title", "")
            text = article.get("raw_text", "")
            content = f"{title} {text}".lower()

            found_category = "другое"
            for category, cat_keywords in keywords.items():
                if any(keyword in content for keyword in cat_keywords):
                    found_category = category
                    break

            categories[found_category].append(article)

        return categories

    async def generate_daily_summary(self, days: int = 1) -> Optional[str]:
        """Legacy метод для обратной совместимости"""
        articles = await self.get_articles_with_links(days, limit=50)
        return self.format_summary_with_links(articles, days)
