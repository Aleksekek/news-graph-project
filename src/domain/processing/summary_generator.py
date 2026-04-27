import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from src.domain.storage.database import ArticleRepository

logger = logging.getLogger(__name__)


class SummaryGenerator:
    """Генератор текстовых сводок из новостей"""

    def __init__(self):
        self.repo = ArticleRepository()

    async def generate_daily_summary(self, days: int = 1) -> Optional[str]:
        """Генерация сводки за N дней"""
        try:
            # Получаем статьи за указанный период (асинхронно)
            articles = await self.repo.get_articles_by_days(days, limit=50)

            if not articles:
                return "📭 За указанный период новостей не найдено."

            # Категоризируем статьи
            categories = self._categorize_articles(articles)

            # Формируем сводку
            summary = f"📰 *Сводка новостей за {days} день(дней)*\n\n"
            summary += f"Всего новостей: *{len(articles)}*\n"
            summary += f"Период: *{(datetime.now() - timedelta(days=days)).strftime('%d.%m')} - {datetime.now().strftime('%d.%m')}*\n\n"

            for category, articles_list in categories.items():
                if articles_list:
                    summary += f"*{category.upper()}*\n"
                    for article in articles_list[:5]:
                        title = article.get("raw_title", "Без заголовка")[:80]
                        published = article.get("published_at")
                        time_str = published.strftime("%H:%M") if published else "--:--"
                        summary += f"• [{time_str}] {title}...\n"
                    summary += "\n"

            summary += "\n📊 Используйте /news [запрос] для поиска конкретных новостей."
            return summary

        except Exception as e:
            logger.error(f"Ошибка генерации сводки: {e}")
            return None

    def _categorize_articles(self, articles: List[Dict]) -> Dict[str, List]:
        """Простая категоризация статей по ключевым словам"""
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
                "госдума",
            ],
            "экономика": [
                "рубль",
                "доллар",
                "нефть",
                "газ",
                "санкции",
                "биржа",
                "акции",
                "инвестиции",
            ],
            "технологии": [
                "ии",
                "искусственный интеллект",
                "робот",
                "соцсети",
                "telegram",
                "нейросеть",
            ],
            "общество": [
                "образование",
                "медицина",
                "культура",
                "спорт",
                "искусство",
                "футбол",
            ],
            "происшествия": [
                "пожар",
                "дтп",
                "наводнение",
                "землетрясение",
                "авария",
                "катастрофа",
            ],
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
