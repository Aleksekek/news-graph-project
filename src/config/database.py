from src.config.settings import settings


class DatabaseConfig:
    """Конфигурация подключения к БД (для обратной совместимости)"""

    @property
    def host(self) -> str:
        return settings.DB_HOST

    @property
    def port(self) -> int:
        return settings.DB_PORT

    @property
    def database(self) -> str:
        return settings.DB_NAME

    @property
    def user(self) -> str:
        return settings.DB_USER

    @property
    def password(self) -> str:
        return settings.DB_PASSWORD

    @property
    def connection_string(self) -> str:
        return settings.database_url

    def as_dict(self) -> dict:
        return settings.database_dict


# Глобальный экземпляр для обратной совместимости
db_config = DatabaseConfig()
