# Вместо монолитного manage.py - модульные команды
import click


@click.group()
def cli():
    """News Graph Project CLI"""
    pass


@cli.command()
@click.option("--limit", default=100, help="Limit articles")
@click.option("--source", required=True, help="Source name (lenta/tinvest)")
def parse(source: str, limit: int):
    """Parse articles from source"""
    from src.application.use_cases.parse_source import ParseSourceUseCase

    use_case = ParseSourceUseCase()
    use_case.execute(source=source, limit=limit)


@cli.command()
@click.option("--start", required=True, help="Start date YYYY-MM-DD")
@click.option("--end", required=True, help="End date YYYY-MM-DD")
def archive(start: str, end: str):
    """Parse archive"""
    # ...


# Остальные команды...# Остальные команды...
