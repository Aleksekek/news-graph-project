"""
LLM-очистка сущностей: находит алиасы и исправляет типы через DeepSeek.

Стратегия батчинга:
  Reference set (топ-50 каждого типа по частоте) обрабатывается скользящим
  окном — чтобы найти кросс-именные дубли (ЦБ РФ = Банк России).
  Хвост обрабатывается батчами по 80 с reference set в контексте каждого запроса.

Рабочий процесс:
  1. Запустить (вызывает LLM, сохраняет результат в JSON):
       python scripts/llm_entity_cleanup.py
  2. Проверить результат в файле llm_cleanup_YYYYMMDD_HHMMSS.json
  3. Применить к БД:
       python scripts/llm_entity_cleanup.py --apply llm_cleanup_*.json

Опции:
  --dry-run              Показать состав батчей без вызова LLM
  --limit N              Ограничить кол-во обрабатываемых сущностей
  --min-count N          Минимальное кол-во статей (default: 3)
  --max-batches N        Максимальное количество батчей для обработки
  --apply <file>         Применить результат из JSON-файла к entity_aliases
  --delete-discards <file>  Удалить мусорные сущности и заблокировать их через entity_aliases
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

from src.app.entity_cleanup import (
    BATCH_SIZE,
    CHUNK_SIZE,
    ENTITY_TYPES,
    MAX_CONCURRENT,
    MIN_COUNT_DEFAULT,
    REFERENCE_WINDOW_STEP,
    TAIL_WINDOW_STEP,
    _make_client,
    apply_aliases_to_db,
    build_prompt,
    call_llm,
    fetch_entities,
    sliding_window_batches,
    sort_tail_by_last_token,
    split_reference_and_tail,
)
from src.database.pool import DatabasePoolManager
from src.utils.logging import setup_logging

logger = setup_logging()


# ─── apply / delete-discards (работают с JSON-файлом) ─────────────────────────


async def apply_results(result_file: Path) -> None:
    data = json.loads(result_file.read_text(encoding="utf-8"))
    raw_aliases: list[dict] = data.get("aliases", [])
    type_fixes:  list[dict] = data.get("type_fixes", [])
    discards:    list[dict] = data.get("discard", [])

    applied = await apply_aliases_to_db(raw_aliases, type_fixes)
    logger.info(f"Применено: {applied} записей в entity_aliases")
    logger.info(
        "Следующий шаг: python scripts/merge_entity_aliases.py "
        "(или --dry-run для предварительного просмотра)"
    )
    if discards:
        logger.info(
            f"Мусор ({len(discards)} сущностей) — не удаляем автоматически, проверьте вручную:"
        )
        for d in discards[:20]:
            logger.info(f"  {d.get('name')!r} ({d.get('type')})")
        if len(discards) > 20:
            logger.info(f"  ... и ещё {len(discards) - 20}")


async def delete_discards(result_file: Path) -> None:
    """
    Удаляет мусорные сущности из entities/article_entities и добавляет их
    в entity_aliases с canonical_type='discard', чтобы они больше не создавались.
    """
    data = json.loads(result_file.read_text(encoding="utf-8"))
    discards: list[dict] = data.get("discard", [])
    if not discards:
        logger.info("Нет сущностей для удаления")
        return

    _upsert_discard = """
    WITH upd AS (
        UPDATE entity_aliases
           SET canonical_name = $3,
               canonical_type = $4
         WHERE alias_name = $1
           AND alias_type IS NOT DISTINCT FROM $2
         RETURNING id
    )
    INSERT INTO entity_aliases (alias_name, alias_type, canonical_name, canonical_type)
    SELECT $1, $2, $3, $4
    WHERE NOT EXISTS (SELECT 1 FROM upd);
    """

    async with DatabasePoolManager.connection() as conn:
        deleted = 0
        discard_params: list[tuple] = []

        for d in discards:
            name        = d.get("name", "")
            entity_type = d.get("type", "")
            if not name or not entity_type:
                continue

            row = await conn.fetchrow(
                "SELECT id FROM entities WHERE normalized_name = $1 AND type = $2",
                name, entity_type,
            )
            if row:
                entity_id = row["id"]
                await conn.execute("DELETE FROM article_entities WHERE entity_id = $1", entity_id)
                await conn.execute("DELETE FROM entities WHERE id = $1", entity_id)
                deleted += 1

            discard_params.append((name, entity_type, name, "discard"))

        await conn.executemany(_upsert_discard, discard_params)

    logger.info(
        f"Удалено сущностей из DB: {deleted}; "
        f"добавлено discard-алиасов: {len(discard_params)} "
        f"(новые статьи с ними будут автоматически отфильтрованы)"
    )


# ─── Генерация с сохранением в JSON-файл ──────────────────────────────────────


async def run_generate(
    min_count: int, limit: int | None, dry_run: bool, max_batches: int | None
) -> None:
    entities = await fetch_entities(min_count=min_count, limit=limit)
    logger.info(f"Загружено {len(entities)} сущностей (count >= {min_count})")

    reference, tail = split_reference_and_tail(entities)
    ref_batches  = sliding_window_batches(reference, BATCH_SIZE, REFERENCE_WINDOW_STEP)
    tail_batches = sliding_window_batches(sort_tail_by_last_token(tail), BATCH_SIZE, TAIL_WINDOW_STEP)

    if max_batches is not None:
        ref_batches  = ref_batches[:max_batches]
        tail_batches = tail_batches[:max(0, max_batches - len(ref_batches))]

    logger.info(
        f"Reference set: {len(reference)} | Tail: {len(tail)} | "
        f"Батчей: ref={len(ref_batches)}, tail={len(tail_batches)}"
        + (f" (ограничено --max-batches {max_batches})" if max_batches else "")
    )

    if dry_run:
        logger.info("[dry-run] Состав батчей:")
        for i, b in enumerate(ref_batches):
            types = ", ".join(f"{t}:{sum(1 for e in b if e['type']==t)}" for t in ENTITY_TYPES)
            logger.info(f"  Ref batch {i+1}: {len(b)} сущностей  [{types}]")
        for i, b in enumerate(tail_batches[:5]):
            first = b[0]["name"] if b else "?"
            last  = b[-1]["name"] if b else "?"
            logger.info(f"  Tail batch {i+1}: {len(b)} сущностей  [{first!r} … {last!r}]")
        if len(tail_batches) > 5:
            logger.info(f"  ... ещё {len(tail_batches) - 5} tail-батчей")
        logger.info("[dry-run] Запустите без --dry-run чтобы отправить запросы к LLM")
        return

    client = _make_client()
    model  = "deepseek-v4-pro"
    sem    = asyncio.Semaphore(MAX_CONCURRENT)

    all_aliases:    list[dict] = []
    all_type_fixes: list[dict] = []
    all_discards:   list[dict] = []
    total_tokens = 0
    errors = 0

    all_batches: list[tuple[list[dict], list[dict] | None]] = (
        [(b, None) for b in ref_batches] + [(b, reference) for b in tail_batches]
    )
    total = len(all_batches)

    partial_path = Path(__file__).parent / "llm_cleanup_partial.json"

    def _save_partial(done: int) -> None:
        partial = {
            "generated_at":  datetime.now().isoformat(),
            "total_entities": len(entities),
            "total_tokens":   total_tokens,
            "errors":         errors,
            "batches_done":   done,
            "batches_total":  total,
            "aliases":        all_aliases,
            "type_fixes":     all_type_fixes,
            "discard":        all_discards,
        }
        partial_path.write_text(json.dumps(partial, ensure_ascii=False, indent=2), encoding="utf-8")

    for chunk_start in range(0, total, CHUNK_SIZE):
        chunk = all_batches[chunk_start : chunk_start + CHUNK_SIZE]
        tasks = [
            call_llm(client, sem, model, build_prompt(batch, ref))
            for batch, ref in chunk
        ]
        chunk_results = await asyncio.gather(*tasks)

        chunk_ok = 0
        for result, tokens in chunk_results:
            total_tokens += tokens
            if result:
                all_aliases.extend(result.get("aliases", []))
                all_type_fixes.extend(result.get("type_fixes", []))
                all_discards.extend(result.get("discard", []))
                chunk_ok += 1
            else:
                errors += 1

        done = chunk_start + len(chunk)
        logger.info(f"[{done}/{total}] Чанк завершён: {chunk_ok}/{len(chunk)} успешно")
        _save_partial(done)

    # Дедупликация по ключу перед сохранением (get() защищает от нестандартных ключей LLM)
    dedup_aliases = list({(a["alias_name"], a["alias_type"]): a for a in all_aliases}.values())
    dedup_fixes   = list({(f["name"], f["current_type"]): f for f in all_type_fixes}.values())
    dedup_discards = list(
        {(d.get("name", ""), d.get("type", "")): d for d in all_discards if d.get("name")}.values()
    )

    output = {
        "generated_at":   datetime.now().isoformat(),
        "total_entities": len(entities),
        "total_tokens":   total_tokens,
        "errors":         errors,
        "aliases":        dedup_aliases,
        "type_fixes":     dedup_fixes,
        "discard":        dedup_discards,
    }

    out_path = Path(__file__).parent / f"llm_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    if partial_path.exists():
        partial_path.unlink()

    logger.info(
        f"Готово: алиасов={len(dedup_aliases)}, "
        f"исправлений типов={len(dedup_fixes)}, "
        f"мусора={len(dedup_discards)}, "
        f"токенов={total_tokens}, "
        f"ошибок={errors}"
    )
    logger.info(f"Результат: {out_path}")
    logger.info(f"Применить: python scripts/llm_entity_cleanup.py --apply {out_path.name}")


# ─── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM-очистка сущностей")
    parser.add_argument("--dry-run",  action="store_true", help="Показать состав батчей без вызова LLM")
    parser.add_argument("--limit",    type=int, default=None, help="Ограничить кол-во обрабатываемых сущностей")
    parser.add_argument(
        "--min-count",
        type=int,
        default=MIN_COUNT_DEFAULT,
        help=f"Мин. кол-во статей (default: {MIN_COUNT_DEFAULT})",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        metavar="N",
        help="Обработать не более N батчей (для тестового прогона)",
    )
    parser.add_argument(
        "--apply",
        type=Path,
        default=None,
        metavar="FILE",
        help="Применить результат из JSON-файла к entity_aliases",
    )
    parser.add_argument(
        "--delete-discards",
        type=Path,
        default=None,
        metavar="FILE",
        help="Удалить мусорные сущности из DB и заблокировать через entity_aliases",
    )
    args = parser.parse_args()

    if args.apply:
        if not args.apply.exists():
            logger.error(f"Файл не найден: {args.apply}")
            sys.exit(1)
        asyncio.run(apply_results(args.apply))
    elif args.delete_discards:
        if not args.delete_discards.exists():
            logger.error(f"Файл не найден: {args.delete_discards}")
            sys.exit(1)
        asyncio.run(delete_discards(args.delete_discards))
    else:
        asyncio.run(
            run_generate(
                min_count=args.min_count,
                limit=args.limit,
                dry_run=args.dry_run,
                max_batches=args.max_batches,
            )
        )


if __name__ == "__main__":
    main()
