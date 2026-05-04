#!/usr/bin/env python3
"""
Healthcheck для контейнерных сервисов: проверяет, что сервис недавно писал в лог.

Зачем: restart: unless-stopped реагирует только на смерть процесса. Зависший
event loop, deadlock в asyncpg, бесконечный retry в LLM — невидимы для Docker
без healthcheck. Этот скрипт ловит "процесс жив, но сервис ничего не делает".

ENV:
    SERVICE_NAME           - имя сервиса (parser|summarizer|ner|bot). Дефолт: "app"
    LOG_DIR                - папка с логами (default "/app/logs")
    HEALTHCHECK_MAX_AGE    - макс. возраст последней записи в логе (сек), default 3600

Логика:
    - Файл /app/logs/<SERVICE_NAME>.log должен существовать
    - Mtime файла должен быть моложе HEALTHCHECK_MAX_AGE
    - Иначе exit 1 (Docker помечает контейнер unhealthy → restart-policy его перезапустит)

No external deps — только stdlib.
"""

import os
import sys
import time


def main() -> int:
    service = os.environ.get("SERVICE_NAME", "app")
    log_dir = os.environ.get("LOG_DIR", "/app/logs")
    max_age = int(os.environ.get("HEALTHCHECK_MAX_AGE", "3600"))

    logfile = os.path.join(log_dir, f"{service}.log")

    if not os.path.exists(logfile):
        print(f"FAIL: log file not found: {logfile}", file=sys.stderr)
        return 1

    try:
        mtime = os.path.getmtime(logfile)
    except OSError as e:
        print(f"FAIL: cannot stat {logfile}: {e}", file=sys.stderr)
        return 1

    age = time.time() - mtime
    if age > max_age:
        print(f"FAIL: log stale {age:.0f}s > {max_age}s", file=sys.stderr)
        return 1

    print(f"OK ({service}, last log {age:.0f}s ago)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
