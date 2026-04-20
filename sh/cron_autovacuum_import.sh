#!/bin/bash
# Copyright 2026 Ринат (pg_expecto)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# cron_autovacuum_import.sh
# */1 * * * * /postgres/pg_expecto/sh/cron_autovacuum_import.sh /log/pg_log > /postgres/pg_expecto/sh/cron_autovacuum_import.log
# version 8.1
# Updated 19.04.2026
#
# ============================================================
# Скрипт для ежеминутного импорта событий autovacuum из лога
# Использование: ./cron_autovacuum_import.sh <путь_к_папке_логов>
# Пример: ./cron_autovacuum_import.sh /log/pg_log
# ============================================================

set -euo pipefail

# --- Параметры подключения к сервисной БД (expecto_db) ---
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-expecto_db}"
PG_USER="${PG_USER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-}"

# Путь к основному скрипту импорта (можно указать абсолютный)
IMPORT_SCRIPT="$(dirname "$0")/import_autovacuum_log.sh"

# ------------------------------------------------------------
# Проверка аргументов
if [ $# -ne 1 ]; then
    echo "Ошибка: укажите путь к папке с логами PostgreSQL."
    echo "Пример: $0 /log/pg_log"
    exit 1
fi

LOG_DIR="$1"

if [ ! -d "$LOG_DIR" ]; then
    echo "Ошибка: директория '$LOG_DIR' не существует."
    exit 1
fi

if [ ! -f "$IMPORT_SCRIPT" ] || [ ! -x "$IMPORT_SCRIPT" ]; then
    echo "Ошибка: скрипт импорта '$IMPORT_SCRIPT' не найден или не исполняемый."
    exit 1
fi

# ------------------------------------------------------------
# 1. Определение актуального лог-файла PostgreSQL
CURRENT_LOG=""
if [ -L "$LOG_DIR/postgresql.log" ]; then
    CURRENT_LOG=$(readlink -f "$LOG_DIR/postgresql.log")
elif [ -f "$LOG_DIR/postgresql.log" ]; then
    CURRENT_LOG="$LOG_DIR/postgresql.log"
else
    # Поиск по маске *.postgresql-*.log (учитывает ваш формат)
    CURRENT_LOG=$(find "$LOG_DIR" -maxdepth 1 -type f -name "*.postgresql-*.log" -printf "%T@ %p\n" 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-)
    if [ -z "$CURRENT_LOG" ]; then
        # Если не нашли по специфичной маске, берём самый свежий .log файл
        CURRENT_LOG=$(find "$LOG_DIR" -maxdepth 1 -type f -name "*.log" -printf "%T@ %p\n" 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-)
    fi
fi

if [ -z "$CURRENT_LOG" ] || [ ! -f "$CURRENT_LOG" ]; then
    echo "Ошибка: не удалось найти лог-файл PostgreSQL в '$LOG_DIR'."
    exit 1
fi

echo "📁 Актуальный лог-файл: $CURRENT_LOG"

# ------------------------------------------------------------
# 2. Получение времени последнего импорта из таблицы autovacuum_log_events
LAST_TS=$(psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DB" -U "$PG_USER" -t -A -c \
    "SELECT COALESCE(to_char(MAX(curr_timestamp), 'YYYY-MM-DD HH24:MI:SS'), '1970-01-01 00:00:00') FROM autovacuum_log_events;" 2>/dev/null)

if [ -z "$LAST_TS" ]; then
    echo "⚠️  Не удалось получить время последнего импорта, используется 1970-01-01."
    LAST_TS="1970-01-01 00:00:00"
fi

echo "⏱️  Время последнего импорта: $LAST_TS"

# ------------------------------------------------------------
# 3. Запуск основного скрипта импорта
echo "🚀 Запуск импорта..."
"$IMPORT_SCRIPT" "$CURRENT_LOG" "$LAST_TS"

exit $?