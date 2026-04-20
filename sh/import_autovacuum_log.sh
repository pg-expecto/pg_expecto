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
# version 8.1.1
# Updated 20.04.2026
#
# import_autovacuum_log.sh
#
# ============================================================
# Скрипт импорта событий autovacuum из лога PostgreSQL в БД
# Исправлена обработка многострочных записей и timestamp
# ============================================================

set -euo pipefail

PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-expecto_db}"
PG_USER="${PG_USER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-}"

if [ $# -ne 2 ]; then
    echo "Ошибка: укажите путь к лог-файлу и дату последнего сбора."
    echo "Пример: $0 /var/log/postgresql/postgresql.log '2026-04-19 10:00:00'"
    exit 1
fi

LOG_FILE="$1"
LAST_TS="$2"

if [ ! -f "$LOG_FILE" ]; then
    echo "Ошибка: лог-файл '$LOG_FILE' не найден."
    exit 1
fi

TEMP_CSV=$(mktemp)
trap 'rm -f "$TEMP_CSV"' EXIT

echo "🔍 Поиск записей autovacuum после '$LAST_TS' в файле '$LOG_FILE' ..."

# Устанавливаем локаль UTF-8 (критично для кириллицы)
export LC_ALL=C.UTF-8 2>/dev/null || export LC_ALL=en_US.UTF-8 2>/dev/null || true

awk -v last_ts="$LAST_TS" '
BEGIN {
    gsub(/-/, " ", last_ts);
    gsub(/:/, " ", last_ts);
    last_epoch = mktime(last_ts);
    if (last_epoch == -1) {
        print "Ошибка: неверный формат даты. Используйте \"YYYY-MM-DD HH24:MI:SS\"" > "/dev/stderr";
        exit 1;
    }
    cnt = 0;
    in_block = 0;
    block = "";
    ts_str = "";
    total_blocks = 0;
}

function process_block() {
    if (!in_block) return;
    total_blocks++;

    if (block !~ /автоматическая очистка таблицы|automatic vacuum of table/) {
        in_block = 0; block = ""; return;
    }

    # Извлечение имени таблицы
    table_name_full = "";
    if (match(block, /автоматическая очистка таблицы "([^"]+)"/)) {
        table_name_full = substr(block, RSTART+30, RLENGTH-31);
    } else if (match(block, /automatic vacuum of table "([^"]+)"/)) {
        table_name_full = substr(block, RSTART+26, RLENGTH-27);
    } else {
        in_block = 0; block = ""; return;
    }

    split(table_name_full, parts, ".");
    db   = parts[1];
    schema = (parts[2] ? parts[2] : "public");
    table = parts[3];
    if (!table) {
        table = parts[2];
        schema = "public";
    }

    # Длительность (секунды → миллисекунды)
    duration = "";
    if (match(block, /прошло:[[:space:]]*([0-9.]+)[[:space:]]*с/)) {
        duration = substr(block, RSTART+7, RLENGTH-8);
        gsub(/[^0-9.]/, "", duration);
        duration = sprintf("%.0f", duration * 1000);
    } else if (match(block, /elapsed:[[:space:]]*([0-9.]+)[[:space:]]*s/)) {
        duration = substr(block, RSTART+8, RLENGTH-9);
        gsub(/[^0-9.]/, "", duration);
        duration = sprintf("%.0f", duration * 1000);
    }

    # Сканирования индекса
    index_scans = "";
    if (match(block, /сканирований индекса:[[:space:]]*([0-9]+)/)) {
        index_scans = substr(block, RSTART+22, RLENGTH-22);
    } else if (match(block, /index scans:[[:space:]]*([0-9]+)/)) {
        index_scans = substr(block, RSTART+13, RLENGTH-13);
    }

    # Страниц удалено
    pages_removed = "";
    if (match(block, /страниц удалено:[[:space:]]*([0-9]+)/)) {
        pages_removed = substr(block, RSTART+17, RLENGTH-17);
    } else if (match(block, /pages:[[:space:]]*([0-9]+)[[:space:]]+removed/)) {
        pages_removed = substr(block, RSTART+7, RLENGTH-15);
    }

    # Осталось страниц
    pages_remain = "";
    if (match(block, /осталось:[[:space:]]*([0-9]+)/)) {
        pages_remain = substr(block, RSTART+9, RLENGTH-9);
    } else if (match(block, /removed,[[:space:]]*([0-9]+)[[:space:]]+remain/)) {
        pages_remain = substr(block, RSTART+18, RLENGTH-25);
    }

    # Экранирование кавычек
    gsub(/"/, "\"\"", ts_str);
    gsub(/"/, "\"\"", db);
    gsub(/"/, "\"\"", schema);
    gsub(/"/, "\"\"", table);

    printf "%s,%s,%s,%s,%s,%s,%s,%s\n",
        ts_str, db, schema, table,
        (duration != "" ? duration : ""),
        (index_scans != "" ? index_scans : ""),
        (pages_removed != "" ? pages_removed : ""),
        (pages_remain != "" ? pages_remain : "");
    cnt++;

    in_block = 0; block = "";
}

{
    # Ищем временную метку в начале строки (без якоря ^ для совместимости)
    if (match($0, /[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+/)) {
        process_block();

        ts_full = substr($0, RSTART, RLENGTH);
        gsub(/\.[0-9]+.*/, "", ts_full);   # удаляем миллисекунды и зону
        ts_str = ts_full;

        # Преобразуем для сравнения эпох
        ts_candidate = ts_str;
        gsub(/-/, " ", ts_candidate);
        gsub(/:/, " ", ts_candidate);
        epoch = mktime(ts_candidate);
        if (epoch == -1) next;

        if (epoch <= last_epoch) next;

        in_block = 1;
        block = $0;
    } else if (in_block) {
        block = block "\n" $0;
    }
}

END {
    process_block();
    # Вывод диагностики (количество обработанных блоков) перед счётчиком
    printf "Всего обработано блоков: %d\n", total_blocks > "/dev/stderr";
    print cnt > "/dev/stderr";
}
' "$LOG_FILE" 2> "$TEMP_CSV.cnt" > "$TEMP_CSV"

# Читаем последнюю строку как количество записей
RECORD_COUNT=$(tail -n1 "$TEMP_CSV.cnt")
# Печатаем диагностические сообщения (если есть)
head -n -1 "$TEMP_CSV.cnt" 2>/dev/null || true

if [ -z "$RECORD_COUNT" ] || [ "$RECORD_COUNT" -eq 0 ] 2>/dev/null; then
    echo "✅ Новых событий autovacuum не найдено."
    exit 0
fi

echo "📋 Найдено строк для импорта: $RECORD_COUNT"

echo "💾 Загрузка в таблицу autovacuum_log_events ..."
psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DB" -U "$PG_USER" -q -c "\COPY autovacuum_log_events (
    curr_timestamp,
    database_name,
    schema_name,
    table_name,
    duration_ms,
    index_scans,
    pages_removed,
    pages_remain
) FROM '$TEMP_CSV' DELIMITER ',' CSV NULL '';"

if [ $? -eq 0 ]; then
    echo "✅ Успешно импортировано $RECORD_COUNT записей."
else
    echo "❌ Ошибка при вставке данных."
    exit 1
fi