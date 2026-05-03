#!/bin/bash
# Copyright 2026 Ринат (pg_expecto)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# autovacuum_log_processing.sh
# Скрипт формирования markdown-отчета по работе autovacuum из лога PostgreSQL
#
# version 8.1.1
# updated 03/05/2026


set -euo pipefail

LOG_FILE="postgresql_extract.log"
OUTPUT_FILE="autovacuum_report.md"

if [ ! -f "$LOG_FILE" ]; then
    echo "Ошибка: лог-файл '$LOG_FILE' не найден."
    exit 1
fi

# Устанавливаем локаль UTF-8 (критично для кириллицы)
export LC_ALL=C.UTF-8 2>/dev/null || export LC_ALL=en_US.UTF-8 2>/dev/null || true

echo "🔍 Анализ лог-файла '$LOG_FILE' ..."

awk '
BEGIN {
    duration_sum = 0;
    scans_sum = 0;
    removed_sum = 0;
    remain_sum = 0;
    in_block = 0;
    block = "";
}

function process_block() {
    if (!in_block) return;

    if (block !~ /автоматическая очистка таблицы|automatic vacuum of table/) {
        in_block = 0; block = ""; return;
    }

    # Длительность: секунды → миллисекунды
    duration = 0;
    if (match(block, /прошло:[[:space:]]*([0-9.]+)[[:space:]]*с/)) {
        val = substr(block, RSTART+7, RLENGTH-8);
        gsub(/[^0-9.]/, "", val);
        duration = val * 1000;
    } else if (match(block, /elapsed:[[:space:]]*([0-9.]+)[[:space:]]*s/)) {
        val = substr(block, RSTART+8, RLENGTH-9);
        gsub(/[^0-9.]/, "", val);
        duration = val * 1000;
    }
    duration_sum += duration;

    # Сканирований индекса
    index_scans = 0;
    if (match(block, /сканирований индекса:[[:space:]]*([0-9]+)/)) {
        index_scans = substr(block, RSTART+22, RLENGTH-22) + 0;
    } else if (match(block, /index scans:[[:space:]]*([0-9]+)/)) {
        index_scans = substr(block, RSTART+13, RLENGTH-13) + 0;
    }
    scans_sum += index_scans;

    # Страниц удалено
    pages_removed = 0;
    if (match(block, /страниц удалено:[[:space:]]*([0-9]+)/)) {
        pages_removed = substr(block, RSTART+17, RLENGTH-17) + 0;
    } else if (match(block, /pages:[[:space:]]*([0-9]+)[[:space:]]+removed/)) {
        pages_removed = substr(block, RSTART+7, RLENGTH-15) + 0;
    }
    removed_sum += pages_removed;

    # Осталось страниц
    pages_remain = 0;
    if (match(block, /осталось:[[:space:]]*([0-9]+)/)) {
        pages_remain = substr(block, RSTART+9, RLENGTH-9) + 0;
    } else if (match(block, /removed,[[:space:]]*([0-9]+)[[:space:]]+remain/)) {
        pages_remain = substr(block, RSTART+18, RLENGTH-25) + 0;
    }
    remain_sum += pages_remain;

    in_block = 0; block = "";
}

{
    # Начало нового блока: строка с временной меткой
    if (match($0, /[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+/)) {
        process_block();
        in_block = 1;
        block = $0;
    } else if (in_block) {
        block = block "\n" $0;
    }
}

END {
    process_block();

    # Запись отчета в формате Markdown
    print "| Длительность autovacuum (ms) | Количество сканирований индексов | Удалено страниц | Оставлено страниц |" > "'"$OUTPUT_FILE"'";
    print "|------------------------------|----------------------------------|-----------------|-------------------|" > "'"$OUTPUT_FILE"'";
    printf "| %d | %d | %d | %d |\n", duration_sum, scans_sum, removed_sum, remain_sum > "'"$OUTPUT_FILE"'";
}
' "$LOG_FILE"

echo "✅ Отчет сохранен в $OUTPUT_FILE"