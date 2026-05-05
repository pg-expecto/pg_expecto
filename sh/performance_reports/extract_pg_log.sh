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
# extract_pg_log.sh
#
# extract_pg_log.sh — извлечение фрагмента лога PostgreSQL по диапазону времени
# Использование: ./extract_pg_log.sh <начало> <конец>
#   <начало> и <конец> в формате "YYYY-MM-DD HH:MI"
#   Логи читаются из /log/pg_log
# Результат: файл postgresql_extract.log в текущей папке
#
# version 8.1.2 – поиск файла по дате в первой строке лога

set -euo pipefail

LOG_DIR="/log/pg_log"

if [ $# -ne 2 ]; then
    echo "Использование: $0 <START_TIME> <END_TIME>" >&2
    exit 1
fi

START_TIME="$1"
END_TIME="$2"

# Проверка формата времени
if ! [[ "$START_TIME" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9]{2}:[0-9]{2}$ ]]; then
    echo "Ошибка: начальное время должно быть в формате YYYY-MM-DD HH:MM" >&2
    exit 1
fi
if ! [[ "$END_TIME" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9]{2}:[0-9]{2}$ ]]; then
    echo "Ошибка: конечное время должно быть в формате YYYY-MM-DD HH:MM" >&2
    exit 1
fi

START_DATE="${START_TIME:0:10}"
END_DATE="${END_TIME:0:10}"
if [ "$START_DATE" != "$END_DATE" ]; then
    echo "Ошибка: начальное и конечное время должны быть в один день" >&2
    exit 1
fi
DATE_STR="$START_DATE"

# Поиск файла лога: перебираем все .log файлы, проверяем первую строку
LOGFILE=""
for f in "$LOG_DIR"/*.log; do
    [ -f "$f" ] || continue
    # Читаем первую строку и проверяем, начинается ли она с нужной даты
    first_line=$(head -1 "$f" 2>/dev/null)
    if [[ "$first_line" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2} ]]; then
        file_date="${first_line:0:10}"
        if [ "$file_date" = "$DATE_STR" ]; then
            LOGFILE="$f"
            break
        fi
    fi
done

if [ -z "$LOGFILE" ]; then
    echo "Ошибка: не найден .log файл с датой $DATE_STR в первой строке ($LOG_DIR)" >&2
    exit 1
fi

echo "Найден файл лога: $LOGFILE"

# Извлечение строк с учётом многострочных сообщений
awk -v start="$START_TIME" -v end="$END_TIME" '
function is_ts_line() {
    return ($1 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/ && $2 ~ /^[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+$/)
}
{
    if (is_ts_line()) {
        ts_min = $1 " " substr($2, 1, 5)
        print_flag = (ts_min >= start && ts_min <= end) ? 1 : 0
    }
    if (print_flag) print
}' "$LOGFILE" > ./postgresql_extract.log

echo "Извлечение завершено. Результат сохранён в ./postgresql_extract.log"
