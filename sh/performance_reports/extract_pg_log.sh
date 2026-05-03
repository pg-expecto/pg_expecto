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
# Скрипт для извлечения фрагмента лога PostgreSQL по диапазону времени
# Использование: ./extract_pg_log.sh <начало> <конец>
#   <начало> и <конец> в формате "YYYY-MM-DD HH:MI"
#   Времена должны быть внутри одних суток
#   Логи читаются из /log/pg_log
# Результат: файл postgresql_extract.log в текущей папке
#
# version 8.1.1
# updated 03/05/2026


set -euo pipefail

# Константа: директория с логами PostgreSQL
LOG_DIR="/log/pg_log"

# Проверка аргументов
if [ $# -ne 2 ]; then
    echo "Использование: $0 <START_TIME> <END_TIME>" >&2
    exit 1
fi

START_TIME="$1"
END_TIME="$2"

# Проверка формата времени (YYYY-MM-DD HH:MM)
if ! [[ "$START_TIME" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9]{2}:[0-9]{2}$ ]]; then
    echo "Ошибка: начальное время должно быть в формате YYYY-MM-DD HH:MM" >&2
    exit 1
fi
if ! [[ "$END_TIME" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9]{2}:[0-9]{2}$ ]]; then
    echo "Ошибка: конечное время должно быть в формате YYYY-MM-DD HH:MM" >&2
    exit 1
fi

# Дата для обеих меток должна совпадать
START_DATE="${START_TIME:0:10}"
END_DATE="${END_TIME:0:10}"
if [ "$START_DATE" != "$END_DATE" ]; then
    echo "Ошибка: начальное и конечное время должны быть в один день" >&2
    exit 1
fi
DATE_STR="$START_DATE"

# Вычисляем следующий день для поиска по дате модификации
NEXT_DATE=$(date -d "$DATE_STR + 1 day" +%Y-%m-%d 2>/dev/null)
if [ -z "$NEXT_DATE" ]; then
    # Попытка для BSD/macOS
    NEXT_DATE=$(date -j -v+1d -f "%Y-%m-%d" "$DATE_STR" +%Y-%m-%d 2>/dev/null)
fi
if [ -z "$NEXT_DATE" ]; then
    echo "Ошибка: не удалось вычислить следующую дату" >&2
    exit 1
fi

# Поиск файла, изменённого в течение нужного дня (с 00:00:00 до 23:59:59)
LOGFILE=$(find "$LOG_DIR" -maxdepth 1 -type f \
    -newermt "$DATE_STR" ! -newermt "$NEXT_DATE" \
    -print -quit)
if [ -z "$LOGFILE" ]; then
    echo "Ошибка: не найден файл лога за $DATE_STR в $LOG_DIR" >&2
    exit 1
fi
echo "Найден файл лога: $LOGFILE"

# Извлечение строк с учётом многострочных сообщений
awk -v start="$START_TIME" -v end="$END_TIME" '
function is_ts_line() {
    # Строка начинается с даты и времени (YYYY-MM-DD HH:MM:SS.sss)
    return ($1 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/ && $2 ~ /^[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+$/)
}
{
    if (is_ts_line()) {
        # Ключ сравнения: дата + часы:минуты
        ts_min = $1 " " substr($2, 1, 5)
        # Флаг: строка попадает в интервал?
        print_flag = (ts_min >= start && ts_min <= end) ? 1 : 0
    }
    if (print_flag) {
        print
    }
}' "$LOGFILE" > ./postgresql_extract.log

echo "Извлечение завершено. Результат сохранён в ./postgresql_extract.log"