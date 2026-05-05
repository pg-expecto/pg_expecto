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
# temp_files_report.sh
# version 8.1.2
# updated 04/05/2026
#
# Анализ лога PostgreSQL на предмет временных файлов.
# Подсчитывает общее количество временных файлов и их суммарный объём (МБ).
# Результат сохраняется в Markdown-файл temp_files_report.md.
#
# Требует: bash, awk (gawk с поддержкой массивов в match)

LOG_FILE="postgresql_extract.log"
REPORT_FILE="temp_files_report.md"

# Проверка наличия лог-файла
if [[ ! -f "$LOG_FILE" ]]; then
    echo "Ошибка: файл '$LOG_FILE' не найден."
    exit 1
fi

# === АНАЛИЗ ЛОГА ===
# Ищем строки, содержащие 'temporary file:' (англ) или 'временный файл:' (рус).
# Из каждой такой строки извлекаем размер (size / размер) и накапливаем.
read temp_files_count temp_files_size < <(
    awk '
        /temporary file:|временный файл:/ {
            count++
            if (match($0, /(size|размер) ([0-9]+)/, arr)) {
                size_bytes += arr[2]
            }
        }
        END {
            # Если найдены записи, выводим количество и объём в МБ (2 знака после запятой)
            if (count > 0)
                printf "%d %.2f", count, size_bytes / 1048576
            else
                printf "0 0.00"
        }
    ' "$LOG_FILE"
)

# === ФОРМИРОВАНИЕ ОТЧЁТА ===
cat > "$REPORT_FILE" <<EOF
| Количество временных файлов | Объем временных файлов (MB) |
|-------------------------------|------------------------------|
| $temp_files_count | $temp_files_size |
EOF

echo "Готово: отчёт сохранён в '$REPORT_FILE'"