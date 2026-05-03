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
# extract_errors.sh
# Анализ лога PostgreSQL: подсчёт ошибок по справочнику кодов,
# вывод только строк с ненулевым количеством ошибок и кодом не 00000.
#
# version 8.1.1
# updated 03/05/2026


# Константы
LOG_FILE="postgresql_extract.log"          # лог-файл
CODES_FILE="A.1.txt"                       # справочник кодов ошибок
REPORT_FILE="error_report.md"             # выходной отчёт в Markdown

# Проверка существования файлов
if [ ! -f "$LOG_FILE" ]; then
    echo "Ошибка: файл лога '$LOG_FILE' не найден." >&2
    exit 1
fi

if [ ! -f "$CODES_FILE" ]; then
    echo "Ошибка: файл кодов '$CODES_FILE' не найден." >&2
    exit 1
fi

# Временный файл для очищенного списка кодов
TMP_CODES=$(mktemp)

# Извлекаем только строки с кодом (5 символов A-Z,0-9) и названием условия
# Файл A.1.txt имеет табуляцию в качестве разделителя
awk -F'\t' '$1 ~ /^[0-9A-Z]{5}$/ {print $1, $2}' "$CODES_FILE" > "$TMP_CODES"

if [ ! -s "$TMP_CODES" ]; then
    echo "Ошибка: не удалось извлечь коды ошибок из '$CODES_FILE'." >&2
    rm -f "$TMP_CODES"
    exit 1
fi

# Формируем отчёт: считаем строки лога, содержащие каждый код
awk -v codefile="$TMP_CODES" '
BEGIN {
    n = 0
    # Чтение справочника кодов
    while ((getline < codefile) > 0) {
        code = $1
        name = $2
        codes[code] = name
        count[code] = 0
        order[++n] = code      # сохраняем исходный порядок
    }
    close(codefile)
}
{
    delete seen
    # Разбиваем строку лога на слова (буквы/цифры)
    n_words = split($0, words, /[^a-zA-Z0-9]+/)
    for (i = 1; i <= n_words; i++) {
        w = words[i]
        if (length(w) == 5 && w ~ /^[0-9A-Z]{5}$/ && w in codes) {
            seen[w] = 1
        }
    }
    # Увеличиваем счётчики найденных кодов (одно увеличение на строку)
    for (c in seen) {
        count[c]++
    }
}
END {
    # Проверяем, есть ли данные для вывода (количество > 0 и код не 00000)
    has_data = 0
    for (i = 1; i <= n; i++) {
        code = order[i]
        if (count[code] > 0 && code != "00000") {
            has_data = 1
            break
        }
    }

    if (has_data) {
        print "| Код ошибки | Имя условия | Количество ошибок |"
        print "| --- | --- | --- |"
        for (i = 1; i <= n; i++) {
            code = order[i]
            if (count[code] > 0 && code != "00000") {
                printf "| %s | %s | %d |\n", code, codes[code], count[code]
            }
        }
    }
}
' "$LOG_FILE" > "$REPORT_FILE"

# Удаляем временный файл
rm -f "$TMP_CODES"

echo "Готово: отчёт сохранён в '$REPORT_FILE'"