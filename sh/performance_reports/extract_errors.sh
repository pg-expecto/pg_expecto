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
# extract_errors.sh — подсчёт ошибок PostgreSQL по справочнику кодов
# Версия: 8.1.1 (исправлен парсинг справочника и лога)

LOG_FILE="postgresql_extract.log"
CODES_FILE="A.1.txt"
REPORT_FILE="error_report.md"

# 1. Проверка наличия файлов
if [ ! -f "$LOG_FILE" ]; then
    echo "Ошибка: файл лога '$LOG_FILE' не найден." >&2
    exit 1
fi
if [ ! -f "$CODES_FILE" ]; then
    echo "Ошибка: файл кодов '$CODES_FILE' не найден." >&2
    exit 1
fi

# 2. Формируем временный справочник: код|имя_условия (только корректные строки)
TMP_CODES=$(mktemp)
awk '
    # Ищем строки, начинающиеся ровно с 5 символов [0-9A-Z] и затем пробельный разделитель
    /^[0-9A-Z]{5}[[:space:]]+[A-Za-z_][A-Za-z_0-9]*/ {
        code = substr($1, 1, 5)          # первый токен — код
        # Удаляем код и следующие за ним пробельные символы, получаем имя условия
        sub(/^[0-9A-Z]{5}[[:space:]]+/, "")
        name = $0
        # Оставляем только первую часть имени (до возможного комментария)
        sub(/[[:space:]].*$/, "", name)
        if (code != "" && name != "")
            print code "|" name
    }
' "$CODES_FILE" > "$TMP_CODES"

if [ ! -s "$TMP_CODES" ]; then
    echo "Ошибка: не удалось извлечь коды ошибок из '$CODES_FILE'." >&2
    rm -f "$TMP_CODES"
    exit 1
fi

# 3. Обработка лога и формирование отчёта
grep -E 'ERROR|ОШИБКА|FATAL|ВАЖНО|ПАНИКА|PANIC' "$LOG_FILE" | \
awk -F '|' -v codefile="$TMP_CODES" '
BEGIN {
    # Загружаем справочник: массив codes[код] = имя, массив count[код] = 0
    while ((getline < codefile) > 0) {
        if ($1 ~ /^[0-9A-Z]{5}$/ && $2 != "") {
            codes[$1] = $2
            count[$1] = 0
            order[++n] = $1
        }
    }
    close(codefile)
}
{
    # Поле 7 — код ошибки, удаляем ведущие/конечные пробелы
    errcode = $7
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", errcode)
    # Учитываем, только если код известен и не "00000"
    if (errcode in codes && errcode != "00000") {
        count[errcode]++
    }
}
END {
    has_data = 0
    for (i = 1; i <= n; i++) {
        if (count[order[i]] > 0) {
            has_data = 1
            break
        }
    }
    if (has_data) {
        print "| Код ошибки | Имя условия | Количество ошибок |"
        print "| --- | --- | --- |"
        for (i = 1; i <= n; i++) {
            c = order[i]
            if (count[c] > 0) {
                printf "| %s | %s | %d |\n", c, codes[c], count[c]
            }
        }
    } else {
        print "Ошибок не обнаружено."
    }
}
' > "$REPORT_FILE"

# 4. Очистка
rm -f "$TMP_CODES"
echo "Готово: отчёт сохранён в '$REPORT_FILE'"
