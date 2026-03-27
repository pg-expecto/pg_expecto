#!/bin/bash

# Проверка аргумента (имя таблицы)
if [ $# -ne 1 ]; then
    echo "Использование: $0 <имя_таблицы>"
    exit 1
fi

TABLE_NAME="$1"
# Экранируем одинарные кавычки на случай, если они всё же появятся
SAFE_TABLE_NAME=$(printf "%s" "$TABLE_NAME" | sed "s/'/''/g")
OUTPUT_FILE="databases_with_table_${TABLE_NAME}.txt"

# Очищаем выходной файл
> "$OUTPUT_FILE"

# Получаем список баз данных, доступных для подключения
# Исключаем служебные базы template0 и template1
databases=$(psql -t -A -c "SELECT datname FROM pg_database WHERE datallowconn = true AND datname NOT IN ('template0', 'template1')")

# Перебираем каждую базу
while IFS= read -r db; do
    [ -z "$db" ] && continue   # пропускаем пустые строки

    # Проверяем существование таблицы с заданным именем (без учёта регистра)
    exists=$(psql -d "$db" -t -A -c \
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE LOWER(table_name) = LOWER('$SAFE_TABLE_NAME'))")

    # Если таблица существует, записываем имя базы в файл
    if [ "$exists" = "t" ]; then
        echo "$db" >> "$OUTPUT_FILE"
    fi
done <<< "$databases"

# Вывод результата
if [ -s "$OUTPUT_FILE" ]; then
    echo "Таблица '$TABLE_NAME' найдена в следующих базах данных:"
    cat "$OUTPUT_FILE"
    echo "Результат сохранён в файл: $OUTPUT_FILE"
else
    echo "Таблица '$TABLE_NAME' не найдена ни в одной из баз данных."
    rm -f "$OUTPUT_FILE"
fi
