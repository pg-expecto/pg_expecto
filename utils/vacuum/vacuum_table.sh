#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Использование: $0 <имя_таблицы> <файл_со_списком_БД>"
    exit 1
fi

TABLE_NAME="$1"
DB_LIST_FILE="$2"
LOG_FILE="vacuum_analyze_${TABLE_NAME}_$(date +%Y%m%d_%H%M%S).log"

if [ ! -f "$DB_LIST_FILE" ]; then
    echo "Ошибка: файл '$DB_LIST_FILE' не найден."
    exit 1
fi

# Экранирование одинарных кавычек (на случай, если они появятся)
SAFE_TABLE_NAME=$(printf "%s" "$TABLE_NAME" | sed "s/'/''/g")

# Функция выполнения VACUUM
do_vacuum() {
    local db="$1"
    local schema="$2"
    local actual_table="$3"
    local full_name="\"$schema\".\"$actual_table\""

    echo "----------------------------------------" >> "$LOG_FILE"
    echo "База данных: $db" >> "$LOG_FILE"
    echo "Выполнение: VACUUM (VERBOSE, ANALYZE) $full_name;" >> "$LOG_FILE"
    psql -d "$db" -c "VACUUM (VERBOSE, ANALYZE) $full_name;" >> "$LOG_FILE" 2>&1
    if [ $? -eq 0 ]; then
        echo "Результат: УСПЕШНО" >> "$LOG_FILE"
    else
        echo "Результат: ОШИБКА (см. выше)" >> "$LOG_FILE"
    fi
}

> "$LOG_FILE"
echo "=== Начало выполнения $(date) ===" >> "$LOG_FILE"
echo "Таблица: $TABLE_NAME" >> "$LOG_FILE"
echo "Список БД: $DB_LIST_FILE" >> "$LOG_FILE"
echo "================================" >> "$LOG_FILE"

while IFS= read -r db || [ -n "$db" ]; do
    [ -z "$db" ] && continue

    db_exists=$(psql -t -A -c "SELECT 1 FROM pg_database WHERE datname = '$db'" 2>/dev/null)
    if [ "$db_exists" != "1" ]; then
        echo "----------------------------------------" >> "$LOG_FILE"
        echo "База данных: $db" >> "$LOG_FILE"
        echo "ОШИБКА: база данных '$db' не найдена в кластере. Пропускаем." >> "$LOG_FILE"
        continue
    fi

    # Поиск таблицы без учёта регистра (прямая подстановка экранированного имени)
    table_info=$(psql -d "$db" -t -A -F '|' -c \
        "SELECT table_schema, table_name FROM information_schema.tables
         WHERE LOWER(table_name) = LOWER('$SAFE_TABLE_NAME')
         LIMIT 1" 2>/dev/null)

    if [ -n "$table_info" ]; then
        schema=$(echo "$table_info" | cut -d'|' -f1)
        actual_table=$(echo "$table_info" | cut -d'|' -f2)
        do_vacuum "$db" "$schema" "$actual_table"
    else
        echo "----------------------------------------" >> "$LOG_FILE"
        echo "База данных: $db" >> "$LOG_FILE"
        echo "Таблица '$TABLE_NAME' не найдена (без учёта регистра). Пропускаем." >> "$LOG_FILE"
    fi
done < "$DB_LIST_FILE"

echo "================================" >> "$LOG_FILE"
echo "=== Окончание выполнения $(date) ===" >> "$LOG_FILE"

echo "Выполнение завершено. Протокол сохранён в файл: $LOG_FILE"
