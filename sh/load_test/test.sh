#!/bin/bash
# test.sh
#
# Скрипт управления нагрузочным тестированием.
# Запускается по cron каждую минуту.
#
# Входной параметр: количество инцидентов в сутки (целое число > 0).
#
# * * * * * /postgres/pg_expecto/sh/prod_test.sh <target_incidents> >> /postgres/pg_expecto/sh/prod_test.sh.log 2>&1

# Пути к вспомогательным скриптам
STOP_SCRIPT="/postgres/pg_expecto/sh/load_test/load_test_stop.sh"
START_SCRIPT="/postgres/pg_expecto/sh/load_test/load_test_start.sh"

# Параметры подключения к БД
DB_NAME="expecto_db"
DB_USER="expecto_user"

# ----------------------------------------------------------------------
# Проверка входного параметра
# ----------------------------------------------------------------------
if [ $# -ne 1 ]; then
    echo "Ошибка: необходимо указать целевое количество инцидентов в сутки" >&2
    exit 1
fi

TARGET_INCIDENTS="$1"
if ! [[ "$TARGET_INCIDENTS" =~ ^[0-9]+$ ]] || [ "$TARGET_INCIDENTS" -eq 0 ]; then
    echo "Ошибка: целевое количество должно быть положительным целым числом" >&2
    exit 1
fi

FLAG_FILE="/postgres/pg_expecto/sh/load_test/LOAD_TEST_IN_PROGRESS"

# ----------------------------------------------------------------------
# 1. Подсчёт записей за сегодня
# ----------------------------------------------------------------------
COUNT=$(psql -d "$DB_NAME" -U "$DB_USER" -t -A -c \
    "SELECT COUNT(*)
     FROM performance_incident
     WHERE start_timepoint >= date_trunc('day', now())
       AND start_timepoint < date_trunc('day', now()) + interval '1 day';" 2>/dev/null)

if ! [[ "$COUNT" =~ ^[0-9]+$ ]]; then
    echo "Ошибка: не удалось получить количество записей из БД" >&2
    exit 1
fi

echo "$(date): Текущее количество инцидентов за сегодня: $COUNT, целевое: $TARGET_INCIDENTS"

# ----------------------------------------------------------------------
# 2. Если достигнуто целевое количество — остановить тест (только если он запущен)
# ----------------------------------------------------------------------
if [ "$COUNT" -ge "$TARGET_INCIDENTS" ]; then
    echo "Целевое количество инцидентов - достигнуто"

    if [ -f "$FLAG_FILE" ]; then
        echo "Нагрузочный тест - запущен"
        echo "Остановка теста"

        if [ -x "$STOP_SCRIPT" ]; then
            "$STOP_SCRIPT"
        else
            echo "Ошибка: скрипт остановки '$STOP_SCRIPT' не найден или не исполняем" >&2
            exit 1
        fi
        exit 0
    else
        echo "Нагрузочный тест - НЕ запущен"
        echo "Выход"
        exit 0
    fi
else
    echo "Целевое количество инцидентов - НЕ достигнуто"

    if [ -f "$FLAG_FILE" ]; 
	then
        echo "Нагрузочный тест - запущен"
        echo "Выход"
        exit 0
    else
        echo "Нагрузочный тест не запущен, запускаем (текущее количество $COUNT < $TARGET_INCIDENTS)."
        if [ -x "$START_SCRIPT" ]; then
            "$START_SCRIPT"
        else
            echo "Ошибка: скрипт запуска '$START_SCRIPT' не найден или не исполняем" >&2
            exit 1
        fi
    fi
fi

exit 0