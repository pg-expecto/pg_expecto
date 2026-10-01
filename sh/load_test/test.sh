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
# test.sh
#
# Скрипт управления нагрузочным тестированием.
# Запускается по cron каждый час 
#
# Входной параметр: количество инцидентов в сутки (целое число > 0).
#
# 0 * * * * /postgres/pg_expecto/sh/load_test/test.sh 6 >> /postgres/pg_expecto/sh/load_test/test.log 2>&1

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
