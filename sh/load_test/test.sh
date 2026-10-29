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
# version 3.1
# Скрипт управления нагрузочным тестированием.
#
# Входной параметр: количество инцидентов в сутки (целое число > 0).
#
# */10 * * * * /postgres/pg_expecto/sh/load_test/test.sh 2 >> /postgres/pg_expecto/sh/load_test/test.log 2>&1

# Пути к вспомогательным скриптам
STOP_SCRIPT="/postgres/pg_expecto/sh/load_test/load_test_stop.sh"
START_SCRIPT="/postgres/pg_expecto/sh/load_test/load_test_start.sh"
PARAM_CONF="/postgres/pg_expecto/sh/load_test/param.conf"

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
# Проверка продуктивного периода
# ----------------------------------------------------------------------
PRODUCTIVE_HOURS=$(psql -d "$DB_NAME" -U "$DB_USER" -t -A -F '|' -c \
    "SELECT productive_load_start_hour, productive_load_end_hour
     FROM markov_config
     LIMIT 1;" 2>/dev/null)

if ! [[ "$PRODUCTIVE_HOURS" =~ ^([0-9]{1,2})\|([0-9]{1,2})$ ]]; then
    echo "Ошибка: не удалось получить продуктивные часы из markov_config" >&2
    exit 1
fi

START_HOUR=$((10#${BASH_REMATCH[1]}))
END_HOUR=$((10#${BASH_REMATCH[2]}))

if [ "$START_HOUR" -lt 0 ] || [ "$START_HOUR" -gt 23 ] || \
   [ "$END_HOUR" -lt 0 ] || [ "$END_HOUR" -gt 23 ] || \
   [ "$START_HOUR" -gt "$END_HOUR" ]; then
    echo "Ошибка: некорректные продуктивные часы в markov_config: $START_HOUR-$END_HOUR" >&2
    exit 1
fi

CURRENT_HOUR=$(date +%H)
CURRENT_HOUR=$((10#$CURRENT_HOUR))

# ----------------------------------------------------------------------
# НЕ ПРОДУКТИВНЫЙ ПЕРИОД
# ----------------------------------------------------------------------
if [ "$CURRENT_HOUR" -lt "$START_HOUR" ] || [ "$CURRENT_HOUR" -gt "$END_HOUR" ];
then
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"НЕ ПРОДУКТИВНЫЙ ПЕРИОД "

    if [ -f "$FLAG_FILE" ];
    then
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Нагрузочный тест - запущен"
        test_mode=$(grep -c "low" "$PARAM_CONF")

        if [[ "$test_mode" == "1" ]]
        then
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"РЕЖИМ LOW"
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Выход"
            exit 0
        else
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Нагрузочный тест - запущен в режиме HIGH"
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Перезапуск теста в режиме LOW"

            if [ -x "$STOP_SCRIPT" ];
            then
                "$STOP_SCRIPT"
            else
                echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: скрипт остановки '$STOP_SCRIPT' не найден или не исполняем" >&2
                exit 1
            fi

            if [ -x "$START_SCRIPT" ];
            then
                echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"ПАРАМЕТРЫ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ: РЕЖИМ LOW"
                cp /postgres/pg_expecto/sh/load_test/param.conf.low "$PARAM_CONF"
                "$START_SCRIPT"
            else
                echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: скрипт запуска '$START_SCRIPT' не найден или не исполняем" >&2
                exit 1
            fi
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Выход"
            exit 0
        fi
    else
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Нагрузочный тест не запущен, запускаем ."
        if [ -x "$START_SCRIPT" ];
        then
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"ПАРАМЕТРЫ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ: РЕЖИМ LOW"
            cp /postgres/pg_expecto/sh/load_test/param.conf.low "$PARAM_CONF"
            "$START_SCRIPT"
        else
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: скрипт запуска '$START_SCRIPT' не найден или не исполняем" >&2
            exit 1
        fi
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Выход"
        exit 0
    fi
fi

# ----------------------------------------------------------------------
# ПРОДУКТИВНЫЙ ПЕРИОД
# ----------------------------------------------------------------------
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"ПРОДУКТИВНЫЙ ПЕРИОД "

# ----------------------------------------------------------------------
# 1. Подсчёт записей за сегодня
# ----------------------------------------------------------------------
COUNT=$(psql -d "$DB_NAME" -U "$DB_USER" -t -A -c \
    "SELECT COUNT(*)
     FROM performance_incident
     WHERE start_timepoint >= date_trunc('day', now())
       AND start_timepoint < date_trunc('day', now()) + interval '1 day';" 2>/dev/null)

if ! [[ "$COUNT" =~ ^[0-9]+$ ]]; then
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: не удалось получить количество записей из БД" >&2
    exit 1
fi

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Текущее количество инцидентов за сегодня: $COUNT, целевое: $TARGET_INCIDENTS"
if [ "$COUNT" -ge "$TARGET_INCIDENTS" ]; 
then
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Целевое количество инцидентов - достигнуто"
	
    if [ -f "$FLAG_FILE" ];
    then
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Нагрузочный тест - запущен"
        test_mode=$(grep -c "low" "$PARAM_CONF")

        if [[ "$test_mode" == "1" ]]
        then
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"РЕЖИМ LOW"
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Выход"
            exit 0
        else
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Нагрузочный тест - запущен в режиме HIGH"
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Перезапуск теста в режиме LOW"

            if [ -x "$STOP_SCRIPT" ];
            then
                "$STOP_SCRIPT"
            else
                echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: скрипт остановки '$STOP_SCRIPT' не найден или не исполняем" >&2
                exit 1
            fi

            if [ -x "$START_SCRIPT" ];
            then
                echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"ПАРАМЕТРЫ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ: РЕЖИМ LOW"
                cp /postgres/pg_expecto/sh/load_test/param.conf.low "$PARAM_CONF"
                "$START_SCRIPT"
            else
                echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: скрипт запуска '$START_SCRIPT' не найден или не исполняем" >&2
                exit 1
            fi
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Выход"
            exit 0
        fi
    else
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Нагрузочный тест не запущен, запускаем ."
        if [ -x "$START_SCRIPT" ];
        then
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"ПАРАМЕТРЫ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ: РЕЖИМ LOW"
            cp /postgres/pg_expecto/sh/load_test/param.conf.low "$PARAM_CONF"
            "$START_SCRIPT"
        else
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: скрипт запуска '$START_SCRIPT' не найден или не исполняем" >&2
            exit 1
        fi
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Выход"
        exit 0
    fi	
fi

test_mode=$(grep -c "high" "$PARAM_CONF")
if [[ "$test_mode" == "0" ]]
then
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Нагрузочный тест - запущен в режиме LOW"
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Перезапуск теста в режиме HIGH"

    if [ -x "$STOP_SCRIPT" ];
    then
        "$STOP_SCRIPT"
    else
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: скрипт остановки '$STOP_SCRIPT' не найден или не исполняем" >&2
        exit 1
    fi

    if [ -x "$START_SCRIPT" ];
    then
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"ПАРАМЕТРЫ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ: РЕЖИМ HIGH"
        cp /postgres/pg_expecto/sh/load_test/param.conf.high "$PARAM_CONF"
        "$START_SCRIPT"
    else
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: скрипт запуска '$START_SCRIPT' не найден или не исполняем" >&2
        exit 1
    fi
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Выход"
    exit 0
fi


# ----------------------------------------------------------------------
# 2. Если достигнуто целевое количество — остановить тест (только если он запущен)
# ----------------------------------------------------------------------
if [ "$COUNT" -ge "$TARGET_INCIDENTS" ]; then
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Целевое количество инцидентов - достигнуто"

    if [ -f "$FLAG_FILE" ]; then
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Нагрузочный тест - запущен"
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Остановка теста"

        if [ -x "$STOP_SCRIPT" ];
        then
            "$STOP_SCRIPT"
        else
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: скрипт остановки '$STOP_SCRIPT' не найден или не исполняем" >&2
            exit 1
        fi
        exit 0
    else
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Нагрузочный тест - НЕ запущен"
    fi

    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"СТАРТ МИНИМАЛЬНОЙ НАГРУЗКИ"
    if [ -x "$START_SCRIPT" ];
    then
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"ПАРАМЕТРЫ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ: РЕЖИМ HIGH"
        cp /postgres/pg_expecto/sh/load_test/param.conf.high "$PARAM_CONF"
        "$START_SCRIPT"
    else
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: скрипт запуска '$START_SCRIPT' не найден или не исполняем" >&2
        exit 1
    fi

else
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Целевое количество инцидентов - НЕ достигнуто"

    if [ -f "$FLAG_FILE" ]; then
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Нагрузочный тест - запущен"
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Выход"
        exit 0
    else
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Нагрузочный тест не запущен, запускаем (текущее количество $COUNT < $TARGET_INCIDENTS)."
        if [ -x "$START_SCRIPT" ];
        then
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"ПАРАМЕТРЫ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ: РЕЖИМ HIGH"
            cp /postgres/pg_expecto/sh/load_test/param.conf.high "$PARAM_CONF"
            "$START_SCRIPT"
        else
            echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S")':'"Ошибка: скрипт запуска '$START_SCRIPT' не найден или не исполняем" >&2
            exit 1
        fi
    fi
fi

exit 0
