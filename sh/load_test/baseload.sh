#!/bin/bash
# Copyright 2026 Ринат (pg_expecto)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# baseload.sh
#
# Постоянная минимальная нагрузка на СУБД через pgbench.
# Предназначен для поддержания конвейера цепи Маркова в рабочем состоянии:
#   - заполняет transition_log / markov_frequencies (обучение);
#   - даёт свежие записи в profile_comparison_log (SICPP, C-alone);
#   - не даёт устареть эталонному окну (baseline_max_age_hours = 12);
#   - поддерживает non-NULL current_state_id для mchain_predict_risk_current_horizon().
#
# Тестовая БД создана и проинициализирована заранее (pgbench -i).
#
# Пример crontab (запуск в 5 минут каждого часа, чтобы не пересекаться с test.sh):
#   5 * * * * /postgres/pg_expecto/sh/load_test/baseload.sh 4 \
#             >> /postgres/pg_expecto/sh/load_test/baseload.log 2>&1
#
# ВАЖНО (см. документацию markov_chain_functions.sql):
#   Нагрузка 24/7 даёт «непродуктивные» профили в ночные часы. Если compare_profiles()
#   не ограничен продуктивными часами (productive_load_start_hour/end_hour), ночные
#   сравнения будут давать ложные CRITICAL. Рекомендация: либо ограничить вызов
#   compare_profiles() продуктивными часами в mchain_train_step(), либо принять
#   повышенный JS-divergence в ночное время.
# Инициализация 
# pgbench -i -s 10 -d expecto_db -U expecto_user

##################################################################################################################
# crontab -e
## Генератор инцидентов (каждый час в 0 минут)
#0 * * * * /postgres/pg_expecto/sh/load_test/test.sh 6 >> /postgres/pg_expecto/sh/load_test/test.log 2>&1
#
## Постоянная минимальная нагрузка (каждый час в 5 минут, 59 мин работы)
#5 * * * * /postgres/pg_expecto/sh/load_test/baseload.sh 4 >> /postgres/pg_expecto/sh/load_test/baseload.log 2>&1
##################################################################################################################

set -u

# ----------------------------------------------------------------------
# Конфигурация
# ----------------------------------------------------------------------
DB_NAME="${DB_NAME:-expecto_db}"
DB_USER="${DB_USER:-expecto_user}"
DB_HOST="${DB_HOST:-${PGHOST:-localhost}}"
DB_PORT="${DB_PORT:-${PGPORT:-5432}}"

# Сценарий pgbench: simple | select-only | tpcb-like | extended
# simple (TPC-B like) — рекомендуемый по умолчанию: смесь чтения/записи,
# реалистичные тренды. select-only даёт «плоские» метрики ожиданий.
PGBENCH_SCENARIO="${PGBENCH_SCENARIO:-simple}"

# Длительность одного цикла (сек). Cron запускает каждый час,
# 3540 сек = 59 мин — оставляет ~1 мин на переподключение.
DURATION_SEC="${DURATION_SEC:-3540}"

# Ограничение TPS на ВЕСЬ пул клиентов (0 = не ограничивать).
# Минимальная нагрузка: 20 TPS (при 4 клиентах = 5 TPS/клиент).
# При увеличении RATE_LIMIT растёт не только «нагрузка», но и шум в
# correlation / os_trend / wait_trend — это уже не «минимальная» нагрузка.
RATE_LIMIT="${RATE_LIMIT:-20}"

# nice, чтобы не конкурировать с test.sh за CPU
NICE_LEVEL="${NICE_LEVEL:-10}"

# Пути и файлы
LOG_DIR="${LOG_DIR:-/postgres/pg_expecto/sh/load_test}"
LOCK_FILE="${LOG_DIR}/BASELOAD_IN_PROGRESS"
PID_FILE="${LOG_DIR}/baseload.pid"

# ----------------------------------------------------------------------
# Проверка аргументов
# ----------------------------------------------------------------------
if [ $# -ne 1 ]; then
    echo "Ошибка: укажите количество параллельных сессий pgbench" >&2
    echo "Использование: $0 <clients>" >&2
    exit 1
fi

CLIENTS="$1"
if ! [[ "$CLIENTS" =~ ^[0-9]+$ ]] || [ "$CLIENTS" -eq 0 ]; then
    echo "Ошибка: количество клиентов должно быть положительным целым числом" >&2
    exit 1
fi
if [ "$CLIENTS" -gt 32 ]; then
    echo "Предупреждение: $CLIENTS клиентов — это уже не «минимальная» нагрузка." >&2
    echo "                 Рекомендуемый диапазон: 2–8." >&2
fi

# ----------------------------------------------------------------------
# Проверка окружения
# ----------------------------------------------------------------------
command -v pgbench >/dev/null 2>&1 || {
    echo "Ошибка: pgbench не найден в PATH" >&2
    exit 1
}
command -v psql >/dev/null 2>&1 || {
    echo "Ошибка: psql не найден в PATH" >&2
    exit 1
}
mkdir -p "$LOG_DIR" || {
    echo "Ошибка: не удалось создать $LOG_DIR" >&2
    exit 1
}

# ----------------------------------------------------------------------
# Защита от наложения (stale lock detection)
# ----------------------------------------------------------------------
if [ -f "$LOCK_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE" 2>/dev/null || echo "")
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "$(date '+%Y-%m-%d %H:%M:%S'): baseload уже запущен (pid=$OLD_PID), выход."
        exit 0
    fi
    echo "$(date '+%Y-%m-%d %H:%M:%S'): обнаружен устаревший lock-файл, удаляем."
    rm -f "$LOCK_FILE" "$PID_FILE"
fi

# ----------------------------------------------------------------------
# Проверка доступности БД и инициализации pgbench
# ----------------------------------------------------------------------
if ! psql -d "$DB_NAME" -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" \
        -t -A -c "SELECT 1" >/dev/null 2>&1; then
    echo "Ошибка: не удалось подключиться к БД $DB_NAME@$DB_HOST:$DB_PORT" >&2
    exit 1
fi

ACCOUNTS_EXISTS=$(psql -d "$DB_NAME" -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" \
    -t -A -c "SELECT to_regclass('public.pgbench_accounts') IS NOT NULL" 2>/dev/null)
if [ "$ACCOUNTS_EXISTS" != "t" ]; then
    echo "Ошибка: таблица pgbench_accounts не найдена в $DB_NAME." >&2
    echo "        Выполните: pgbench -i -s 10 -d $DB_NAME -U $DB_USER" >&2
    exit 1
fi

# ----------------------------------------------------------------------
# Lock-файлы и trap (корректная очистка при SIGTERM/SIGINT)
# ----------------------------------------------------------------------
cleanup() {
    local code=$?
    rm -f "$LOCK_FILE" "$PID_FILE"
    echo "$(date '+%Y-%m-%d %H:%M:%S'): baseload завершён (pid=$$, exit=$code)"
}
trap cleanup EXIT INT TERM

echo $$ > "$LOCK_FILE"
echo $$ > "$PID_FILE"

# ----------------------------------------------------------------------
# Аргументы pgbench
# ----------------------------------------------------------------------
PGBENCH_ARGS=(
    -c "$CLIENTS"
    -j "$CLIENTS"                 # threads = clients (для 2–8 это ок)
    -b "$PGBENCH_SCENARIO"
    -T "$DURATION_SEC"
    -d "$DB_NAME"
    -U "$DB_USER"
    -h "$DB_HOST"
    -p "$DB_PORT"
    -n                            # --no-vacuum: не тратить время на VACUUM
    -P 60                         # прогресс раз в минуту
)

if [ -n "$RATE_LIMIT" ] && [ "$RATE_LIMIT" != "0" ]; then
    PGBENCH_ARGS+=( -R "$RATE_LIMIT" )
fi

# ----------------------------------------------------------------------
# Запуск
# ----------------------------------------------------------------------
echo "$(date '+%Y-%m-%d %H:%M:%S'): старт baseload: scenario=$PGBENCH_SCENARIO, clients=$CLIENTS, duration=${DURATION_SEC}s, rate_limit=${RATE_LIMIT:-none}"

nice -n "$NICE_LEVEL" pgbench "${PGBENCH_ARGS[@]}" 2>&1 \
    | while IFS= read -r line; do
          echo "$(date '+%Y-%m-%d %H:%M:%S') | $line"
      done

EXIT_CODE=${PIPESTATUS[0]}
exit "$EXIT_CODE"