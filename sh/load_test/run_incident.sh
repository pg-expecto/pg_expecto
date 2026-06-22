#!/usr/bin/env bash
# run_incident.sh – запуск VACUUM FREEZE в запланированные моменты для имитации инцидентов.
# version 12.1
# 19.06.2026

set -euo pipefail

# --- Настраиваемые параметры ---
PGDATABASE="${PGDATABASE:-expecto_db}"
PGUSER="${PGUSER:-expecto_user}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"

VACUUM_COST_DELAY="${VACUUM_COST_DELAY:-20}"   # ms
VACUUM_COST_LIMIT="${VACUUM_COST_LIMIT:-200}"
TABLE_NAME="${TABLE_NAME:-pgbench_accounts}"

LOCK_DIR="${LOCK_DIR:-/tmp}"
VACUUM_LOCK_FILE="${LOCK_DIR}/incident_vacuum.lock"

MIN_INCIDENT_DURATION_SECONDS="${MIN_INCIDENT_DURATION_SECONDS:-30}"  # сек
MAX_RETRIES="${MAX_RETRIES:-5}"
TARGET_INCIDENTS_PER_DAY="${TARGET_INCIDENTS_PER_DAY:-2}"

PSQL="${PSQL:-psql}"
# ---------------------------------------------------------------------

exec_sql_scalar() {
    local sql="$1"
    ${PSQL} -d "${PGDATABASE}" -U "${PGUSER}" -h "${PGHOST}" -p "${PGPORT}" -t -A -c "$sql" 2>/dev/null
}

# Функция обработки одного события
process_event() {
    local scheduled_id=$1
    local scheduled_time=$(exec_sql_scalar "SELECT scheduled_start FROM incident_schedule WHERE id = ${scheduled_id}")
    local priority=$(exec_sql_scalar "SELECT priority FROM incident_schedule WHERE id = ${scheduled_id}")
    local retry_count=$(exec_sql_scalar "SELECT retry_count FROM incident_schedule WHERE id = ${scheduled_id}")

    echo "$(date): Обработка события ID=${scheduled_id}, scheduled=${scheduled_time}, попытка #$((retry_count+1))"

    local start_time=$(date +%s)
    ${PSQL} -d "${PGDATABASE}" -U "${PGUSER}" -h "${PGHOST}" -p "${PGPORT}" -v ON_ERROR_STOP=1 <<-SQL
        SET vacuum_cost_delay = ${VACUUM_COST_DELAY};
        SET vacuum_cost_limit = ${VACUUM_COST_LIMIT};
        VACUUM FREEZE ${TABLE_NAME};
SQL
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    echo "$(date): VACUUM выполнен за ${duration} секунд."

    if [[ ${duration} -ge ${MIN_INCIDENT_DURATION_SECONDS} ]]; then
        local incident_id=$(exec_sql_scalar "
            INSERT INTO performance_incident (priority, start_timepoint, finish_timepoint)
            VALUES (${priority}, now(), now())
            RETURNING id;
        ")
        ${PSQL} -d "${PGDATABASE}" -U "${PGUSER}" -h "${PGHOST}" -p "${PGPORT}" -v ON_ERROR_STOP=1 <<-SQL
            UPDATE incident_schedule
            SET executed = TRUE, actual_start = now(), actual_finish = now()
            WHERE id = ${scheduled_id};
SQL
        echo "$(date): Инцидент #${incident_id} зафиксирован (длительность ${duration} сек)."
        return 0
    else
        local new_retry_count=$((retry_count + 1))
        if [[ ${new_retry_count} -ge ${MAX_RETRIES} ]]; then
            ${PSQL} -d "${PGDATABASE}" -U "${PGUSER}" -h "${PGHOST}" -p "${PGPORT}" -v ON_ERROR_STOP=1 <<-SQL
                UPDATE incident_schedule
                SET executed = TRUE, retry_count = ${new_retry_count}
                WHERE id = ${scheduled_id};
SQL
            echo "$(date): Достигнут лимит попыток (${MAX_RETRIES}) для события ID=${scheduled_id}, помечено выполненным без инцидента."
            return 1
        else
            ${PSQL} -d "${PGDATABASE}" -U "${PGUSER}" -h "${PGHOST}" -p "${PGPORT}" -v ON_ERROR_STOP=1 <<-SQL
                UPDATE incident_schedule
                SET retry_count = ${new_retry_count}
                WHERE id = ${scheduled_id};
SQL
            echo "$(date): Попытка не удалась (длительность ${duration} сек < ${MIN_INCIDENT_DURATION_SECONDS}), будет повтор."
            return 2
        fi
    fi
}

# --- Генерация расписания ---
${PSQL} -d "${PGDATABASE}" -U "${PGUSER}" -h "${PGHOST}" -p "${PGPORT}" \
    -c "SELECT generate_incident_schedule(7);" >/dev/null 2>&1

# --- Основной блок с блокировкой ---
(
    flock -n 200 || {
        echo "$(date): Не удалось захватить блокировку – возможно, уже выполняется другой процесс."
        exit 0
    }

    # Убрали local – теперь просто присваиваем переменные
    incident_count=$(exec_sql_scalar "SELECT count_incidents_last_24h()")
    echo "$(date): Количество инцидентов за 24 часа: ${incident_count}, целевое: ${TARGET_INCIDENTS_PER_DAY}"

    extra_event_id=""
    if [[ ${incident_count} -lt ${TARGET_INCIDENTS_PER_DAY} ]]; then
        echo "$(date): Мало инцидентов, создаем внеплановое событие."
        extra_event_id=$(exec_sql_scalar "
            INSERT INTO incident_schedule (scheduled_start, duration_minutes, priority, retry_count)
            VALUES (now(), 5, 4, 0)
            RETURNING id;
        ")
        echo "$(date): Создано внеплановое событие ID=${extra_event_id}"
        process_event "${extra_event_id}" || true
    fi

    scheduled_id=$(exec_sql_scalar "
        SELECT id
        FROM incident_schedule
        WHERE executed = FALSE
          AND scheduled_start <= now()
        ORDER BY scheduled_start
        LIMIT 1
    ")

    if [[ -n "${scheduled_id}" ]]; then
        if [[ "${scheduled_id}" != "${extra_event_id}" ]]; then
            echo "$(date): Найдено запланированное событие ID=${scheduled_id}"
            process_event "${scheduled_id}" || true
        else
            echo "$(date): Событие ID=${scheduled_id} уже обработано как внеплановое."
        fi
    else
        echo "$(date): Нет запланированных событий на текущий момент."
    fi

) 200>"$VACUUM_LOCK_FILE" &

echo "$(date): Завершение основного скрипта (фоновый процесс запущен)."