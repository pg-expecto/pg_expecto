#!/usr/bin/env bash
# run_vacuum.sh – запуск VACUUM FREEZE для таблиц pgbench не чаще 1 раза в час
# в случайную минуту текущего часа.
# Предназначен для запуска по cron каждую минуту:
#   * * * * * /path/to/run_vacuum.sh >> /var/log/vacuum.log 2>&1
#
# Условия:
#   1) Не запускать, если VACUUM FREEZE уже выполняется.
#   2) Запускать только в одну (случайную) минуту каждого часа.
#   3) Удалять устаревшие файлы целевой минуты (после перехода часа).

set -euo pipefail

# --- настраиваемые параметры -------------------------------------------------
PGDATABASE="${PGDATABASE:-pgbench_db}"
PGUSER="${PGUSER:-expecto_user}"

VACUUM_COST_DELAY="${VACUUM_COST_DELAY:-20}"   # ms
VACUUM_COST_LIMIT="${VACUUM_COST_LIMIT:-200}"
TABLE_NAME="${TABLE_NAME:-pgbench_accounts}"

LOCK_DIR="${LOCK_DIR:-/tmp}"
TARGET_MINUTE_PREFIX="vacuum_target_minute_"
VACUUM_LOCK_FILE="${LOCK_DIR}/vacuum_freeze.lock"

PSQL="${PSQL:-psql}"
# -----------------------------------------------------------------------------

# Текущий час и минута (часовой пояс UTC или системный – по вашему усмотрению)
CURRENT_HOUR=$(date +'%Y%m%d%H')
CURRENT_MINUTE=$(date +'%M' | sed 's/^0*//')  # убираем ведущие нули (09 → 9)

# === Функция удаления устаревших файлов целевой минуты ===
cleanup_old_target_files() {
    local pattern="${LOCK_DIR}/${TARGET_MINUTE_PREFIX}*"
    for f in $pattern; do
        # Если файл не существует (например, нет ни одного), пропускаем
        [[ -f "$f" ]] || continue
        # Извлекаем суффикс дата+час (YYYMMDDHH)
        suffix="${f##*${TARGET_MINUTE_PREFIX}}"
        if [[ "$suffix" != "$CURRENT_HOUR" ]]; then
            rm -f "$f"
            echo "$(date): Удалён старый файл целевой минуты: $f"
        fi
    done
}

# === Функция генерации новой случайной минуты для текущего часа ===
generate_target_minute() {
    local rand_min=$(( RANDOM % 60 ))
    echo "$rand_min" > "${LOCK_DIR}/${TARGET_MINUTE_PREFIX}${CURRENT_HOUR}"
    echo "$(date): Сгенерирована целевая минута для часа ${CURRENT_HOUR}: ${rand_min}"
}

# --- 1) Очищаем старые файлы целевой минуты (для часов, отличных от текущего) ---
cleanup_old_target_files

# --- 2) Определяем целевую минуту текущего часа ---
TARGET_FILE="${LOCK_DIR}/${TARGET_MINUTE_PREFIX}${CURRENT_HOUR}"
if [[ ! -f "$TARGET_FILE" ]]; then
    generate_target_minute
fi

TARGET_MINUTE=$(cat "$TARGET_FILE" 2>/dev/null || echo "")
if [[ ! "$TARGET_MINUTE" =~ ^[0-9]+$ ]] || [[ "$TARGET_MINUTE" -lt 0 ]] || [[ "$TARGET_MINUTE" -gt 59 ]]; then
    echo "$(date): Некорректный файл целевой минуты, создаём новый"
    generate_target_minute
    TARGET_MINUTE=$(cat "$TARGET_FILE")
fi

# --- 3) Проверяем, наступила ли целевая минута ---
if [[ "$CURRENT_MINUTE" -ne "$TARGET_MINUTE" ]]; then
    # Не наступила – выходим
    exit 0
fi

# --- 4) Целевая минута совпала – пытаемся запустить VACUUM (если ещё не выполняется) ---
# Используем flock с дескриптором, чтобы надёжно выполнить многострочную команду в фоне.
(
    # Пытаемся эксклюзивно захватить блокировку; если не удалось – выходим.
    flock -n 200 || exit 0

    echo "$(date): Начинаем VACUUM FREEZE на таблице ${TABLE_NAME} (минута ${CURRENT_MINUTE})"

    # Выполняем VACUUM через psql. Все настройки – только для этой сессии.
    ${PSQL} -d "${PGDATABASE}" -U "${PGUSER}" -v ON_ERROR_STOP=1 <<-SQL
        SET vacuum_cost_delay = ${VACUUM_COST_DELAY};
        SET vacuum_cost_limit = ${VACUUM_COST_LIMIT};
        VACUUM FREEZE ${TABLE_NAME};
SQL

    echo "$(date): VACUUM FREEZE на таблице ${TABLE_NAME} завершён"

    # По желанию: удалить файл блокировки после использования (необязательно)
    # rm -f "$VACUUM_LOCK_FILE"
) 200>"$VACUUM_LOCK_FILE" &   # Фоновый запуск всего блока

# Скрипт завершается сразу, не дожидаясь завершения VACUUM.
echo "$(date): VACUUM FREEZE запущен в фоне (блокировка удерживается)"