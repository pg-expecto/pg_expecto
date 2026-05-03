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
# checkpoint_log_processing.sh
#
# version 8.1.1
# updated 03/05/2026
#
# === НАСТРОЙКИ ===
LOG_FILE="postgresql_extract.log"
REPORT_FILE="checkpoint_report.md"

# Проверка наличия лог-файла
if [[ ! -f "$LOG_FILE" ]]; then
    echo "Ошибка: файл '$LOG_FILE' не найден."
    exit 1
fi

# === АНАЛИЗ ЛОГА ===
# Извлекаем все нужные числа из строк с контрольными точками,
# суммируем их и записываем в переменные bash
read count buffers WAL_added WAL_del WAL_rewrite \
     write_duration sync_duration sync_files < <(
    awk '
        /контрольная точка завершена/ {
            count++

            # записано буферов
            if (match($0, /записано буферов: ([0-9]+)/, a))
                buffers += a[1]

            # добавлено файлов WAL
            if (match($0, /добавлено файлов WAL ([0-9]+)/, a))
                wal_added += a[1]

            # удалено WAL
            if (match($0, /удалено: ([0-9]+)/, a))
                wal_del += a[1]

            # переработано WAL
            if (match($0, /переработано: ([0-9]+)/, a))
                wal_rew += a[1]

            # время записи
            if (match($0, /запись=([0-9.]+) сек\./, a))
                write_dur += a[1]

            # время синхронизации
            if (match($0, /синхр\.=([0-9.]+) сек\./, a))
                sync_dur += a[1]

            # синхронизировано файлов
            if (match($0, /синхронизировано_файлов=([0-9]+)/, a))
                sync_f += a[1]
        }
        END {
            # Формат вывода: все числа через пробел
            printf "%d %d %d %d %d %.3f %.3f %d",
                count, buffers, wal_added, wal_del, wal_rew,
                write_dur, sync_dur, sync_f
        }
    ' "$LOG_FILE"
)

# === ФОРМИРОВАНИЕ ОТЧЁТА ===
cat > "$REPORT_FILE" <<EOF
| Количество checkpoint | Записано буферов | Добавлено WAL | Удалено WAL | Переиспользовано WAL | Время записи(сек.) | Время синхронизации(сек.) | Синхронизировано файлов |
|-----------------------|------------------|---------------|-------------|----------------------|--------------------|---------------------------|--------------------------|
| $count | $buffers | $WAL_added | $WAL_del | $WAL_rewrite | $write_duration | $sync_duration | $sync_files |
EOF

echo "Готово: отчёт сохранён в '$REPORT_FILE'"