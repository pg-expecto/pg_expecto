#!/usr/bin/env bash
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
# Linux_details.sh
# Скрипт для получения версии ядра и параметров планировщика ввода-вывода
# Результат сохраняется в файл Linux_details.txt
# Запуск от non-root пользователя. При отсутствии доступа выводится предупреждение.
# version 8.1
# updated 17/04/2026

set -euo pipefail

OUTPUT_FILE="Linux_details.txt"

# Проверка возможности записи в файл
if ! touch "$OUTPUT_FILE" 2>/dev/null; then
    echo "Ошибка: нет прав на запись в файл $OUTPUT_FILE в текущей директории." >&2
    exit 1
fi

# Весь основной вывод направляем в файл
{
    # Цвета для оформления (можно использовать, но в файле они не нужны, оставим без ANSI)
    echo "=== Информация о системе ==="
    echo "Дата и время выполнения: $(date '+%Y-%m-%d %H:%M:%S')"
    
    # 1. Версия ядра (всегда доступна)
    KERNEL_VERSION=$(uname -r)
    echo "Версия ядра: ${KERNEL_VERSION}"
    
    # 2. Параметры планировщика I/O
    echo ""
    echo "=== Параметры планировщика I/O ==="
    
    # Проверяем, существует ли директория /sys/block
    if [[ ! -d "/sys/block" ]]; then
        echo "ОШИБКА: Директория /sys/block не найдена. Невозможно получить информацию о блочных устройствах."
        exit 1
    fi
    
    # Флаг, чтобы отслеживать, было ли хоть одно устройство обработано
    found_device=false
    
    # Перебираем все устройства в /sys/block (исключаем loop и ram устройства)
    for device_path in /sys/block/*; do
        device=$(basename "$device_path")
        
        # Пропускаем устройства loop* и ram*
        if [[ "$device" =~ ^(loop|ram) ]]; then
            continue
        fi
        
        found_device=true
        echo ""
        echo "Устройство: /dev/$device"
        
        # Файл scheduler
        scheduler_file="$device_path/queue/scheduler"
        if [[ -r "$scheduler_file" ]]; then
            scheduler_info=$(cat "$scheduler_file" 2>/dev/null || echo "Ошибка чтения")
            echo "  Доступные планировщики и текущий: $scheduler_info"
        else
            echo "  [НЕТ ДОСТУПА] Не удалось прочитать $scheduler_file (требуются права root?)"
        fi
        
        # Дополнительные параметры планировщика (директория iosched)
        iosched_dir="$device_path/queue/iosched"
        if [[ -d "$iosched_dir" ]]; then
            echo "  Параметры текущего планировщика ($device):"
            for param_file in "$iosched_dir"/*; do
                if [[ -f "$param_file" ]]; then
                    param_name=$(basename "$param_file")
                    if [[ -r "$param_file" ]]; then
                        param_value=$(cat "$param_file" 2>/dev/null || echo "Ошибка чтения")
                        echo "    $param_name = $param_value"
                    else
                        echo "    [НЕТ ДОСТУПА] $param_name"
                    fi
                fi
            done
        else
            echo "  Директория iosched отсутствует (возможно, планировщик не поддерживает дополнительные параметры или устройство не является диском)."
        fi
    done
    
    if [[ "$found_device" == false ]]; then
        echo "Не найдено ни одного блочного устройства (кроме loop/ram)."
    fi
    
    echo ""
} > "$OUTPUT_FILE" 2>&1

# Вывод уведомления в консоль
echo "Результаты сохранены в файл: $OUTPUT_FILE"