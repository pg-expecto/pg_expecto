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
# vm_dirty_values.sh
# version 6.1 - оптимизированная версия

REPORT_FILE='/postgres/pg_expecto/vm_dirty.log'

# Функция для вычисления медианы указанного поля
calculate_median() {
    local field=$1
    local description=$2
    
    local median=$(tail -n 60 "$REPORT_FILE" | awk -F'|' -v field="$field" '
    {
        # Сохраняем значения в массив
        values[NR] = $field
    }
    END {
        n = NR
        if (n == 0) exit
        
        # Используем встроенную сортировку
        asort(values)
        
        # Вычисляем медиану
        if (n % 2 == 1) {
            # Нечетное количество элементов
            median = values[int((n + 1) / 2)]
        } else {
            # Четное количество элементов
            median = (values[n / 2] + values[n / 2 + 1]) / 2
        }
        
        printf "%.2f\n", median
    }')
    
    echo $median
}

# Вычисляем медианы для всех полей
#calculate_median 2 "DIRTY_KB"
#calculate_median 3 "DIRTY_PERCENT"
#calculate_median 4 "DIRTY_BG_PERCENT"
#calculate_median 5 "AVAILABLE_MEM_MB"

result=$(calculate_median 2)' '$(calculate_median 3)' '$(calculate_median 4)' '$(calculate_median 5)

echo $result

exit 0
