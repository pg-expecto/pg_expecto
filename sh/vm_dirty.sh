#!/bin/bash
# Расчет dirty pages относительно лимитов
# vm_dirty.sh &
# version 7.1

REPORT_FILE='/postgres/pg_expecto/vm_dirty.log'
echo ' ' > $REPORT_FILE

while true; do
    # Получаем только точное значение nr_dirty
    DIRTY_PAGES=$(grep -w '^nr_dirty' /proc/vmstat | awk '{print $2}')
    
    # Проверяем, что значение получено
    if [ -z "$DIRTY_PAGES" ]; then
        echo "Ошибка: не удалось получить nr_dirty из /proc/vmstat"
        exit 1
    fi
    
    DIRTY_KB=$((DIRTY_PAGES * 4))
    
    # Получаем все параметры
    DIRTY_RATIO=$(cat /proc/sys/vm/dirty_ratio 2>/dev/null || echo 0)
    DIRTY_BYTES=$(cat /proc/sys/vm/dirty_bytes 2>/dev/null || echo 0)
    DIRTY_BG_RATIO=$(cat /proc/sys/vm/dirty_background_ratio 2>/dev/null || echo 0)
    DIRTY_BG_BYTES=$(cat /proc/sys/vm/dirty_background_bytes 2>/dev/null || echo 0)
    
    # Более точная оценка доступной памяти
    AVAILABLE_MEM_KB=$(grep MemAvailable /proc/meminfo | awk '{print $2}')
    if [ -z "$AVAILABLE_MEM_KB" ]; then
        # Fallback для старых ядер
        FREE_KB=$(grep MemFree /proc/meminfo | awk '{print $2}')
        CACHED_KB=$(grep ^Cached /proc/meminfo | awk '{print $2}')
        AVAILABLE_MEM_KB=$((FREE_KB + CACHED_KB))
    fi
    
    # Расчет лимита для dirty (основного лимита) - для информации
    if [ "$DIRTY_BYTES" -gt 0 ]; then
        # Используем абсолютное значение в байтах (конвертируем в KB)
        DIRTY_LIMIT_KB=$((DIRTY_BYTES / 1024))
    elif [ "$DIRTY_RATIO" -gt 0 ]; then
        # Используем процент от доступной памяти
        DIRTY_LIMIT_KB=$((AVAILABLE_MEM_KB * DIRTY_RATIO / 100))
    else
        # Оба параметра равны 0 - лимит не установлен
        DIRTY_LIMIT_KB=0
    fi
    
    # Расчет лимита для dirty_background (фонового лимита) - для информации
    if [ "$DIRTY_BG_BYTES" -gt 0 ]; then
        # Используем абсолютное значение в байтах (конвертируем в KB)
        DIRTY_BG_LIMIT_KB=$((DIRTY_BG_BYTES / 1024))
    elif [ "$DIRTY_BG_RATIO" -gt 0 ]; then
        # Используем процент от доступной памяти
        DIRTY_BG_LIMIT_KB=$((AVAILABLE_MEM_KB * DIRTY_BG_RATIO / 100))
    else
        # Оба параметра равны 0 - лимит не установлен
        DIRTY_BG_LIMIT_KB=0
    fi
    
    # Расчет процентов только по относительным лимитам (ratio)
    # Если заданы абсолютные значения (bytes), то проценты = 0
    
    # Для dirty лимита
    if [ "$DIRTY_BYTES" -gt 0 ]; then
        # Если заданы абсолютные значения, проценты не считаем
        DIRTY_PERCENT=0
    elif [ "$DIRTY_RATIO" -gt 0 ]; then
        # Считаем процент от относительного лимита
        DIRTY_RATIO_LIMIT_KB=$((AVAILABLE_MEM_KB * DIRTY_RATIO / 100))
        if [ "$DIRTY_RATIO_LIMIT_KB" -gt 0 ]; then
            DIRTY_PERCENT=$((DIRTY_KB * 100 / DIRTY_RATIO_LIMIT_KB))
        else
            DIRTY_PERCENT=0
        fi
    else
        # Нет ни абсолютных, ни относительных значений
        DIRTY_PERCENT=0
    fi
    
    # Для dirty_background лимита
    if [ "$DIRTY_BG_BYTES" -gt 0 ]; then
        # Если заданы абсолютные значения, проценты не считаем
        DIRTY_BG_PERCENT=0
    elif [ "$DIRTY_BG_RATIO" -gt 0 ]; then
        # Считаем процент от относительного лимита
        DIRTY_BG_RATIO_LIMIT_KB=$((AVAILABLE_MEM_KB * DIRTY_BG_RATIO / 100))
        if [ "$DIRTY_BG_RATIO_LIMIT_KB" -gt 0 ]; then
            DIRTY_BG_PERCENT=$((DIRTY_KB * 100 / DIRTY_BG_RATIO_LIMIT_KB))
        else
            DIRTY_BG_PERCENT=0
        fi
    else
        # Нет ни абсолютных, ни относительных значений
        DIRTY_BG_PERCENT=0
    fi
    
    # Для отладки можно раскомментировать следующие строки:
    # echo "-------------------------------------------------------------------------------------------------"
    # echo "Dirty: ${DIRTY_KB} KB (${DIRTY_PAGES} страниц)"
    # echo "Available memory: ${AVAILABLE_MEM_KB} KB"
    # echo "dirty_ratio: ${DIRTY_RATIO}%, dirty_bytes: ${DIRTY_BYTES}"
    # echo "dirty_background_ratio: ${DIRTY_BG_RATIO}%, dirty_background_bytes: ${DIRTY_BG_BYTES}"
    # echo "Dirty limit: ${DIRTY_LIMIT_KB} KB"
    # echo "Dirty background limit: ${DIRTY_BG_LIMIT_KB} KB"
    # echo "Current: ${DIRTY_PERCENT}% от dirty ratio limit, ${DIRTY_BG_PERCENT}% от dirty_background ratio limit"
    
    AVAILABLE_MEM_MB=$((AVAILABLE_MEM_KB / 1024))
    
    echo $(date "+%d-%m-%Y %H:%M:%S")' | '$DIRTY_KB' | '$DIRTY_PERCENT' | '$DIRTY_BG_PERCENT' | '$AVAILABLE_MEM_MB >> $REPORT_FILE
    
    #актуальные данные в течении 1 минуты
    lines=$(wc -l < $REPORT_FILE)
    if [ $lines -ge 62 ]
    then 
        sed -i '1d' $REPORT_FILE
    fi
    
    sleep 1
done