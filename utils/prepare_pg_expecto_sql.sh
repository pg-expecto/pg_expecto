#!/bin/bash

# Скрипт для сборки SQL-файлов по списку, очистки от версионных комментариев,
# добавления лицензии и копирования результата в /tmp с правами 777.

set -euo pipefail  # Прерывать выполнение при ошибках, неинициализированных переменных и ошибках в конвейерах

# Имена файлов
SOURCE_LIST="sql_source.txt"
LICENSE_FILE="license.txt"
OUTPUT_FILE="pg_expecto.sql"
TEMP_FILE=$(mktemp)  # Временный файл для сборки
TEMP_LICENSE=$(mktemp)

# Функция очистки временных файлов
cleanup() {
    rm -f "$TEMP_FILE" "$TEMP_LICENSE"
}
trap cleanup EXIT

# Проверка наличия обязательных файлов
if [[ ! -f "$SOURCE_LIST" ]]; then
    echo "Ошибка: файл '$SOURCE_LIST' не найден." >&2
    exit 1
fi

if [[ ! -f "$LICENSE_FILE" ]]; then
    echo "Ошибка: файл '$LICENSE_FILE' не найден." >&2
    exit 1
fi

echo "1. Чтение списка файлов из '$SOURCE_LIST'..."

# Сборка единого файла
> "$TEMP_FILE"  # Очищаем временный файл

while IFS= read -r filename || [[ -n "$filename" ]]; do
    # Пропускаем пустые строки и комментарии в списке (начинающиеся с #)
    [[ -z "$filename" || "$filename" =~ ^[[:space:]]*# ]] && continue

    # Удаляем начальные и конечные пробелы
    filename=$(echo "$filename" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')

    if [[ ! -f "$filename" ]]; then
        echo "Предупреждение: файл '$filename' не существует, пропускаем." >&2
        continue
    fi

    if [[ ! -r "$filename" ]]; then
        echo "Предупреждение: файл '$filename' недоступен для чтения, пропускаем." >&2
        continue
    fi

    echo "   Добавление: $filename"
    cat "$filename" >> "$TEMP_FILE"
    # Добавляем перевод строки после каждого файла для предотвращения склейки строк
    echo "" >> "$TEMP_FILE"
done < "$SOURCE_LIST"

echo "2. Сформирован временный файл со всем содержимым."

# Удаление строк комментариев, содержащих "version"
# Обрабатываем однострочные комментарии SQL (начинающиеся с --) и строки, содержащие "version"
# Многострочные комментарии /* ... */ не обрабатываются для простоты, при необходимости можно расширить.
echo "3. Удаление строк комментариев с 'version'..."
sed -i '/^[[:space:]]*--.*version/Id' "$TEMP_FILE"

# Альтернативный вариант: удалить только подстроку "version" в комментариях, но по заданию удаляем всю строку.
# Если требуется удалить только слово "version", используйте:
# sed -i '/^[[:space:]]*--/ s/version//gI' "$TEMP_FILE"

# Добавление лицензии в начало файла
echo "4. Добавление содержимого '$LICENSE_FILE' в начало..."
cat "$LICENSE_FILE" "$TEMP_FILE" > "$OUTPUT_FILE"

echo "5. Копирование '$OUTPUT_FILE' в /tmp/..."
cp "$OUTPUT_FILE" /tmp/

echo "6. Установка прав 777 на /tmp/$OUTPUT_FILE..."
chmod 777 "/tmp/$OUTPUT_FILE"

echo "Готово. Результирующий файл: /tmp/$OUTPUT_FILE"

exit 0
