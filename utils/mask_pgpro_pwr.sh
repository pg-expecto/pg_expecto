#!/bin/bash
# mask_pgpro_pwr.sh
set -euo pipefail

usage() {
    echo "Usage: $0 input.html [output.html]"
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

INPUT="$1"
OUTPUT="${2:-masked_${INPUT}}"

if [ ! -f "$INPUT" ]; then
    echo "Error: File '$INPUT' not found" >&2
    exit 1
fi

# 1. Формирование списка баз данных через psql
DB_LIST_FILE="db.txt"
if ! command -v psql &>/dev/null; then
    echo "Error: psql not found. Cannot retrieve database list." >&2
    exit 1
fi

if ! psql -Aqtc 'SELECT datname FROM pg_database' > "$DB_LIST_FILE" 2>/dev/null; then
    echo "Error: Failed to execute psql command. Check connection and permissions." >&2
    exit 1
fi

if [ ! -s "$DB_LIST_FILE" ]; then
    echo "Error: Database list is empty. Cannot proceed with masking." >&2
    exit 1
fi

echo "Database list saved to $DB_LIST_FILE"

# 2. Формирование списка пользователей (ролей)
USER_LIST_FILE="users.txt"
if ! psql -Aqtc "SELECT rolname FROM pg_roles WHERE rolname NOT LIKE 'pg_%' AND rolname != 'postgres'" > "$USER_LIST_FILE" 2>/dev/null; then
    echo "Warning: Failed to retrieve user list. Skipping user masking." >&2
    USER_LIST_FILE=""
fi

if [ -s "$USER_LIST_FILE" ]; then
    echo "User list saved to $USER_LIST_FILE"
else
    echo "No users found or user list empty. Skipping user masking."
    USER_LIST_FILE=""
fi

# 3. Замена названий баз данных на DB-N (используется awk)
replace_db_names() {
    local input_file="$1"
    local output_file="$2"
    local db_file="$3"

    awk -v db_file="$db_file" '
    function escape_regex(str) {
        gsub(/[.[\\*+?^${}()|]/, "\\\\&", str)
        return str
    }
    BEGIN {
        i = 1
        while ((getline db < db_file) > 0) {
            if (db == "") continue
            map[db] = "DB-" i
            i++
        }
        close(db_file)
    }
    {
        for (db in map) {
            escaped = escape_regex(db)
            gsub("\\y" escaped "\\y", map[db])
        }
        print
    }' "$input_file" > "$output_file"
}

# 4. Замена названий пользователей на USER-N
replace_user_names() {
    local input_file="$1"
    local output_file="$2"
    local user_file="$3"

    awk -v user_file="$user_file" '
    function escape_regex(str) {
        gsub(/[.[\\*+?^${}()|]/, "\\\\&", str)
        return str
    }
    BEGIN {
        i = 1
        while ((getline usr < user_file) > 0) {
            if (usr == "") continue
            map[usr] = "USER-" i
            i++
        }
        close(user_file)
    }
    {
        for (usr in map) {
            escaped = escape_regex(usr)
            gsub("\\y" escaped "\\y", map[usr])
        }
        print
    }' "$input_file" > "$output_file"
}

# Временные файлы
TMP1=$(mktemp)
TMP2=$(mktemp)
TMP3=$(mktemp)
trap "rm -f $TMP1 $TMP2 $TMP3" EXIT

# Последовательная обработка: базы данных -> пользователи -> IP -> SQL
replace_db_names "$INPUT" "$TMP1" "$DB_LIST_FILE"

if [ -n "$USER_LIST_FILE" ] && [ -s "$USER_LIST_FILE" ]; then
    replace_user_names "$TMP1" "$TMP2" "$USER_LIST_FILE"
else
    cp "$TMP1" "$TMP2"
fi

# 5. Маскировка IP-адресов
mask_ip() {
    sed -E \
        -e 's/\b([0-9]{1,3}\.){3}[0-9]{1,3}\b/ip/g' \
        -e 's/\b([0-9a-fA-F]{1,4}:){1,7}[0-9a-fA-F]{1,4}\b/ip/g' \
        -e 's/\b::([0-9a-fA-F]{1,4}:){0,6}[0-9a-fA-F]{1,4}\b/ip/g' \
        "$1"
}

mask_ip "$TMP2" > "$TMP3"

# 6. Маскировка SQL-литералов
mask_sql_literals() {
    awk '
    BEGIN { in_sql = 0 }
    /<pre[^>]*class="[^"]*sql[^"]*"[^>]*>/ || /<code[^>]*class="[^"]*sql[^"]*"[^>]*>/ {
        in_sql = 1
    }
    /<\/pre>/ || /<\/code>/ {
        in_sql = 0
    }
    {
        if (in_sql) {
            # Строковые литералы (с учётом удвоенных кавычек)
            gsub(/\047(\047\047|[^\047])*\047/, "\047?\047")
            # Числовые литералы
            gsub(/\b[0-9]+(\.[0-9]+)?\b/, "?")
        }
        print
    }' "$1"
}

mask_sql_literals "$TMP3" > "$OUTPUT"

echo "Masked report saved to $OUTPUT"
