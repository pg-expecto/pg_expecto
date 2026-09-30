cd /postgres/pge/
vi sql_source.txt
vi license.txt
vi prepare_pg_expecto_sql.sh
chmod 750 prepare_pg_expecto_sql.sh
./prepare_pg_expecto_sql.sh
1. Чтение списка файлов из 'sql_source.txt'...
   Добавление: /tmp/core_cluster_functions.sql
   Добавление: /tmp/core_cluster_tables.sql
   Добавление: /tmp/core_functions.sql
   Добавление: /tmp/core_os_functions.sql
   Добавление: /tmp/core_os_tables.sql
   Добавление: /tmp/core_statement_functions.sql
   Добавление: /tmp/core_statement_tables.sql
   Добавление: /tmp/core_tables.sql
   Добавление: /tmp/load_test_functions.sql
   Добавление: /tmp/load_test_tables.sql
   Добавление: /tmp/repors_queryid_stat.sql
   Добавление: /tmp/report_iostat.sql
   Добавление: /tmp/report_load_test_loading.sql
   Добавление: /tmp/report_postgresql_cluster_performance.sql
   Добавление: /tmp/report_postgresql_wait_event_type.sql
   Добавление: /tmp/report_queryid_for_pareto.sql
   Добавление: /tmp/report_shared_buffers.sql
   Добавление: /tmp/report_sql_list.sql
   Добавление: /tmp/report_vm_dirty.sql
   Добавление: /tmp/report_vmstat.sql
   Добавление: /tmp/report_vmstat_iostat.sql
   Добавление: /tmp/report_vmstat_performance.sql
   Добавление: /tmp/report_wait_event_type_for_pareto.sql
   Добавление: /tmp/report_wait_event_type_vmstat.sql
   Добавление: /tmp/stats_proсessing_functions.sql
   Добавление: /tmp/stats_proсessing_tables.sql
   Добавление: /tmp/performance_metrics_for_markov_chain.sql
2. Сформирован временный файл со всем содержимым.
3. Удаление строк комментариев с 'version'...
4. Добавление содержимого 'license.txt' в начало...
5. Копирование 'pg_expecto.sql' в /tmp/...
6. Установка прав 777 на /tmp/pg_expecto.sql...
Готово. Результирующий файл: /tmp/pg_expecto.sql

