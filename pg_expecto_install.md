# pg_expecto

**pg_expecto** — это инструмент мониторинга и нагрузочного тестирования PostgreSQL. Он собирает статистику работы базы данных, анализирует события ожидания, сохраняет метрики и формирует отчёты. В состав входят:

- Сбор статистики через расширения `pg_stat_statements` и `pg_wait_sampling`
- Мониторинг системных ресурсов (vmstat, iostat)
- Хранилище результатов в базе данных `expecto_db`
- Набор скриптов для автоматического сбора и анализа данных
- Утилиты для нагрузочного тестирования (`pgbench`-подобные)

## Требования

- Операционная система Linux (протестировано на CentOS, RHEL, Ubuntu)
- PostgreSQL 12 или выше (с поддержкой расширений `pg_stat_statements`, `pg_wait_sampling`)
- Права суперпользователя PostgreSQL (для создания ролей и баз данных)
- Установленные утилиты: `vmstat`, `iostat` (входят в пакет sysstat)
- Доступ в файловую систему для создания каталогов и записи логов

## Установка

1. **Подготовка**  
   Скопируйте архив с pg_expecto во временный каталог (например, `/tmp/pg_expecto`) и распакуйте его.

2. **Создание целевого каталога**  
   ```bash
   mkdir /postgres/pg_expecto
   cp /tmp/pg_expecto/pg_expecto_install.sh /postgres/pg_expecto/
   cd /postgres/pg_expecto
   chmod 750 pg_expecto_install.sh
   ```

3. **Запуск установщика**  
   ```bash
   ./pg_expecto_install.sh
   ```
   Установщик выполнит следующие действия:
   - Создаст служебные папки: `sh/`, `sql/`, `sh/load_test/`, `sh/performance_reports/`, `sh/wait_event_kb/`
   - Скопирует файлы из `/tmp/pg_expecto/sh` и `/tmp/pg_expecto/sql` в целевые директории
   - Создаст базы данных `expecto_db` и `pgbench_db`, пользователя `expecto_user` с паролем `ChangeAfterInstall`
   - Настроит `.pgpass` и `pg_hba.conf` для доступа `expecto_user` к базам
   - Установит расширения `pg_stat_statements` и `pg_wait_sampling` в `expecto_db`
   - Инициализирует репозиторий (выполнит скрипт `pg_expecto.sql` и подготовительные процедуры)
   - Запустит фоновые процессы мониторинга: `vmstat`, `iostat`, `vm_dirty.sh`

   Все действия логируются в `pg_expecto_install.log` и `pg_expecto_install.err`.

## Настройка после установки

### 1. Изменение пароля пользователя `expecto_user`
```sql
ALTER ROLE expecto_user PASSWORD 'новый_пароль';
```

### 2. Обновление файла `~/.pgpass`
Замените пароль в строках, добавленных установщиком:
```
127.0.0.1:5432:expecto_db:expecto_user:новый_пароль
localhost:5432:expecto_db:expecto_user:новый_пароль
127.0.0.1:5432:pgbench_db:expecto_user:новый_пароль
localhost:5432:pgbench_db:expecto_user:новый_пароль
```

### 3. Перезагрузка конфигурации PostgreSQL
Если вы вносили изменения в `pg_hba.conf`, выполните:
```sql
SELECT pg_reload_conf();
```

## Настройка автоматического запуска

Для непрерывного сбора статистики добавьте следующие строки в crontab пользователя `postgres` (через `crontab -e`):
```
*/1 * * * * /postgres/pg_expecto/sh/pg_expecto.sh
*/1 * * * * /postgres/pg_expecto/sh/load_test/load_test.sh
```

## Использование

### Основные скрипты

| Скрипт | Назначение |
|--------|------------|
| `sh/pg_expecto.sh` | Основной сбор статистики: вызовы SQL-функций, запись метрик в репозиторий. |
| `sh/load_test/load_test.sh` | Запуск нагрузочного теста с использованием pgbench. |
| `sh/performance_reports/` | Генерация отчётов по производительности. |
| `sh/wait_event_kb/` | Анализ событий ожидания. |
| `sh/vm_dirty.sh` | Мониторинг параметров dirty страниц в ядре. |

### Логи и выходные данные

- Результаты мониторинга vmstat и iostat пишутся в `/postgres/pg_expecto/vmstat.log` и `/postgres/pg_expecto/iostat.log`.
- Данные pg_expecto хранятся в базе `expecto_db`.
- Ошибки и ход выполнения можно отслеживать в логах, создаваемых при установке и работе скриптов.

## Удаление

1. Остановите фоновые процессы:
   ```bash
   pkill -u postgres -x "vmstat"
   pkill -u postgres -x "iostat"
   pkill -u postgres -x "vm_dirty.sh"
   ```
2. Удалите базы данных:
   ```sql
   DROP DATABASE IF EXISTS expecto_db WITH (FORCE);
   DROP DATABASE IF EXISTS pgbench_db WITH (FORCE);
   DROP ROLE IF EXISTS expecto_user;
   ```
3. Удалите каталог `/postgres/pg_expecto`.


**Внимание!**  
После установки обязательно смените пароль пользователя `expecto_user` и настройте файл `~/.pgpass` для безопасной работы. Не используйте пароль по умолчанию в production-среде.
```
