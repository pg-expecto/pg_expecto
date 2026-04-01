# pg_expecto – Мониторинг и нагрузочное тестирование PostgreSQL

`pg_expecto` – это набор shell- и SQL-скриптов для комплексного мониторинга производительности PostgreSQL, сбора системных метрик (vmstat, iostat), автоматизированного нагрузочного тестирования и генерации детализированных аналитических отчётов.

## 📁 Структура репозитория

```
pg_expecto/
├── sh/
│   ├── pg_expecto.sh          # Корневой скрипт сбора метрик
│   ├── vm_dirty.sh            # Фоновый сбор данных о dirty pages
│   ├── vm_dirty_values.sh     # Расчёт медианных значений dirty pages
│   ├── load_test/             # Нагрузочное тестирование
│   │   ├── readme.md          # Документация по нагрузочному тестированию
│   │   └── ...                # Скрипты и конфигурация для pgbench
│   └── performance_reports/   # Генерация отчётов
│       ├── readme.md          # Документация по отчётам
│       └── ...                # Скрипты анализа и отчётов
```

---

## ⚙️ Основные компоненты

### 1. `pg_expecto.sh` – Центральный сборщик метрик

Корневой скрипт, который выполняет циклический сбор данных о производительности PostgreSQL и ОС.

**Функциональность**:
- Блокировка одновременного запуска через файл-флаг `/postgres/pg_expecto/PG_EXPECTO_IN_PROGRESS`.
- Сбор статистики по SQL-выражениям (функция `statement_stat()`).
- Расчёт метрик производительности кластера (функции `cluster_stat_median()`, `performance_metrics()`).
- Сброс статистики (`pg_stat_reset`, `pg_stat_statements_reset`, `pg_wait_sampling_reset_profile`).
- Обработка данных `vmstat` и `iostat` из лог-файлов с записью в БД.
- Очистка устаревших данных (функция `cleaning()`).
- Перезапуск сборщиков `vmstat` и `iostat` каждые 10 минут.

**Используемые файлы**:
- `/postgres/pg_expecto/vmstat.log` – данные `vmstat`
- `/postgres/pg_expecto/iostat.log` – данные `iostat`
- `/postgres/pg_expecto/PG_EXPECTO_IN_PROGRESS` – флаг выполнения

**Логирование**:
- `pg_expecto.log` – общий лог
- `pg_expecto.err` – ошибки выполнения

---

### 2. `vm_dirty.sh` – Мониторинг dirty pages

Фоновый скрипт для постоянного отслеживания состояния dirty страниц в ядре Linux.

**Выполняет каждую секунду**:
- Чтение `nr_dirty` из `/proc/vmstat`
- Получение параметров: `dirty_ratio`, `dirty_bytes`, `dirty_background_ratio`, `dirty_background_bytes`
- Расчёт доступной памяти (`MemAvailable`)
- Вычисление лимитов и процентов использования
- Запись строки с меткой времени в `/postgres/pg_expecto/vm_dirty.log`

**Формат строки лога**:
```
DD-MM-YYYY HH:MM:SS | DIRTY_KB | DIRTY_PERCENT | DIRTY_BG_PERCENT | AVAILABLE_MEM_MB
```

Лог поддерживает не более 62 записей (1 минута с запасом).

---

### 3. `vm_dirty_values.sh` – Медианные значения dirty pages

Вспомогательный скрипт, который вычисляет медианные значения за последние 60 секунд (строк) из `vm_dirty.log` и выводит их одной строкой.

**Выводит**:
```
DIRTY_KB_median DIRTY_PERCENT_median DIRTY_BG_PERCENT_median AVAILABLE_MEM_MB_median
```

Используется в `pg_expecto.sh` для добавления сводных данных в строку `vmstat`.

---

### 4. Подсистема нагрузочного тестирования (`load_test/`)

Автоматизированная система на базе `pgbench` с поддержкой смешанной OLTP/OLAP-нагрузки, плавным наращиванием числа клиентов и сохранением метрик.

**Ключевые файлы**:
- `param.conf` – конфигурация теста (диапазон клиентов, веса сценариев, тип нагрузки)
- `load_test_start.sh` – инициализация теста (создание БД, функций, флагов)
- `load_test.sh` – основная итерация теста (вызов по cron)
- `load_test_stop.sh` – принудительная остановка и очистка

**Требования**:
- PostgreSQL 10+ с расширением `pg_stat_statements` и `pg_wait_sampling`
- `pgbench` в `PATH`
- Служебная БД `expecto_db` с набором функций (описаны в `load_test/readme.md`)

Подробная документация находится в [load_test/readme.md](sh/load_test/readme.md).

---

### 5. Подсистема генерации отчётов (`performance_reports/`)

Набор скриптов для формирования статистических и корреляционных отчётов о производительности PostgreSQL и системных метрик.

**Основные скрипты**:
- `performance_report.sh` – отчёт за произвольный временной интервал
- `incident_report.sh` – анализ инцидента (сравнение интервалов до и во время инцидента)
- `load_test_report.sh` – отчёт по текущему нагрузочному тесту
- `queryid_report.sh` – детальный анализ конкретного запроса по `queryid`
- `summary_report.sh` – ядро системы, генерирующее комплексный отчёт

**Выходные данные**:
- Все отчёты сохраняются в `/tmp/pg_expecto_reports/`
- Форматы: текстовые таблицы, корреляционные матрицы, диаграммы Парето, исходные данные для построения графиков

Подробная документация находится в [performance_reports/readme.md](sh/performance_reports/readme.md).

---

## 🚀 Быстрый старт

### Требования
- ОС: Linux (скрипты используют `/proc` и специфичные для Linux команды)
- PostgreSQL 10+ с установленными расширениями: `pg_stat_statements`, `pg_wait_sampling`
- Bash (для скриптов с `#!/bin/bash`) или POSIX-совместимый shell
- Права на выполнение `psql`, `vmstat`, `iostat`, `pgbench`
- Служебная БД `expecto_db` и пользователь `expecto_user`

### Настройка окружения
1. Создайте каталог `/postgres/pg_expecto/` (или измените пути в скриптах).
2. Разместите файлы из `sh/` в этом каталоге.
3. Настройте права на выполнение:
   ```bash
   chmod +x /postgres/pg_expecto/*.sh
   ```
4. Создайте БД и пользователя:
   ```sql
   CREATE DATABASE expecto_db;
   CREATE USER expecto_user WITH PASSWORD '...';
   GRANT ALL PRIVILEGATES ON DATABASE expecto_db TO expecto_user;
   ```
5. Создайте необходимые SQL-функции (см. документацию к подсистемам).

### Запуск сбора метрик
```bash
/postgres/pg_expecto/pg_expecto.sh
```
Рекомендуется добавить в `cron` с периодичностью, соответствующей интервалу сбора данных.

### Запуск фоновых сборщиков
```bash
# Запуск мониторинга dirty pages
/postgres/pg_expecto/vm_dirty.sh &

# Запуск сбора vmstat и iostat (обычно перезапускаются pg_expecto.sh)
vmstat 60 -S M -t > /postgres/pg_expecto/vmstat.log 2>&1 &
iostat 60 -d -x -m -t > /postgres/pg_expecto/iostat.log 2>&1 &
```

---

## 📊 Логирование и управление состоянием

| Файл | Назначение |
|------|------------|
| `/postgres/pg_expecto/PG_EXPECTO_IN_PROGRESS` | Блокировка одновременного запуска `pg_expecto.sh` |
| `/postgres/pg_expecto/load_test/LOAD_TEST_STARTED` | Флаг активного нагрузочного теста |
| `/postgres/pg_expecto/load_test/LOAD_TEST_IN_PROGRESS` | Блокировка итерации теста |
| `*.log` | Хронология выполнения скриптов |
| `*.err` | Ошибки выполнения |

---

## 🔧 Настройка

Основные параметры настраиваются через переменные внутри скриптов:
- **Пути**: `current_path`, `REPORT_DIR`, `LOG_FILE`, `ERR_FILE`
- **БД**: `expecto_db`, `expecto_user`
- **Периодичность**: в `pg_expecto.sh` – интервал перезапуска `vmstat`/`iostat` (сейчас 10 минут)
- **Частота сбора**: в `vm_dirty.sh` – пауза `sleep 1`

Для нагрузочного тестирования конфигурация вынесена в `load_test/param.conf`, для отчётов – в `performance_reports/reports.conf`.

---

## 🐛 Устранение неполадок

- **Ошибка `psql: FATAL: database "expecto_db" does not exist`** – создайте БД и пользователя.
- **Нет данных в отчётах** – проверьте, что `vmstat` и `iostat` пишут логи, а БД содержит функции `os_stat_vmstat()` и `os_stat_iostat_device()`.
- **Флаг не снимается** – убедитесь, что скрипт завершился корректно. Принудительно удалите `/postgres/pg_expecto/PG_EXPECTO_IN_PROGRESS`.
- **vm_dirty.sh не запускается** – проверьте права на чтение `/proc/vmstat` и `/proc/sys/vm/*`.

---

## 📄 Лицензия

Скрипты предоставляются "как есть" без каких-либо гарантий. Использование на свой страх и риск.

Этот README объединяет описание всех предоставленных файлов и подсистем, сохраняя детализацию из исходных `readme.md` для нагрузочного тестирования и отчётов. Документ структурирован для быстрого понимания архитектуры и начала работы.
