# pg_expecto – Статистический анализ производительности и ожиданий СУБД PostgreSQL

**pg_expecto** – это комплексный инструмент для глубокого статистического анализа и тестирования производительности PostgreSQL. Релиз знаменует появление мощного и свободно распространяемого решения для администраторов баз данных и разработчиков.

Основная задача pg_expecto – предоставить администраторам и разработчикам инструментарий для выявления узких мест и оптимизации работы PostgreSQL. В отличие от некоторых современных решений, pg_expecto сознательно фокусируется на надёжных и проверенных статистических методах, что обеспечивает полный контроль и прозрачность процесса анализа.

## Ключевые особенности

- **Всесторонний статистический и корреляционный анализ** – глубокий анализ производительности PostgreSQL и событий ожидания (`wait_event_type` / `wait_event`), установление корреляции между внутренним состоянием СУБД и общей производительностью системы.
- **Мониторинг операционной системы** – сбор и анализ метрик ОС с помощью утилит `vmstat` и `iostat`, что позволяет напрямую увязать нагрузку на диск, память и процессор с поведением базы данных.
- **Встроенное нагрузочное тестирование** – проведение нагрузочных тестов для оценки поведения базы данных под давлением и определения пределов её производительности.
- **Построение отчетов для Excel** – экспорт результатов анализа в форматы, совместимые с Microsoft Excel, для дальнейшей обработки, визуализации и представления данных.
- **Обширная база знаний** – передача пользователям большого объёма материалов по результатам экспериментов с проектом [pg_hazel](https://dzen.ru/suite/009d4a06-f053-4377-8fdc-76721bf79c50), служащих ценным источником знаний и практических примеров.
- **Проактивный мониторинг** – формирование репрезентативных профилей производительности систем наблюдения посредством разработки стандартизированных файлов метрических данных. Регистрация события снижения производительности СУБД служит триггером инициирования стандартных и приоритетных инцидентов реагирования.
- **Интеграция с нейросетью** – автоматическая подготовка промптов для нейросети с целью анализа статистически обработанных метрик производительности СУБД и инфраструктуры.
- **Версия 7** – статистическая обработка метрик, промпты для подготовки отчетов нейросетью.

## Системные требования

Для работы pg_expecto требуются установленные утилиты **vmstat** и **iostat**.
Рекомендуется - архиватор **zip** для архивации отчетов и промптов.

### Важно
1. Для работы pg_expecto требуются установленные библиотеки расширений **pg_stat_statements** и **pg_wait_sampling**.

Значение параметра `shared_preload_libraries` должно быть:

```
shared_preload_libraries = 'pg_stat_statements, pg_wait_sampling'
```
**Порядок библиотек важен.**

2. Для анализа фоновых процессов и ошибок СУБД, необходимо установить конфигурационне параметры СУБД:
- log_checkpoints = on
- log_autovacuum_min_duration = 0
- track_io_timing = on
- logging_collector = 'on'
- log_directory = '/log/pg_log'
- log_destination = 'stderr'
- log_rotation_size = '0'
- log_rotation_age = '1d'
- log_line_prefix = '%m| %d| %a| %u| %h| %p| %e| '
- log_truncate_on_rotation = 'on'

## Установка

1. Распаковать zip-архив проекта (`pg_expecto-main.zip`). В результате будет подготовлена папка: **pg_expecto-main**.
2. Скопировать содержимое на целевой сервер СУБД в папку: `/tmp/pg_expecto`.
3. Используя учётную запись *postgres*, на целевом сервере СУБД создать сервисную папку:  
   `mkdir /postgres/pg_expecto`
4. Скопировать инсталлятор:  
   `cp /tmp/pg_expecto/pg_expecto_install.sh /postgres/pg_expecto/`
5. Перейти в папку для начала установки:  
   `cd /postgres/pg_expecto`
6. Подготовить скрипт инсталлятора:  
   `chmod 750 pg_expecto_install.sh`
7. Запустить инсталлятор:  
   `./pg_expecto_install.sh`

## Мониторинг работоспособности pg_expecto

Для просмотра лога работы используйте команду:

```bash
tail -f /postgres/pg_expecto/sh/pg_expecto.log
```

## Использование

Подробный пример использования pg_expecto v.7 с нейросетью DeepSeek описан в статье:  
[PG_EXPECTO v.7 + DeepSeek: полный цикл диагностики производительности PostgreSQL — от нагрузочного тестирования до разбора инцидентов](https://dzen.ru/a/aaATukWy4T8iHW1t?share_to=link)

## Контакты

- Ринат Сунгатуллин: **kznalp@yandex.ru**
- [Дзен-канал](https://dzen.ru/kznalp) : https://dzen.ru/kznalp
- [Телеграм-канал](https://t.me/pg_expecto) : https://t.me/pg_expecto
- [Max](https://max.ru/join/T8sCiETC85Tr4Dkh_nM362PVcCbGDLagF4RZKHf4Udg)

## Статус проекта

Текущая версия: 8.1

## 📜 Лицензия

Начиная с версии 8.0, проект `pg_expecto` распространяется под лицензией [Apache License 2.0](LICENSE).  
Предыдущие версии (до 12.04.2026) доступны под лицензией MIT.  
Полный текст лицензии находится в файле [LICENSE](LICENSE) в корне репозитория.

---

# pg_expecto – Statistical Performance and Wait Event Analysis for PostgreSQL

**pg_expecto** is a comprehensive toolkit for deep statistical analysis and performance testing of PostgreSQL. This release marks the arrival of a powerful, freely distributable solution for database administrators and developers.

The main goal of pg_expecto is to provide administrators and developers with the tools to identify bottlenecks and optimize PostgreSQL performance. Unlike some modern solutions, pg_expecto deliberately focuses on reliable and proven statistical methods, ensuring full control and transparency of the analysis process.

## Key Features

- **Comprehensive statistical and correlation analysis** – deep analysis of PostgreSQL performance and wait events (`wait_event_type` / `wait_event`), establishing correlations between the internal state of the DBMS and overall system performance.
- **Operating system monitoring** – collection and analysis of OS metrics using `vmstat` and `iostat`, directly linking disk, memory, and CPU load to database behavior.
- **Built-in load testing** – the ability to run load tests to evaluate database behavior under pressure and determine performance limits.
- **Excel reporting** – export analysis results to Microsoft Excel‑compatible formats for further processing, visualization, and presentation.
- **Extensive knowledge base** – a wealth of materials from experiments with the [pg_hazel](https://dzen.ru/suite/009d4a06-f053-4377-8fdc-76721bf79c50) project, serving as a valuable source of knowledge and practical examples.
- **Proactive monitoring** – creation of representative performance profiles for monitoring systems through standardized metric data files. Detection of DBMS performance degradation events triggers standard and priority incident response workflows.
- **Neural network integration** – automatic generation of prompts for neural networks to analyze statistically processed DBMS and infrastructure performance metrics.
- **Version 7** – statistical processing of metrics and prompts for generating neural‑network‑assisted reports.

## System Requirements

pg_expecto requires the **vmstat** and **iostat** utilities to be installed.

### Important
pg_expecto requires the **pg_stat_statements** and **pg_wait_sampling** extension libraries to be installed.

The `shared_preload_libraries` parameter must be set as follows:

```
shared_preload_libraries = 'pg_stat_statements, pg_wait_sampling'
```

**The order of libraries is important.**

## Installation

1. Unpack the project zip archive (`pg_expecto-main.zip`). This will create the folder **pg_expecto-main**.
2. Copy the contents to the target DBMS server into `/tmp/pg_expecto`.
3. Using the *postgres* account, create a service directory on the target DBMS server:  
   `mkdir /postgres/pg_expecto`
4. Copy the installer:  
   `cp /tmp/pg_expecto/pg_expecto_install.sh /postgres/pg_expecto/`
5. Change to the installation directory:  
   `cd /postgres/pg_expecto`
6. Make the installer script executable:  
   `chmod 750 pg_expecto_install.sh`
7. Run the installer:  
   `./pg_expecto_install.sh`

## Monitoring pg_expecto

To view the log, use:

```bash
tail -f /postgres/pg_expecto/sh/pg_expecto.log
```

## Usage

A detailed example of using pg_expecto v.7 with the DeepSeek neural network is available in the article (Russian):  
[PG_EXPECTO v.7 + DeepSeek: полный цикл диагностики производительности PostgreSQL — от нагрузочного тестирования до разбора инцидентов](https://dzen.ru/a/aaATukWy4T8iHW1t?share_to=link)

## Contacts

- Rinat Sungatullin: **kznalp@yandex.ru**
- [Dzen channel](https://dzen.ru/kznalp) : https://dzen.ru/kznalp
- [Telegram channel](https://t.me/pg_expecto) : https://t.me/pg_expecto
- [Max](https://max.ru/join/T8sCiETC85Tr4Dkh_nM362PVcCbGDLagF4RZKHf4Udg)

## Project Status

Current version: 8.1

## 📜 License

Starting from version 8.0, `pg_expecto` is distributed under the [Apache License 2.0](LICENSE).  
Previous versions (prior to 2026-04-12) are available under the MIT License.  
The full license text is available in the [LICENSE](LICENSE) file in the repository root.
