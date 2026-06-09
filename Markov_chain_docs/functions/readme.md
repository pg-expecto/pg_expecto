# Хранимые функции цепи Маркова

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-316192?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Версия](https://img.shields.io/badge/версия-10.1.4-blue)](https://github.com/your-repo/markov-chain)
[![Лицензия](https://img.shields.io/badge/лицензия-Apache%202.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)

## Общее описание

Данный набор PL/pgSQL функций реализует **онлайн‑обучение цепи Маркова** на потоке метрик производительности (`корреляция`, `тренд операционной скорости`, `тренд ожидания`). Модель автоматически обновляется каждую минуту, адаптивно забывает устаревшие данные и выдаёт краткосрочные прогнозы риска аварий (инцидентов). Основная функция `mchain_train_step()` должна вызываться **каждую минуту** (например, из процедуры `performance_metrics`). Средняя частота реальных инцидентов — **≈1 раз в день**, что используется для динамической настройки забывания.

Все функции имеют префикс `mchain_` и могут быть установлены выполнением скрипта `markov_chain_functions.sql`. Для работы требуются таблицы, созданные скриптом `markov_chain_tables.sql`.

---

## Содержание

- [Основные функции обучения](#основные-функции-обучения)
- [Функции адаптивного забывания](#функции-адаптивного-забывания)
- [Функции прогнозирования риска](#функции-прогнозирования-риска)
- [Функции очистки и обслуживания](#функции-очистки-и-обслуживания)
- [Вспомогательные функции](#вспомогательные-функции)
- [Функции оценки достоверности](#функции-оценки-достоверности)
- [Триггерная функция](#триггерная-функция)
- [Примеры вызовов](#примеры-вызовов)

---

## Основные функции обучения

### `mchain_train_step()`
**Назначение:** основной шаг онлайн-обучения. Вызывается **каждую минуту**.
**Возвращает:** `TEXT` — статус выполнения (`'Step completed'`, `'Initial state saved'`, `'No metrics available'` и т.д.).
**Алгоритм:**
1. Получает текущие метрики через `get_current_os_waiting_correlation_for_markov_chain()`.
2. Вычисляет `state_id` текущего состояния.
3. Читает предыдущее состояние из таблицы `markov_chain`.
4. Логирует переход в `transition_log` и обновляет `markov_frequencies` (вызов `mchain_log_transition`).
5. Обновляет строку в `markov_chain` (сдвиг состояний).
6. Если с момента последнего забывания прошло `interval_minute` минут (из `markov_config`), вызывает `mchain_apply_forgetting()`.

### `mchain_log_transition(p_from_state SMALLINT, p_to_state SMALLINT)`
**Назначение:** записывает переход в журнал и увеличивает частоту в `markov_frequencies`. Вызывается из `mchain_train_step`.
**Возвращает:** `VOID`.

### `update_markov_probabilities()`
**Назначение:** пересчитывает матрицу вероятностей `markov_probabilities` из сырых частот `markov_frequencies` (нормировка по строкам). Также вызывает `rebuild_markov_absorbing()`.
**Возвращает:** `VOID`.

### `rebuild_markov_absorbing()`
**Назначение:** строит поглощающую матрицу `markov_absorbing`:
- Для всех неаварийных состояний копируются переходы из `markov_probabilities`.
- Для аварийных состояний (`correlation < 0 AND os_trend = -1`) создаётся только петля с вероятностью 1.0.
**Возвращает:** `VOID`.

---

## Функции адаптивного забывания

### `mchain_apply_forgetting(alpha_override REAL DEFAULT NULL)`
**Назначение:** применяет забывание к накопленным частотам. Вызывается автоматически из `mchain_train_step` по интервалу или вручную.
**Алгоритм:**
- Проверяет `adaptive_forgetting_enabled` и достаточность данных (`mchain_check_sufficiency`).
- Вычисляет эффективный коэффициент `alpha`:
  - Если `alpha_override IS NOT NULL` — используется он.
  - Иначе если `use_adaptive_alpha = true`:
    - При `last_incident_time IS NULL` → `min_alpha`.
    - Иначе `days_since = (now() - last_incident_time)/86400`;
      `alpha = base_alpha * exp(-days_since / incident_half_life_days)`;
      `alpha = GREATEST(alpha, min_alpha)`.
  - Иначе — фиксированное `alpha` из `markov_config`.
- Выполняет: `UPDATE markov_frequencies SET frequency = frequency * (1 - alpha)`, затем удаляет записи с `frequency < 1e-6`.
- Пересчитывает вероятности (`update_markov_probabilities`).
- Обновляет `last_forget_time` и логирует вызов в `apply_forgetting_log`.
**Возвращает:** `VOID`.

### `mchain_check_sufficiency(min_transitions INT DEFAULT NULL, max_prob_change REAL DEFAULT 0.05, weeks_history INT DEFAULT 2)`
**Назначение:** проверяет, достаточно ли модель обучена для применения забывания.
**Критерии:**
- Общее число переходов в `transition_log` ≥ `min_transitions_for_forgetting` (из конфига, по умолчанию 5000).
- Если данных достаточно (`≥ 2×min_transitions`), оценивается максимальное изменение вероятностей за последние `weeks_history` недель. Если изменение > `max_prob_change` — возвращается `FALSE`.
**Возвращает:** `BOOLEAN`.

### `mchain_enable_forgetting_when_sufficient()`
**Назначение:** включает адаптивное забывание (`adaptive_forgetting_enabled = true`) **только** если `mchain_check_sufficiency()` вернула `TRUE`. Иначе возвращает сообщение об отказе.
**Возвращает:** `TEXT` — сообщение о результате.

### `mchain_force_enable_forgetting()`
**Назначение:** принудительно включает адаптивное забывание (без проверки достаточности).
**Возвращает:** `TEXT` — сообщение о включении.

---

## Функции прогнозирования риска

### `mchain_predict_risk_1min()`
**Возвращает:** `TABLE (risk REAL, curr_situation TEXT, curr_transitions_to_risk BIGINT, curr_total_transitions_known BIGINT)`
**Назначение:** прогноз вероятности аварии на следующей минуте (одношаговый) с диагностикой. Использует прямое суммирование вероятностей перехода в аварийные состояния.

### `mchain_predict_risk_k(k INT)`
**Назначение:** универсальная функция прогноза риска хотя бы одного попадания в аварию за `k` шагов (минут). Использует поглощающую цепь Маркова.
**Алгоритм:**
- Определяет текущее состояние.
- Если состояние неизвестно — априорная оценка `risk = 1 - (1-0.05)^k`.
- Иначе инициализирует вектор вероятностей длиной 189 и умножает его на матрицу `markov_absorbing` `k` раз.
- Риск = сумма вероятностей всех аварийных состояний.
**Возвращает:** такую же таблицу, как `mchain_predict_risk_1min`.

### `mchain_predict_risk_15min()`, `mchain_predict_risk_30min()`, `mchain_predict_risk_1hour()`
**Назначение:** обёртки над `mchain_predict_risk_k` с фиксированными `k` (15, 30, 60 соответственно).
**Возвращают:** ту же структуру.

---

## Функции очистки и обслуживания

Используются в **cron** для автоматического удаления старых записей. Обе функции имеют параметр `p_retention_days` (если не указан, берут значение из `markov_config`).

| Функция | Таблица | Retention по умолчанию | Cron пример |
|---------|---------|----------------------|--------------|
| `mchain_clean_transition_log(p_retention_days INT DEFAULT NULL)` | `transition_log` | 21 день | `15 1 * * *` |
| `mchain_clean_apply_forgetting_log(p_retention_days INT DEFAULT NULL)` | `apply_forgetting_log` | 21 день | `0 2 * * *` |

**Возвращают:** `TEXT` — количество удалённых строк.

---

## Вспомогательные функции

### `get_current_os_waiting_correlation_for_markov_chain()`
**Назначение:** возвращает текущие метрики за последний час из `cluster_stat_median`:
- `current_correlation REAL` — корреляция скорость‑ожидания.
- `current_os_trend SMALLINT` — знак наклона линии регрессии для скорости (−1,0,1).
- `current_wait_trend SMALLINT` — знак наклона для ожиданий.
**Используется внутри** `mchain_train_step` и прогнозных функций.

### `get_state_id(r REAL, os_trend SMALLINT, wait_trend SMALLINT)`
**Назначение:** отображает тройку метрик в целочисленный `state_id` (0..188). Функция **IMMUTABLE**.
**Возвращает:** `SMALLINT`.

### `fill_state_descriptions()`
**Назначение:** заполняет справочник `state_descriptions` всеми 189 комбинациями (`correlation` от −1.0 до +1.0 шагом 0.1, `os_trend` и `wait_trend` из −1,0,1). Вызывается автоматически при первом вызове `mchain_train_step`, если таблица пуста.
**Возвращает:** `VOID`.

### `mchain_get_current_state_id()`
**Назначение:** отладочная функция, возвращает `state_id` текущего состояния.
**Возвращает:** `SMALLINT` или `NULL`, если метрики недоступны.

### `mchain_log_error(p_function_name TEXT, p_error_message TEXT, p_error_detail TEXT, p_error_hint TEXT, p_context JSONB)`
**Назначение:** записывает ошибку в таблицу `mchain_error_log` и выводит предупреждение в журнал PostgreSQL. Используется во всех основных функциях для отказоустойчивости.
**Возвращает:** `VOID`.

---

## Функции оценки достоверности

### `mchain_forecast_reliability()`
**Назначение:** вычисляет интегральный рейтинг достоверности прогнозов от **0** (недостоверен) до **5** (максимально достоверен) на основе:
- Общего числа переходов (<100 → 0, 100–499 → 1, 500–4999 → 2, ≥5000 → 3 базы).
- Стабильности вероятностей (изменение <2% → бонус +2, <5% → +1).
- Покрытия частых состояний (≥90% → +1).
**Возвращает:** `INT`.

### `mchain_reliability_report()`
**Назначение:** возвращает подробный текстовый отчёт с метриками, пороговыми значениями и рекомендациями по улучшению достоверности модели.
**Возвращает:** `TEXT` (многострочный).

---

## Триггерная функция

### `update_last_incident_time()`
**Назначение:** автоматически обновляет поле `last_incident_time` в `markov_config` при каждом аварийном переходе (новой записи в `transition_log`, где `to_state` относится к аварийному состоянию). Привязана к триггеру `trigger_update_incident_time`.
**Возвращает:** `TRIGGER`.

---

## Примеры вызовов

### Минутное обучение (через cron или pgAgent)
```sql
SELECT mchain_train_step();
```

### Ручное применение забывания с переопределением alpha
```sql
SELECT mchain_apply_forgetting(0.05);
```

### Получение прогноза риска на 15 минут
```sql
SELECT * FROM mchain_predict_risk_15min();
```

### Проверка достаточности и включение забывания
```sql
SELECT mchain_enable_forgetting_when_sufficient();
```

### Очистка transition_log старше 30 дней
```sql
SELECT mchain_clean_transition_log(30);
```

### Генерация отчёта о достоверности
```sql
SELECT mchain_reliability_report();
```

---

## Примечание о cron

В файле `crontab.txt` приведены рекомендуемые задания для регулярного обслуживания (см. таблицу в разделе «Функции очистки»). Убедитесь, что путь к `psql` и параметры подключения настроены правильно.

---

## Лицензия

Apache License Version 2.0. Подробности в файле [LICENSE](https://github.com/pg-expecto/pg_expecto/blob/main/LICENSE).
