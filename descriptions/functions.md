## SQL файлы

| Список используемых файлов |
|----------------------------|
| core_cluster_functions.sql |
| core_functions.sql |
| core_os_functions.sql |
| core_statement_functions.sql |
| load_test_functions.sql |
| markov_chain_functions.sql |
| repors_queryid_stat.sql |
| report_iostat.sql |
| report_load_test_loading.sql |
| report_postgresql_cluster_performance.sql |
| report_postgresql_wait_event_type.sql |
| report_queryid_for_pareto.sql |
| report_shared_buffers.sql |
| report_sql_list.sql |
| report_vm_dirty.sql |
| report_vmstat.sql |
| report_vmstat_iostat.sql |
| report_vmstat_performance.sql |
| report_wait_event_type_for_pareto.sql |
| report_wait_event_type_vmstat.sql |
| stats_proсessing_functions.sql |

---

## Хранимые функции

| Наименование файла | Наименование функции | Краткое описание функции |
|--------------------|----------------------|---------------------------|
| core_cluster_functions.sql | cluster_stat_median | Очистка старых статистических данных. |
| core_cluster_functions.sql | performance_metrics | Рассчитать метрики оценки производительности. |
| core_cluster_functions.sql | stop_incidents | Завершить инциденты производительности. |
| core_cluster_functions.sql | start_incident | Начать инцидент производительности с заданным приоритетом. |
| core_functions.sql | default_configuration | Установить базовую конфигурацию. |
| core_functions.sql | set_day_for_store | Установить глубину хранения. |
| core_functions.sql | cleaning | Очистка старых статистических данных. |
| core_functions.sql | get_hour_before | Получить текстовую строку времени на час раньше. |
| core_os_functions.sql | os_stat_vmstat | Сформировать статистику по метрикам vmstat. |
| core_os_functions.sql | os_stat_iostat_device | Сформировать статистику по метрикам iostat. |
| core_statement_functions.sql | statement_stat | Собрать исходные данные в таблицу statement_stat. |
| core_statement_functions.sql | statement_stat_median | Агрегировать статистические данные за период. |
| core_statement_functions.sql | wait_event_jsonb | Сформировать jsonb по ожиданиям SQL запроса для заданного типа ожиданий. |
| core_statement_functions.sql | wait_queryid_jsonb | Сформировать jsonb по ожиданиям SQL запроса. |
| load_test_functions.sql | load_test_new_test | Начать новый тест. |
| load_test_functions.sql | load_test_set_testdb | Установить имя тестовой БД. |
| load_test_functions.sql | load_test_get_current_test_id | Получить id текущего теста. |
| load_test_functions.sql | load_test_get_current_test_pass_id | Получить id тестового прохода. |
| load_test_functions.sql | load_test_current_pass | Текущий проход. |
| load_test_functions.sql | load_test_get_load | Текущее количество подключений для pgbench. |
| load_test_functions.sql | load_test_set_load | Установить текущую нагрузку connections с учётом 3-часовой длительности и 10-минутных итераций. |
| load_test_functions.sql | load_test_get_load_by_scenario | Текущее количество подключений для pgbench для заданного сценария. |
| load_test_functions.sql | load_test_set_scenario_queryid | Установить queryid для сценариев. |
| load_test_functions.sql | load_test_get_start_timestamp | Получить время начала теста. |
| load_test_functions.sql | load_test_get_finish_timestamp | Получить время окончания теста. |
| load_test_functions.sql | load_test_set_start_load | Установить начальное количество подключений для pgbench. |
| load_test_functions.sql | load_test_set_max_load | Установить максимальное количество подключений для pgbench. |
| load_test_functions.sql | load_test_is_test_could_be_finished | Проверить, можно ли остановить тест. |
| load_test_functions.sql | load_test_start_collect_data | Начать сбор данных для статистики для текущей фазы теста. |
| load_test_functions.sql | load_test_stop_collect_data | Завершить сбор данных для статистики для текущей фазы теста. |
| load_test_functions.sql | load_test_has_the_first_hour_passed | Проверить, прошёл ли первый час работы. |
| load_test_functions.sql | load_test_increment_pass_counter | Увеличить счётчик итераций. |
| load_test_functions.sql | load_test_set_weight_for_scenario | Установить вес для тестового сценария. |
| load_test_functions.sql | save_dirty_background_ratio | Сохранить значение vm.dirty_background_ratio. |
| load_test_functions.sql | save_dirty_ratio | Сохранить значение vm.dirty_ratio. |
| load_test_functions.sql | save_dirty_background_bytes | Сохранить значение vm.dirty_background_bytes. |
| load_test_functions.sql | save_dirty_bytes | Сохранить значение vm.dirty_bytes. |
| load_test_functions.sql | save_dirty_expire_centisecs | Сохранить значение vm.dirty_expire_centisecs. |
| load_test_functions.sql | save_dirty_writeback_centisecs | Сохранить значение vm.dirty_writeback_centisecs. |
| load_test_functions.sql | save_vfs_cache_pressure | Сохранить значение vm.vfs_cache_pressure. |
| load_test_functions.sql | save_swappiness | Сохранить значение vm.swappiness. |
| load_test_functions.sql | get_vm_params_list | Получить список текущих параметров управления RAM. |
| load_test_functions.sql | poisson_random | Генерация случайного числа по распределению Пуассона. |
| load_test_functions.sql | load_test_poisson_session_count | Получить количество сессий для pgbench по Пуассону. |
| load_test_functions.sql | load_test_poisson_set_period_hours | Установить период для пуассоновского распределения. |
| load_test_functions.sql | load_test_poisson_set_average_load | Установить среднюю нагрузку для пуассоновского распределения. |
| markov_chain_functions.sql | get_current_os_waiting_correlation_for_markov_chain | Получить текущую корреляцию и тренды для цепи Маркова. |
| markov_chain_functions.sql | mchain_train_step | Одношаговое обучение цепи (вызов каждую минуту). |
| markov_chain_functions.sql | mchain_apply_forgetting | Применить забывание (адаптивное или по конфигурации). |
| markov_chain_functions.sql | mchain_check_sufficiency | Проверить достаточность обучения (объём данных + стабильность). |
| markov_chain_functions.sql | mchain_log_transition | Записать переход между состояниями и обновить частоты. |
| markov_chain_functions.sql | mchain_clean_transition_log | Очистить журнал переходов. |
| markov_chain_functions.sql | fill_state_descriptions | Заполнить справочник состояний цепи Маркова. |
| markov_chain_functions.sql | get_state_id | Получить числовой идентификатор состояния по параметрам. |
| markov_chain_functions.sql | rebuild_markov_absorbing | Перестроить таблицу поглощающей матрицы. |
| markov_chain_functions.sql | update_last_incident_time | Обновить время последнего инцидента (триггер). |
| markov_chain_functions.sql | update_markov_probabilities | Заполнить матрицу вероятностей из частот. |
| markov_chain_functions.sql | mchain_clean_apply_forgetting_log | Очистить журнал вызовов забывания. |
| markov_chain_functions.sql | mchain_log_error | Записать ошибку в журнал ошибок. |
| markov_chain_functions.sql | mchain_predict_risk_1min | Прогноз риска аварии на следующей минуте. |
| markov_chain_functions.sql | mchain_predict_risk_k | Прогноз риска аварии за k шагов (минут). |
| markov_chain_functions.sql | mchain_predict_risk_15min | Прогноз риска аварии на ближайшие 15 минут. |
| markov_chain_functions.sql | mchain_predict_risk_30min | Прогноз риска аварии на ближайшие 30 минут. |
| markov_chain_functions.sql | mchain_predict_risk_1hour | Прогноз риска аварии на ближайший час. |
| markov_chain_functions.sql | mchain_get_current_state_id | Вернуть идентификатор текущего состояния. |
| markov_chain_functions.sql | mchain_forecast_reliability | Оценить достоверность прогнозов от 0 до 5. |
| markov_chain_functions.sql | mchain_reliability_report | Вернуть развёрнутый отчёт о достоверности прогнозов. |
| markov_chain_functions.sql | mchain_enable_forgetting_when_sufficient | Включить адаптивное забывание при достаточности данных. |
| markov_chain_functions.sql | mchain_force_enable_forgetting | Принудительно включить адаптивное забывание. |
| repors_queryid_stat.sql | report_queryid_stat | История выполнения и ожиданий по отдельному SQL запросу. |
| report_iostat.sql | report_iostat | Данные для графиков по IOSTAT. |
| report_load_test_loading.sql | report_load_test_loading | График изменения нагрузки в ходе нагрузочного тестирования. |
| report_postgresql_cluster_performance.sql | report_postgresql_cluster_performance | Данные для построения графиков производительности и ожиданий СУБД. |
| report_postgresql_wait_event_type.sql | report_postgresql_wait_event_type | Корреляция ожиданий СУБД и vmstat. |
| report_queryid_for_pareto.sql | report_queryid_for_pareto | Диаграмма Парето по queryid. |
| report_shared_buffers.sql | report_shared_buffers | Статистика shared_buffers. |
| report_sql_list.sql | report_sql_list | Список SQL выражений за период. |
| report_vm_dirty.sql | report_vm_dirty | Статистика dirty_ratio / dirty_background_ratio. |
| report_vmstat.sql | report_vmstat | Данные для графиков по VMSTAT. |
| report_vmstat_iostat.sql | report_vmstat_iostat | Корреляция метрик vmstat и iostat. |
| report_vmstat_performance.sql | report_vmstat_performance | Статистика производительности vmstat. |
| report_wait_event_type_for_pareto.sql | report_wait_event_type_for_pareto | Диаграмма Парето по wait_event_type. |
| report_wait_event_type_vmstat.sql | report_wait_event_type_vmstat | Корреляция ожиданий СУБД и vmstat. |
| stats_proсessing_functions.sql | truncate_time_series | Быстрая очистка таблиц временных рядов. |
| stats_proсessing_functions.sql | quick_significance_check | Быстрая проверка значимости корреляции. |
| stats_proсessing_functions.sql | student_t_cdf | Вычисление CDF распределения Стьюдента (вспомогательная). |
| stats_proсessing_functions.sql | incomplete_beta | Неполная бета-функция (вспомогательная). |
| stats_proсessing_functions.sql | log_gamma | Логарифм гамма-функции (вспомогательная). |
| stats_proсessing_functions.sql | fill_corr_values_for_positive_corr | Заполнить значения корреляции для положительного коэффициента. |
| stats_proсessing_functions.sql | fill_corr_values_for_negative_corr | Заполнить значения корреляции для отрицательного коэффициента. |
| stats_proсessing_functions.sql | the_line_of_least_squares | Линия наименьших квадратов для регрессии Y = a + bt. |
| stats_proсessing_functions.sql | Y_X_regression_line | Линия наименьших квадратов для регрессии Y = a + bX. |
| stats_proсessing_functions.sql | interpretation_r2_coefficient | Интерпретация коэффициента детерминации. |
| stats_proсessing_functions.sql | interpretation_K_coefficient | Интерпретация коэффициента тренда. |
| stats_proсessing_functions.sql | fill_in_wce_activities | Заполнение таблицы activities. |
| stats_proсessing_functions.sql | get_wce_activities | Получить активности по типу ожидания и значению ВКО. |
| stats_proсessing_functions.sql | fill_in_comprehensive_analysis_wait_event_type | Заполнить данные комплексного анализа ожиданий. |
| stats_proсessing_functions.sql | fill_in_comprehensive_analysis_correlation | Заполнить данные комплексного анализа корреляции. |
| stats_proсessing_functions.sql | calculate_cpi_matrix | Вычислить индекс приоритета корреляции (CPI). |
| stats_proсessing_functions.sql | calc_wait_event_type_criteria_weight | Расчёт весов критериев для wait_event_type. |
| stats_proсessing_functions.sql | norm_wait_event_type_criteria_matrix | Нормализовать значения в матрице критериев. |
