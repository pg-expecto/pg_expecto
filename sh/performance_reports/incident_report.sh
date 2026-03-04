#!/bin/sh
########################################################################################################
# incident_report.sh
# Отчет по производительности СУБД и инфраструктуры
# version 7.1
########################################################################################################

#Обработать код возврата 
function exit_code {
ecode=$1
if [[ $ecode != 0 ]];
then
	ecode=$1
	LOG_FILE=$2
	ERR_FILE=$3
	
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR : Details in '$ERR_FILE
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR : Details in '$ERR_FILE >> $LOG_FILE
	
    exit $ecode
fi
}

script=$(readlink -f $0)
current_path=`dirname $script`


LOG_FILE=$current_path'/incident_report.log'
ERR_FILE=$current_path'/incident_report.err'
REPORT_DIR='/tmp/pg_expecto_reports'

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - НАЧАТ ' > $LOG_FILE


timestamp_label=$(date "+%Y%m%d")'T'$(date "+%H%M")

expecto_db='expecto_db'
expecto_user='expecto_user'

rm /tmp/pg_expecto_reports/*

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : СТАРТ ' > $LOG_FILE

if [ $# -ne 1 ]
then
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  summary_report :  ONLY INCIDENT TIME MUST BE SET '
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  summary_report :  ONLY INCIDENT TIME MUST BE SET ' >> $LOG_FILE
  exit 0
fi 


finish_incident_timestamp=$1
start_incindent_timestamp=`psql -Aqtc "select to_char(to_timestamp('$finish_incident_timestamp','YYYY-MM-DD HH24:MI') - interval '1 hour' , 'YYYY-MM-DD HH24:MI')"`
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : start_incindent_timestamp = '$start_incindent_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : start_incindent_timestamp = '$start_incindent_timestamp >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : finish_incident_timestamp = '$finish_incident_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : finish_incident_timestamp = '$finish_incident_timestamp >> $LOG_FILE

finish_test_timestamp=$start_incindent_timestamp
start_test_timestamp=`psql -Aqtc "select to_char(to_timestamp('$finish_test_timestamp','YYYY-MM-DD HH24:MI') - interval '1 hour' , 'YYYY-MM-DD HH24:MI')"`
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : start_test_timestamp = '$start_test_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : start_test_timestamp = '$start_test_timestamp >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : finish_test_timestamp = '$finish_test_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : finish_test_timestamp = '$finish_test_timestamp >> $LOG_FILE


devices_list=`$current_path'/'get_reports_param.sh $current_path devices_list 2>$ERR_FILE`
exit_code $? $LOG_FILE $ERR_FILE
  
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  summary_report : devices_list = '$devices_list
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : devices_list = '$devices_list >> $LOG_FILE	


######################################################################################################################################################
# ИНЦИДЕНТ
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ИНЦИДЕНТ: СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ИНЦИДЕНТ: СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - НАЧАТ' >> $LOG_FILE

$current_path'/'summary_report.sh "$start_incindent_timestamp" "$finish_incident_timestamp" 
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ИНЦИДЕНТ: СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ИНЦИДЕНТ: СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - ЗАКОНЧЕН' >> $LOG_FILE
# ИНЦИДЕНТ
######################################################################################################################################################

######################################################################################################################################################
# СРАВНИТЕЛЬНЫЙ ОТРЕЗОК
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ИНЦИДЕНТ: СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ИНЦИДЕНТ: СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - НАЧАТ' >> $LOG_FILE

$current_path'/'summary_report.sh "$start_test_timestamp" "$finish_test_timestamp" 'TEST'
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ИНЦИДЕНТ: СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ИНЦИДЕНТ: СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - ЗАКОНЧЕН' >> $LOG_FILE
# СРАВНИТЕЛЬНЫЙ ОТРЕЗОК
######################################################################################################################################################



echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1. НАСТРОЙКИ СУБД и VM'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1. НАСТРОЙКИ СУБД и VM' >> $LOG_FILE
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR

devices_list=`$current_path'/'get_reports_param.sh $current_path devices_list 2>$ERR_FILE`
exit_code $? $LOG_FILE $ERR_FILE
array=($devices_list)

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - НАЧАТО'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - НАЧАТО' >> $LOG_FILE

##########################################################################################
# НАСТРОЙКИ
REPORT_FILE='_1.settings.txt'
echo 'НАСТРОЙКИ СУБД и VM' > $REPORT_FILE 
#файл postgresql.auto.conf
psql -c 'select version()' >> $REPORT_FILE 
data_directory=`psql -Aqtc  'SHOW data_directory'`
postgresql_auto_conf=$data_directory'/postgresql.auto.conf'
#cat $postgresql_auto_conf >> $REPORT_FILE
grep -vwE "(log_filename)" $postgresql_auto_conf >> $REPORT_FILE
echo ' ' >> $REPORT_FILE

#количество ядер CPU
lscpu >>  $REPORT_FILE
echo ' ' >> $REPORT_FILE

#размер RAM
ram=`free -b | awk '/^Mem:/ {printf "%.2f GB\n", $2/1024/1024/1024}'`
echo 'RAM = '$ram >> $REPORT_FILE
echo ' ' >> $REPORT_FILE

#IO
lsblk >> $REPORT_FILE
echo 'devices = '$devices_list >> $REPORT_FILE
echo ' ' >> $REPORT_FILE

#VM
#################################################################################
# ПАРАМЕТРЫ vm
  psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( get_vm_params_list())" >> $REPORT_FILE 2>$ERR_FILE
  if [ $? -ne 0 ]
  then
	echo 'ERROR : queryid_stat TERMINATED WITH ERROR. SEE DETAILS IN '$ERR_FILE
	echo 'ERROR : queryid_stat TERMINATED WITH ERROR. SEE DETAILS IN '$ERR_FILE >> $LOG_FILE
	exit 100
  fi
# ПАРАМЕТРЫ vm
#################################################################################
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
echo 'МЕТОДОЛОГИЯ СТАТИСТИЧЕСКОГО АНАЛИЗА PG_EXPECTO' >> $REPORT_FILE
echo 'МЕТОДИКА 3-Х ЭТАПНОГО СТАТИСТИЧЕСКОГО АНАЛИЗА ОЖИДАНИЙ СУБД'>> $REPORT_FILE
echo 'Этап-1.СТАТИСТИЧЕСКАЯ ЗНАЧИМОСТЬ КОЭФФИЦИЕНТА КОРРЕЛЯЦИИ:'>> $REPORT_FILE
echo '  p-value < 0.05 — корреляция считается статистически значимой. Анализ целесообразен.'>> $REPORT_FILE
echo '  p-value >= 0.05 — связь нестабильна и может быть случайной. Интерпретация силы корреляции неприменима.'>> $REPORT_FILE
echo ' '>> $REPORT_FILE
echo 'Этап-2.ВЗВЕШЕННАЯ КОРРЕЛЯЦИЯ ОЖИДАНИЙ (ВКО)'>> $REPORT_FILE
echo '  Аналитическая метрика, предназначенная для ранжирования типов событий ожидания.'>> $REPORT_FILE
echo '  по степени их влияния на общую нагрузку системы.'>> $REPORT_FILE
echo '  Чем выше значение ВКО, тем критичнее проблема.'>> $REPORT_FILE
echo '  >= 0.2         КРИТИЧЕСКОЕ ЗНАЧЕНИЕ : Немедленный анализ и действие. '>> $REPORT_FILE
echo '  [0.1 ; 0.2[    ВЫСОКОЕ ЗНАЧЕНИЕ : Глубокий анализ и планирование оптимизации.'>> $REPORT_FILE
echo '  [0.04 ; 0.1[   СРЕДНЕЕ ЗНАЧЕНИЕ : Контекстный анализ и наблюдение. '>> $REPORT_FILE
echo '  [0.01 ; 0.04[ НИЗКОЕ ЗНАЧЕНИЕ : Наблюдение и документирование.'>> $REPORT_FILE
echo '  < 0.01 Игнорировать в текущем анализе.'>> $REPORT_FILE
echo ' '
echo 'Этап-3.ИНТЕРПРЕТАЦИЯ КОЭФФИЦИЕНТА ДЕТЕРМИНАЦИИ R2'>> $REPORT_FILE
echo '  >= 0.8      — Исключительно сильная модель.'>> $REPORT_FILE
echo '  [0.6 ; 0.8[ — Качественная модель.'>> $REPORT_FILE
echo '  [0.4 ; 0.6[ — Приемлемая модель (средняя).'>> $REPORT_FILE
echo '  [0.2 ; 0.4[ — Слабая модель.'>> $REPORT_FILE
echo '   < 0.2      — Непригодная модель.'>> $REPORT_FILE
echo '   '>> $REPORT_FILE
echo 'АНАЛИЗ ОЖИДАНИЙ(wait_event_type) '>> $REPORT_FILE
echo 'Этап-1. Интерпретация корреляций.'>> $REPORT_FILE
echo '  Отбросить невалидные значения (p-value > 0.05) : связь нестабильна и может быть случайной.'>> $REPORT_FILE
echo 'Этап-2. Интерпретация ВКО.'>> $REPORT_FILE
echo '  Отбросить значения, если ВКО < 0.01 : Игнорировать в текущем анализе.'>> $REPORT_FILE
echo 'Этап-3. Интерпретация коэффициента детерминации R2.'>> $REPORT_FILE
echo '  Отбросить значения, если R2 < 0.2 :Непригодная модель'>> $REPORT_FILE
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
echo 'МЕТОДИКА 2-Х ЭТАПНОГО СТАТИСТИЧЕСКОГО АНАЛИЗА МЕТРИК'>> $REPORT_FILE
echo 'Этап-1.СТАТИСТИЧЕСКАЯ ЗНАЧИМОСТЬ КОЭФФИЦИЕНТА КОРРЕЛЯЦИИ:'>> $REPORT_FILE
echo '  p-value < 0.05 — корреляция считается статистически значимой. Анализ целесообразен.'>> $REPORT_FILE
echo '  p-value >= 0.05 — связь нестабильна и может быть случайной. Интерпретация силы корреляции неприменима.'>> $REPORT_FILE
echo ' '>> $REPORT_FILE
echo ' '
echo 'Этап-2.ИНТЕРПРЕТАЦИЯ КОЭФФИЦИЕНТА ДЕТЕРМИНАЦИИ R2'>> $REPORT_FILE
echo '  >= 0.8      — Исключительно сильная модель.'>> $REPORT_FILE
echo '  [0.6 ; 0.8[ — Качественная модель.'>> $REPORT_FILE
echo '  [0.4 ; 0.6[ — Приемлемая модель (средняя).'>> $REPORT_FILE
echo '  [0.2 ; 0.4[ — Слабая модель.'>> $REPORT_FILE
echo '   < 0.2      — Непригодная модель.'>> $REPORT_FILE
echo '   '>> $REPORT_FILE
echo 'АНАЛИЗ КОРРЕЛЯЦИ МЕЖДУ МЕТРИКАМИ '>> $REPORT_FILE
echo 'Этап-1. Интерпретация корреляций.'>> $REPORT_FILE
echo '  Отбросить невалидные значения (p-value > 0.05) : связь нестабильна и может быть случайной.'>> $REPORT_FILE
echo 'Этап-2. Интерпретация коэффициента детерминации R2.'>> $REPORT_FILE
echo '  Отбросить значения, если R2 < 0.2 :Непригодная модель'>> $REPORT_FILE
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
# НАСТРОЙКИ
##########################################################################################

##########################################################################################
# ИНЦИДЕНТ: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1.ИНЦИДЕНТ: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1.ИНЦИДЕНТ: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT' >> $LOG_FILE

REPORT_FILE='_2.postgresql_vmstat.txt'
echo 'ИНЦИДЕНТ ПРОИЗВОДИТЕЛЬНОСТИ СУБД: КОМПЛЕКСНЫЙ КОРРЕЛЯЦИОННЫЙ АНАЛИЗ СУБД и VMSTAT' > $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
cat '1.1.postgresql.wait_event_type.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
cat '1.2.vmstat.performance.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
cat '1.3.wait_event_type_vmstat.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
cat '1.4.wait_event_type_pareto.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
cat '1.5.queryid_pareto.txt' >> $REPORT_FILE 
# ИНЦИДЕНТ: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT
##########################################################################################

##########################################################################################
# ТЕСТОВЫЙ ОТРЕЗОК: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.ТЕСТОВЫЙ ОТРЕЗОК: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.ТЕСТОВЫЙ ОТРЕЗОК: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT' >> $LOG_FILE

REPORT_FILE='_2.1.test.postgresql_vmstat.txt'
echo 'ТЕСТОВЫЙ ОТРЕЗОК ПРОИЗВОДИТЕЛЬНОСТИ СУБД: КОМПЛЕКСНЫЙ КОРРЕЛЯЦИОННЫЙ АНАЛИЗ СУБД и VMSTAT' > $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
cat '1.1.1.test.postgresql.wait_event_type.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
cat '1.2.1.test.vmstat.performance.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
cat '1.3.1.test.wait_event_type_vmstat.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
cat '1.4.1.test.wait_event_type_pareto.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
cat '1.5.1.test.queryid_pareto.txt' >> $REPORT_FILE 
# ТЕСТОВЫЙ ОТРЕЗОК: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT
##########################################################################################



##########################################################################################
# ПРОМПТЫ
######################################################################################################
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : PROMPT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : PROMPT' >> $LOG_FILE

REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR
REPORT_FILE='_3.1.prompt.txt'  
echo 'Сформируй сводный отчет по производительности СУБД и инфраструктуры.' > $REPORT_FILE
echo 'Для формирования отчета используй списки, вместо таблиц.' >> $REPORT_FILE 
echo 'Состав отчета: ' >> $REPORT_FILE 
echo '# Общая информация' >> $REPORT_FILE 
echo '# Общий анализ операционной скорости и ожиданий СУБД' >> $REPORT_FILE 
echo '## Граничные значение операционной скорости (SPEED) и ожиданий СУБД(WAITINGS)' >> $REPORT_FILE 
echo '## Анализ трендов операционной скорости (SPEED) и ожиданий СУБД(WAITINGS)' >> $REPORT_FILE 
echo '## 1. СТАТИСТИЧЕСКИЙ АНАЛИЗ ОЖИДАНИЙ СУБД' >> $REPORT_FILE 
echo '### Итог по разделу "1. СТАТИСТИЧЕСКИЙ АНАЛИЗ ОЖИДАНИЙ СУБД"' >> $REPORT_FILE 
echo '## 2. ТРЕНДОВЫЙ АНАЛИЗ ПРОИЗВОДИТЕЛЬНОСТИ vmstat' >> $REPORT_FILE 
echo '### Итог по разделу "2. ТРЕНДОВЫЙ АНАЛИЗ ПРОИЗВОДИТЕЛЬНОСТИ vmstat"' >> $REPORT_FILE 
echo '## 3. СТАТИСТИЧЕСКИЙ АНАЛИЗ ОЖИДАНИЙ СУБД и МЕТРИК vmstat' >> $REPORT_FILE 
echo '### Итог по разделу "3. СТАТИСТИЧЕСКИЙ АНАЛИЗ ОЖИДАНИЙ СУБД и МЕТРИК vmstat"' >> $REPORT_FILE 
echo '## 4. ДИАГРАММЫ ПАРЕТО ПО WAIT_EVENT_TYPE и QUERYID' >> $REPORT_FILE 
echo '### Итог по разделу "4. ДИАГРАММЫ ПАРЕТО ПО WAIT_EVENT_TYPE и QUERYID"' >> $REPORT_FILE 
echo '# Детальный анализ – граничные значения и корреляции' >> $REPORT_FILE 
echo '## Ожидания СУБД' >> $REPORT_FILE 
echo '## Память и буферный кэш' >> $REPORT_FILE 
echo '## Дисковая подсистема (I/O)' >> $REPORT_FILE 
echo '## CPU и системные вызовы' >> $REPORT_FILE 
echo '##  Блокировки и ожидания LWLock' >> $REPORT_FILE 
echo '##  Анализ запросов (queryid)' >> $REPORT_FILE 
echo '# Ключевые проблемы' >> $REPORT_FILE 
echo '##  Проблемы СУБД ' >> $REPORT_FILE 
echo '##  Проблемы инфраструктуры' >> $REPORT_FILE 
echo '# Рекомендации' >> $REPORT_FILE 
echo '## Рекомендации по настройкам СУБД' >> $REPORT_FILE 
echo '## Рекомендации по настройкам операционной системы' >> $REPORT_FILE 
echo '# Заключение ' >> $REPORT_FILE 

##########################################################################################
# СРАВНИТЕЛЬНЫЙ ОТЧЕТ ПО ИНЦИДЕНТУ И ТЕСТОВОМУ ОТРЕЗКУ 
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR
REPORT_FILE='_3.3.prompt.diff.txt'  
echo 'Сформируй сводный сравнительный отчет по производительности СУБД и инфраструктуры:' > $REPORT_FILE
echo '_2.postgresql_vmstat.txt - ИНЦИДЕНТ ПРОИЗВОДИТЕЛЬНОСТИ СУБД' >> $REPORT_FILE
echo '_2.1.test.postgresql_vmstat.txt - ТЕСТОВЫЙ ОТРЕЗОК ПРОИЗВОДИТЕЛЬНОСТИ СУБД' >> $REPORT_FILE
echo 'Для формирования отчета используй списки, вместо таблиц. ' >> $REPORT_FILE
echo 'Состав отчета: ' >> $REPORT_FILE
echo '# Общая информация' >> $REPORT_FILE
echo '# Общий анализ операционной скорости и ожиданий СУБД для "ИНЦИДЕНТ ПРОИЗВОДИТЕЛЬНОСТИ СУБД" и "ТЕСТОВЫЙ ОТРЕЗОК ПРОИЗВОДИТЕЛЬНОСТИ СУБД"' >> $REPORT_FILE
echo '## Сравнительный анализ граничных значений операционной скорости (SPEED) и ожиданий СУБД(WAITINGS)' >> $REPORT_FILE
echo '## Сравнительный анализ трендов операционной скорости (SPEED) и ожиданий СУБД(WAITINGS)' >> $REPORT_FILE
echo '## 1. СРАВНИТЕЛЬНЫЙ СТАТИСТИЧЕСКИЙ АНАЛИЗ ОЖИДАНИЙ СУБД' >> $REPORT_FILE
echo '### Итог по разделу "1. СРАВНИТЕЛЬНЫЙ СТАТИСТИЧЕСКИЙ АНАЛИЗ ОЖИДАНИЙ СУБД"' >> $REPORT_FILE
echo '## 2. СРАВНИТЕЛЬНЫЙ ТРЕНДОВЫЙ АНАЛИЗ ПРОИЗВОДИТЕЛЬНОСТИ vmstat' >> $REPORT_FILE
echo '### Итог по разделу "2. СРАВНИТЕЛЬНЫЙ ТРЕНДОВЫЙ АНАЛИЗ ПРОИЗВОДИТЕЛЬНОСТИ vmstat"' >> $REPORT_FILE
echo '## 3. СРАВНИТЕЛЬНЫЙ  СТАТИСТИЧЕСКИЙ АНАЛИЗ ОЖИДАНИЙ СУБД и МЕТРИК vmstat' >> $REPORT_FILE
echo '### Итог по разделу "3. СРАВНИТЕЛЬНЫЙ  СТАТИСТИЧЕСКИЙ АНАЛИЗ ОЖИДАНИЙ СУБД и МЕТРИК vmstat"' >> $REPORT_FILE
echo '## 4. СРАВНЕНИЕ ДИАГРАММ ПАРЕТО ПО WAIT_EVENT_TYPE и QUERYID' >> $REPORT_FILE
echo '### Итог по разделу "4. СРАВНЕНИЕ ДИАГРАММ ПАРЕТО ПО WAIT_EVENT_TYPE и QUERYID' >> $REPORT_FILE
echo '# Детальный анализ – граничные значения и корреляции для "ИНЦИДЕНТ ПРОИЗВОДИТЕЛЬНОСТИ СУБД" и "ТЕСТОВЫЙ ОТРЕЗОК ПРОИЗВОДИТЕЛЬНОСТИ СУБД"' >> $REPORT_FILE
echo '## Ожидания СУБД' >> $REPORT_FILE
echo '## Память и буферный кэш' >> $REPORT_FILE
echo '## Дисковая подсистема (I/O)' >> $REPORT_FILE
echo '## CPU и системные вызовы' >> $REPORT_FILE
echo '##  Блокировки и ожидания LWLock' >> $REPORT_FILE
echo '##  Анализ запросов (queryid)' >> $REPORT_FILE
echo '# Ключевые проблемы для "ИНЦИДЕНТ ПРОИЗВОДИТЕЛЬНОСТИ СУБД" и "ТЕСТОВЫЙ ОТРЕЗОК ПРОИЗВОДИТЕЛЬНОСТИ СУБД"' >> $REPORT_FILE
echo '##  Проблемы СУБД ' >> $REPORT_FILE
echo '##  Проблемы инфраструктуры' >> $REPORT_FILE

##########################################################################################
# КОРОТКИЙ СРАВНИТЕЛЬНЫЙ ОТЧЕТ ПО ИНЦИДЕНТУ И ТЕСТОВОМУ ОТРЕЗКУ 
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR
REPORT_FILE='_3.2.prompt.short.diff.txt'  
echo 'Сформируй краткий сравнительный отчет по производительности СУБД и инфраструктуры:' > $REPORT_FILE
echo '_2.postgresql_vmstat.txt - ИНЦИДЕНТ ПРОИЗВОДИТЕЛЬНОСТИ СУБД' >> $REPORT_FILE
echo '_2.1.test.postgresql_vmstat.txt - ТЕСТОВЫЙ ОТРЕЗОК ПРОИЗВОДИТЕЛЬНОСТИ СУБД' >> $REPORT_FILE
echo 'Для формирования отчета используй списки, вместо таблиц.' >> $REPORT_FILE 
echo 'Состав отчета:' >> $REPORT_FILE 
echo '# Существенные различия метрик производительностим СУБД и инфраструктуры' >> $REPORT_FILE
echo '# Сравнительный статистический анализ(сравнение коэффициентов корреляции и регрессии) метрик производительностим СУБД и инфраструктуры' >> $REPORT_FILE
echo '# Наиболее вероятная причина снижения производительности СУБД в ходе инцидента' >> $REPORT_FILE

##########################################################################################
# РЕКОМЕНДАЦИИПО ИНЦИДЕНТУ И ТЕСТОВОМУ ОТРЕЗКУ 
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR
REPORT_FILE='_3.4.prompt.diff.advice.txt'  
echo 'Сформируй рекомендации по итогам анализа инцидента производительности СУБД и инфраструктуры:' > $REPORT_FILE
echo '_2.postgresql_vmstat.txt - ИНЦИДЕНТ ПРОИЗВОДИТЕЛЬНОСТИ СУБД' >> $REPORT_FILE
echo '_2.1.test.postgresql_vmstat.txt - ТЕСТОВЫЙ ОТРЕЗОК ПРОИЗВОДИТЕЛЬНОСТИ СУБД' >> $REPORT_FILE
echo 'Для формирования отчета используй списки, вместо таблиц.' >> $REPORT_FILE 
echo 'Состав отчета:' >> $REPORT_FILE 
echo '# Ключевые проблемы для "ИНЦИДЕНТ ПРОИЗВОДИТЕЛЬНОСТИ СУБД" и "ТЕСТОВЫЙ ОТРЕЗОК ПРОИЗВОДИТЕЛЬНОСТИ СУБД"' >> $REPORT_FILE
echo '##  Проблемы СУБД ' >> $REPORT_FILE
echo '##  Проблемы инфраструктуры' >> $REPORT_FILE
echo '# Рекомендации по итогам анализа инцидента' >> $REPORT_FILE
echo '##  Рекомендации по оптимизации СУБД ' >> $REPORT_FILE
echo '##  Рекомендации по оптимизации инфраструктуры' >> $REPORT_FILE




##########################################################################################
# СТАТИСТИКА IOSTAT
######################################################################################################
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.СТАТИСТИКА IOSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.СТАТИСТИКА IOSTAT' >> $LOG_FILE
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR

let i=0
while :
do  
  device=${array[$i]}
  size=${#device}
	
  if [ "$size" == 0 ];
  then 
   break
  fi 
  
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  DEVICE =  '$device
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  DEVICE =  '$device >> $LOG_FILE
  REPORT_FILE='_iostat_'$device'.txt'  
  echo 'СТАТИСТИКА IOSTAT. device = '$device > $REPORT_FILE
  
  CURRENT_REPORT_FILE='2.1.vmstat_iostat_'$device'.txt'
  cat $CURRENT_REPORT_FILE > $REPORT_FILE 
  
  let i=i+1
done


# СТАТИСТИКА IOSTAT
##########################################################################################







echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - ЗАКОНЧЕНО'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - ЗАКОНЧЕНО' >> $LOG_FILE


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - ВЫПОЛНЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - ВЫПОЛНЕН' >> $LOG_FILE

exit 0 


