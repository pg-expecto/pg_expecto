#!/bin/sh
########################################################################################################
# load_test_report.sh
# Отчет по нагрузочному тестированию
# version 7.4.2
# updated 31/03/2026
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


LOG_FILE=$current_path'/load_test_report.log'
ERR_FILE=$current_path'/load_test_report.err'
REPORT_DIR='/tmp/pg_expecto_reports'

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - НАЧАТ ' > $LOG_FILE


timestamp_label=$(date "+%Y%m%d")'T'$(date "+%H%M")

expecto_db='expecto_db'
expecto_user='expecto_user'

#rm $REPORT_DIR'/*'
rm /tmp/pg_expecto_reports/*

current_test_id=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT load_test_get_current_test_id()"` 2>$ERR_FILE
echo 'current_test_id = '$current_test_id

start_timestamp=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT TO_CHAR(MIN(p.start_timestamp),'YYYY-MM-DD HH24:MI') FROM   load_test_pass p WHERE  p.test_id = $current_test_id AND p.pass_counter >= 6"` 2>$ERR_FILE
echo 'start_timestamp = '$start_timestamp

finish_timestamp=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT TO_CHAR(MAX(p.finish_timestamp),'YYYY-MM-DD HH24:MI') FROM   load_test_pass p WHERE  p.test_id = $current_test_id AND p.pass_counter >= 6"` 2>$ERR_FILE
echo 'finish_timestamp = '$finish_timestamp
 



echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : start_timestamp = '$start_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : start_timestamp = '$start_timestamp >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : finish_timestamp = '$finish_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : finish_timestamp = '$finish_timestamp >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ГРАФИК ИЗМЕНЕНИЯ НАГРУЗКИ В ХОДЕ ТЕСТИРОВАНИЯ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ГРАФИК ИЗМЕНЕНИЯ НАГРУЗКИ В ХОДЕ ТЕСТИРОВАНИЯ' >> $LOG_FILE
REPORT_FILE='x.load_test_loading.txt'
psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_load_test_loading())" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR
reports_load_test_loading=$REPORT_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE



echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - НАЧАТ' >> $LOG_FILE

$current_path'/'summary_report.sh "$start_timestamp" "$finish_timestamp" 
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  СВОДНЫЙ ОТЧЕТ ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД И МЕТРИК ОС - ЗАКОНЧЕН' >> $LOG_FILE

##################################################################################################################################
# SCENARIO REPORT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОТЧЕТ ПО SQL СЦЕНАРИЯМ - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОТЧЕТ ПО SQL СЦЕНАРИЯМ - НАЧАТ ' >> $LOG_FILE

current_test_id=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT load_test_get_current_test_id()"` 2>$ERR_FILE
max_sc_count=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT count(id) FROM testing_scenarios WHERE test_id = $current_test_id"` 2>$ERR_FILE

for ((sc_count=1; sc_count <= $max_sc_count; sc_count++ )) 
do 
	queryid=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT queryid FROM testing_scenarios WHERE id = $sc_count AND  test_id = $current_test_id"` 2>$ERR_FILE
		
	#####################################################################################################
	## ОЖИДАНИЯ ПО queryid
	for wait_event_type in 'BufferPin' 'Extension' 'IO' 'IPC' 'Lock' 'LWLock' 'Timeout'
	do 
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СЦЕНАРИЙ-'$sc_count' WAIT_EVENT_TYPE='$wait_event_type
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : СЦЕНАРИЙ-'$sc_count' WAIT_EVENT_TYPE='$wait_event_type >> $LOG_FILE
	  
	  REPORT_FILE=$current_path'/scenario.'$sc_count'.'$queryid'.'$wait_event_type'.txt'
	  
	  psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_queryid_stat("$queryid" , '$wait_event_type' , '$start_timestamp' , '$finish_timestamp'))" > $REPORT_FILE 2>$ERR_FILE
	  if [ $? -ne 0 ]
	  then
		echo 'ERROR : queryid_stat TERMINATED WITH ERROR. SEE DETAILS IN '$ERR_FILE
		echo 'ERROR : queryid_stat TERMINATED WITH ERROR. SEE DETAILS IN '$ERR_FILE >> $LOG_FILE
		exit 100
	  fi

		chmod 777 $REPORT_FILE
		mv $REPORT_FILE $REPORT_DIR

		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE
	done  
	## ОЖИДАНИЯ ПО queryid
	#####################################################################################################
	
done

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОТЧЕТ ПО SQL СЦЕНАРИЯМ - ЗАКОНЧЕН '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ОТЧЕТ ПО SQL СЦЕНАРИЯМ - ЗАКОНЧЕН ' >> $LOG_FILE
# SCENARIO REPORT
##################################################################################################################################

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK ' >> $LOG_FILE


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1. НАСТРОЙКИ СУБД и VM'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1. НАСТРОЙКИ СУБД и VM' >> $LOG_FILE
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR

devices_list=`$current_path'/'get_reports_param.sh $current_path devices_list 2>$ERR_FILE`
exit_code $? $LOG_FILE $ERR_FILE
array=($devices_list)



echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - НАЧАТО'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - НАЧАТО' >> $LOG_FILE



####################################################################################################################################################################################
# ВХОДНЫЕ ДАННЫЕ ДЛЯ НЕЙРОСЕТИ
##########################################################################################
# 1.НАСТРОЙКИ
REPORT_FILE='_1.settings.txt'
echo 'НАСТРОЙКИ СУБД и VM' > $REPORT_FILE 
#файл postgresql.auto.conf
psql -c 'select version()' >> $REPORT_FILE 
#data_directory=`psql -Aqtc  'SHOW data_directory'`
#postgresql_auto_conf=$data_directory'/postgresql.auto.conf'
##cat $postgresql_auto_conf >> $REPORT_FILE
#grep -vwE "(log_filename)" $postgresql_auto_conf >> $REPORT_FILE
psql -Aqtc "select name , setting from pg_settings where not pending_restart and name != 'log_filename'" >> $REPORT_FILE
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
# 2.СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT
##########################################################################################
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1.СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1.СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT' >> $LOG_FILE

REPORT_FILE='_2.postgresql_vmstat.txt'
echo 'КОМПЛЕКСНЫЙ КОРРЕЛЯЦИОННЫЙ АНАЛИЗ СУБД и VMSTAT' > $REPORT_FILE 
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
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
cat 'x.sql_list.txt' >> $REPORT_FILE 
# СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ
##########################################################################################

##########################################################################################
# 3.СТАТИСТИКА VMSTAT - IOSTAT
##########################################################################################
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.СТАТИСТИКА IOSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.СТАТИСТИКА IOSTAT' >> $LOG_FILE
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR

  REPORT_FILE='_3.vmstat_iostat.txt'  
  echo 'КОМПЛЕКСНЫЙ КОРРЕЛЯЦИОННЫЙ АНАЛИЗ МЕТРИК VMSTAT-IOSTAT' > $REPORT_FILE
  echo '-------------------------------------------------------------------------' >> $REPORT_FILE

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
  
  CURRENT_REPORT_FILE='2.1.vmstat_iostat_'$device'.txt'
  cat $CURRENT_REPORT_FILE >> $REPORT_FILE 
  echo '-------------------------------------------------------------------------' >> $REPORT_FILE
  
  let i=i+1
done
# СТАТИСТИКА VMSTAT - IOSTAT
##########################################################################################



##########################################################################################
# ПРОМПТЫ
######################################################################################################
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : PROMPT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : PROMPT' >> $LOG_FILE

# СВОДНЫЙ АНАЛИЗ 
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR
REPORT_FILE='_4.1.performance_prompt.txt'  
echo 'Сформируй сводный отчет по производительности СУБД и инфраструктуры.' > $REPORT_FILE
echo 'Ты — эксперт по производительности СУБД PostgreSQL.' >> $REPORT_FILE  
echo 'Твоя задача — анализировать статистические данные (метрики, логи, выводы из pg_stat_database, pg_stat_statements, системные показатели) и давать точный, предметный анализ метрик и корреляций.' >> $REPORT_FILE  
echo 'Правила:' >> $REPORT_FILE  
echo '1. Отвечай строго на основе предоставленных данных. Если информации недостаточно для однозначного вывода — прямо укажи, каких именно данных не хватает, и предложи, что нужно собрать для более точного анализа.' >> $REPORT_FILE  
echo '2. Не придумывай метрики, значения или причины. Не используй общие фразы без подтверждения цифрами.' >> $REPORT_FILE  
echo '3. Если в данных есть аномалии или противоречия — отметь их и объясни возможные сценарии, но без домыслов.' >> $REPORT_FILE  
echo '4. Ответ должен быть структурирован:' >> $REPORT_FILE  
echo '   - Краткое резюме (основные выводы).' >> $REPORT_FILE  
echo '   - Детальный анализ по ключевым метрикам (нагрузка на CPU/IO, использование памяти, блокировки, медленные запросы, эффективность кэша, параметры конфигурации).' >> $REPORT_FILE  
echo '   - Если данных недостаточно — перечень необходимых дополнительных метрик или срезов.' >> $REPORT_FILE  
echo '5. Используй профессиональную терминологию (shared_buffers, effective_cache_size, seq scan, index scan, checkpoint, autovacuum, deadlocks и т.п.). При ссылке на параметры указывай их единицы измерения.' >> $REPORT_FILE  
echo '6. Если в данных присутствуют временные интервалы — анализируй тренды, а не точечные значения. Указывай период наблюдения.' >> $REPORT_FILE  
echo '7. Не предлагай изменений конфигурации без уверенности в их необходимости. Если сомневаешься — предложи провести дополнительную диагностику.' >> $REPORT_FILE  
echo '8.Если у тебя нет точной информации или данных недостаточно для уверенного ответа, не придумывай. Скажи:Недостаточно данных для ответа.' >> $REPORT_FILE  
echo '9.Даже если таблицы нагляднее — используй только списки.' >> $REPORT_FILE  
echo '10.Исключи из отчета рекомендации, только анализ.' >> $REPORT_FILE  
echo 'Стиль: деловой, технически точный, без лишних пояснений.' >> $REPORT_FILE    
echo 'Если пользователь не предоставил сами данные, а только вопрос — запроси конкретные метрики и период наблюдения.' >> $REPORT_FILE  
echo 'Сформируй сводный отчет по производительности СУБД и инфраструктуры.' >> $REPORT_FILE  
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
echo '# Заключение ' >> $REPORT_FILE  

# РАЗНОСТНЫЙ АНАЛИЗ ДЛЯ СРАВНЕНИЯ ЭКСПЕРИМЕНТОВ
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR
REPORT_FILE='_4.2.performance_prompt.diff.txt'  
echo 'Сформируй сводный сравнительный отчет по производительности СУБД и инфраструктуры' > $REPORT_FILE
echo 'Ты — эксперт по производительности СУБД PostgreSQL.' >> $REPORT_FILE  
echo 'Твоя задача — анализировать статистические данные (метрики, логи, выводы из pg_stat_database, pg_stat_statements, системные показатели) и давать точный, предметный анализ метрик и корреляций.' >> $REPORT_FILE  
echo 'Правила:' >> $REPORT_FILE  
echo '1. Отвечай строго на основе предоставленных данных. Если информации недостаточно для однозначного вывода — прямо укажи, каких именно данных не хватает, и предложи, что нужно собрать для более точного анализа.' >> $REPORT_FILE  
echo '2. Не придумывай метрики, значения или причины. Не используй общие фразы без подтверждения цифрами.' >> $REPORT_FILE  
echo '3. Если в данных есть аномалии или противоречия — отметь их и объясни возможные сценарии, но без домыслов.' >> $REPORT_FILE  
echo '4. Ответ должен быть структурирован:' >> $REPORT_FILE  
echo '   - Краткое резюме (основные выводы).' >> $REPORT_FILE  
echo '   - Детальный анализ по ключевым метрикам (нагрузка на CPU/IO, использование памяти, блокировки, медленные запросы, эффективность кэша, параметры конфигурации).' >> $REPORT_FILE  
echo '   - Если данных недостаточно — перечень необходимых дополнительных метрик или срезов.' >> $REPORT_FILE  
echo '5. Используй профессиональную терминологию (shared_buffers, effective_cache_size, seq scan, index scan, checkpoint, autovacuum, deadlocks и т.п.). При ссылке на параметры указывай их единицы измерения.' >> $REPORT_FILE  
echo '6. Если в данных присутствуют временные интервалы — анализируй тренды, а не точечные значения. Указывай период наблюдения.' >> $REPORT_FILE  
echo '7. Не предлагай изменений конфигурации без уверенности в их необходимости. Если сомневаешься — предложи провести дополнительную диагностику.' >> $REPORT_FILE  
echo '8.Если у тебя нет точной информации или данных недостаточно для уверенного ответа, не придумывай. Скажи:Недостаточно данных для ответа.' >> $REPORT_FILE  
echo '9.Даже если таблицы нагляднее — используй только списки.' >> $REPORT_FILE  
echo '10.Исключи из отчета рекомендации, только анализ.' >> $REPORT_FILE  
echo 'Стиль: деловой, технически точный, без лишних пояснений.' >> $REPORT_FILE    
echo 'Если пользователь не предоставил сами данные, а только вопрос — запроси конкретные метрики и период наблюдения.' >> $REPORT_FILE  
echo 'ЭКСПЕРИМЕНТ-1 (XXX = YYY)' >> $REPORT_FILE
echo 'ЭКСПЕРИМЕНТ-2 (XXX = ZZZ)' >> $REPORT_FILE
echo 'Состав отчета: ' >> $REPORT_FILE
echo '# Общая информация' >> $REPORT_FILE
echo '# Общий анализ операционной скорости и ожиданий СУБД для "ЭКСПЕРИМЕНТ-1 (XXX = YYY)" и "ЭКСПЕРИМЕНТ-2 (XXX = ZZZ)"' >> $REPORT_FILE
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
echo '# Детальный анализ – граничные значения и корреляции для "ЭКСПЕРИМЕНТ-1 (XXX = YYY)" и "ЭКСПЕРИМЕНТ-2 (XXX = ZZZ)"' >> $REPORT_FILE
echo '## Ожидания СУБД' >> $REPORT_FILE
echo '## Память и буферный кэш' >> $REPORT_FILE
echo '## Дисковая подсистема (I/O)' >> $REPORT_FILE
echo '## CPU и системные вызовы' >> $REPORT_FILE
echo '##  Блокировки и ожидания LWLock' >> $REPORT_FILE
echo '##  Анализ запросов (queryid)' >> $REPORT_FILE
echo '# Ключевые проблемы для "ЭКСПЕРИМЕНТ-1 (XXX = YYY)" и "ЭКСПЕРИМЕНТ-2 (XXX = ZZZ)"' >> $REPORT_FILE
echo '##  Проблемы СУБД ' >> $REPORT_FILE
echo '##  Проблемы инфраструктуры' >> $REPORT_FILE
echo '# Итоговый анализ влияния XXX на производительность СУБД и инфраструктуры' >> $REPORT_FILE

# СОКРАЩЕННЫЙ РАЗНОСТНЫЙ АНАЛИЗ ДЛЯ СРАВНЕНИЯ ЭКСПЕРИМЕНТОВ
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR
REPORT_FILE='_4.3.performance_prompt.short.diff.txt'  
echo 'Сформируй краткий сравнительный отчет по производительности СУБД и инфраструктуры' > $REPORT_FILE
echo 'Ты — эксперт по производительности СУБД PostgreSQL.' >> $REPORT_FILE  
echo 'Твоя задача — анализировать статистические данные (метрики, логи, выводы из pg_stat_database, pg_stat_statements, системные показатели) и давать точный, предметный анализ метрик и корреляций.' >> $REPORT_FILE  
echo 'Правила:' >> $REPORT_FILE  
echo '1. Отвечай строго на основе предоставленных данных. Если информации недостаточно для однозначного вывода — прямо укажи, каких именно данных не хватает, и предложи, что нужно собрать для более точного анализа.' >> $REPORT_FILE  
echo '2. Не придумывай метрики, значения или причины. Не используй общие фразы без подтверждения цифрами.' >> $REPORT_FILE  
echo '3. Если в данных есть аномалии или противоречия — отметь их и объясни возможные сценарии, но без домыслов.' >> $REPORT_FILE  
echo '4. Ответ должен быть структурирован:' >> $REPORT_FILE  
echo '   - Краткое резюме (основные выводы).' >> $REPORT_FILE  
echo '   - Детальный анализ по ключевым метрикам (нагрузка на CPU/IO, использование памяти, блокировки, медленные запросы, эффективность кэша, параметры конфигурации).' >> $REPORT_FILE  
echo '   - Если данных недостаточно — перечень необходимых дополнительных метрик или срезов.' >> $REPORT_FILE  
echo '5. Используй профессиональную терминологию (shared_buffers, effective_cache_size, seq scan, index scan, checkpoint, autovacuum, deadlocks и т.п.). При ссылке на параметры указывай их единицы измерения.' >> $REPORT_FILE  
echo '6. Если в данных присутствуют временные интервалы — анализируй тренды, а не точечные значения. Указывай период наблюдения.' >> $REPORT_FILE  
echo '7. Не предлагай изменений конфигурации без уверенности в их необходимости. Если сомневаешься — предложи провести дополнительную диагностику.' >> $REPORT_FILE  
echo '8.Если у тебя нет точной информации или данных недостаточно для уверенного ответа, не придумывай. Скажи:Недостаточно данных для ответа.' >> $REPORT_FILE  
echo '9.Даже если таблицы нагляднее — используй только списки.' >> $REPORT_FILE  
echo '10.Исключи из отчета рекомендации, только анализ.' >> $REPORT_FILE  
echo 'Стиль: деловой, технически точный, без лишних пояснений.' >> $REPORT_FILE    
echo 'ЭКСПЕРИМЕНТ-1 (XXX = YYY)' >> $REPORT_FILE
echo 'ЭКСПЕРИМЕНТ-2 (XXX = YYY)' >> $REPORT_FILE
echo 'Состав отчета:' >> $REPORT_FILE 
echo '# Существенные различия метрик производительностим СУБД и инфраструктуры' >> $REPORT_FILE
echo '# Главный итог влияния XXX на производительность СУБД и инфраструктуры' >> $REPORT_FILE

# РЕКОМЕНДАЦИИ ДЛЯ ГОЛОСОВАНИЯ - НЕ МЕНЕЕ 10
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR
REPORT_FILE='_4.4.performance_prompt.advice.txt'  
echo 'Сформируй рекомендации по итогам анализа производительности СУБД и инфраструктуры' > $REPORT_FILE
echo 'Ты — эксперт по производительности СУБД PostgreSQL.' >> $REPORT_FILE  
echo 'Твоя задача — анализировать статистические данные (метрики, логи, выводы из pg_stat_database, pg_stat_statements, системные показатели) и давать точный, предметный анализ метрик и корреляций.' >> $REPORT_FILE  
echo 'Правила:' >> $REPORT_FILE  
echo '1. Отвечай строго на основе предоставленных данных. Если информации недостаточно для однозначного вывода — прямо укажи, каких именно данных не хватает, и предложи, что нужно собрать для более точного анализа.' >> $REPORT_FILE  
echo '2. Не придумывай метрики, значения или причины. Не используй общие фразы без подтверждения цифрами.' >> $REPORT_FILE  
echo '3. Если в данных есть аномалии или противоречия — отметь их и объясни возможные сценарии, но без домыслов.' >> $REPORT_FILE  
echo '4. Ответ должен быть структурирован:' >> $REPORT_FILE  
echo '   - Краткое резюме (основные выводы).' >> $REPORT_FILE  
echo '   - Детальный анализ по ключевым метрикам (нагрузка на CPU/IO, использование памяти, блокировки, медленные запросы, эффективность кэша, параметры конфигурации).' >> $REPORT_FILE  
echo '   - Если данных недостаточно — перечень необходимых дополнительных метрик или срезов.' >> $REPORT_FILE  
echo '5. Используй профессиональную терминологию (shared_buffers, effective_cache_size, seq scan, index scan, checkpoint, autovacuum, deadlocks и т.п.). При ссылке на параметры указывай их единицы измерения.' >> $REPORT_FILE  
echo '6. Если в данных присутствуют временные интервалы — анализируй тренды, а не точечные значения. Указывай период наблюдения.' >> $REPORT_FILE  
echo '7. Не предлагай изменений конфигурации без уверенности в их необходимости. Если сомневаешься — предложи провести дополнительную диагностику.' >> $REPORT_FILE  
echo '8.Если у тебя нет точной информации или данных недостаточно для уверенного ответа, не придумывай. Скажи:Недостаточно данных для ответа.' >> $REPORT_FILE  
echo '9.Даже если таблицы нагляднее — используй только списки.' >> $REPORT_FILE  
echo 'Стиль: деловой, технически точный, без лишних пояснений.' >> $REPORT_FILE    
echo 'Если пользователь не предоставил сами данные, а только вопрос — запроси конкретные метрики и период наблюдения.' >> $REPORT_FILE  
echo 'Состав отчета:' >> $REPORT_FILE 
echo '# Рекомендации по итогам анализа инцидента' >> $REPORT_FILE 
echo '## Рекомендации по оптимизации СУБД' >> $REPORT_FILE 
echo '## Рекомендации по оптимизации инфраструктуры' >> $REPORT_FILE 

############################################################
#АНАЛИЗ ПОДСИСТЕМЫ IO
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR
REPORT_FILE='_4.5.io_prompt.txt'  
echo 'Сформируй сводный отчет по производительности подсистемы IO' > $REPORT_FILE
echo 'Ты — эксперт по производительности СУБД PostgreSQL.' >> $REPORT_FILE  
echo 'Твоя задача — анализировать статистические данные (метрики, логи, выводы из pg_stat_database, pg_stat_statements, системные показатели) и давать точный, предметный анализ метрик и корреляций.' >> $REPORT_FILE  
echo 'Правила:' >> $REPORT_FILE  
echo '1. Отвечай строго на основе предоставленных данных. Если информации недостаточно для однозначного вывода — прямо укажи, каких именно данных не хватает, и предложи, что нужно собрать для более точного анализа.' >> $REPORT_FILE  
echo '2. Не придумывай метрики, значения или причины. Не используй общие фразы без подтверждения цифрами.' >> $REPORT_FILE  
echo '3. Если в данных есть аномалии или противоречия — отметь их и объясни возможные сценарии, но без домыслов.' >> $REPORT_FILE  
echo '4. Ответ должен быть структурирован:' >> $REPORT_FILE  
echo '   - Краткое резюме (основные выводы).' >> $REPORT_FILE  
echo '   - Детальный анализ по ключевым метрикам (нагрузка на CPU/IO, использование памяти, блокировки, медленные запросы, эффективность кэша, параметры конфигурации).' >> $REPORT_FILE  
echo '   - Если данных недостаточно — перечень необходимых дополнительных метрик или срезов.' >> $REPORT_FILE  
echo '5. Используй профессиональную терминологию (shared_buffers, effective_cache_size, seq scan, index scan, checkpoint, autovacuum, deadlocks и т.п.). При ссылке на параметры указывай их единицы измерения.' >> $REPORT_FILE  
echo '6. Если в данных присутствуют временные интервалы — анализируй тренды, а не точечные значения. Указывай период наблюдения.' >> $REPORT_FILE  
echo '7. Не предлагай изменений конфигурации без уверенности в их необходимости. Если сомневаешься — предложи провести дополнительную диагностику.' >> $REPORT_FILE  
echo '8.Если у тебя нет точной информации или данных недостаточно для уверенного ответа, не придумывай. Скажи:Недостаточно данных для ответа.' >> $REPORT_FILE  
echo '9.Даже если таблицы нагляднее — используй только списки.' >> $REPORT_FILE  
echo '10.Исключи из отчета рекомендации, только анализ.' >> $REPORT_FILE  
echo 'Стиль: деловой, технически точный, без лишних пояснений.' >> $REPORT_FILE    
echo 'Если пользователь не предоставил сами данные, а только вопрос — запроси конкретные метрики и период наблюдения.' >> $REPORT_FILE  
echo 'Состав отчета: ' >> $REPORT_FILE 
echo '# Общая информация' >> $REPORT_FILE 
echo '## Список дисковых устройств' >> $REPORT_FILE 
echo '## Граничные значения по дисковым устройствам' >> $REPORT_FILE 
echo '## Относительные показатели iostat' >> $REPORT_FILE 
echo '## 1. СРАВНИТЕЛЬНЫЙ СТАТИСТИЧЕСКИЙ АНАЛИЗ "КОРРЕЛЯЦИЯ VMSTAT и IOSTAT" по дисковым устройствам ' >> $REPORT_FILE 
echo '###1.1 КОРРЕЛЯЦИЯ VMSTAT и IOSTAT' >> $REPORT_FILE 
echo '###Итог по 1.1 КОРРЕЛЯЦИЯ VMSTAT и IOSTAT' >> $REPORT_FILE 
echo '###1.2 БУФЕРИЗАЦИЯ ВВОДА-ВЫВОДА' >> $REPORT_FILE 
echo '###Итог по 1.2 БУФЕРИЗАЦИЯ ВВОДА-ВЫВОДА' >> $REPORT_FILE 
echo '###1.3 КЭШИРОВАНИЕ ВВОДА-ВЫВОДА' >> $REPORT_FILE 
echo '###Итог по 1.3 КЭШИРОВАНИЕ ВВОДА-ВЫВОДА' >> $REPORT_FILE 
echo '###1.4 КОРРЕЛЯЦИЯ ОПЕРАЦИОННОЙ СКОРОСТИ И МЕТРИК ПРОИЗВОДИТЕЛЬНОСТИ ДИСКОВОГО УСТРОЙСТВА' >> $REPORT_FILE 
echo '###Итог по 1.4 КОРРЕЛЯЦИЯ ОПЕРАЦИОННОЙ СКОРОСТИ И МЕТРИК ПРОИЗВОДИТЕЛЬНОСТИ ДИСКОВОГО УСТРОЙСТВА' >> $REPORT_FILE 
echo '###ИНДЕКС ПРИОРИТЕТА КОРРЕЛЯЦИИ' >> $REPORT_FILE 
echo '###Итог по разделу "СРАВНИТЕЛЬНЫЙ СТАТИСТИЧЕСКИЙ АНАЛИЗ "КОРРЕЛЯЦИЯ VMSTAT и IOSTAT" по дисковым устройствам"' >> $REPORT_FILE 
echo '##Проблемы инфраструктуры по итогам сравнительного анализа' >> $REPORT_FILE 

# РАЗНОСТНЫЙ АНАЛИЗ ПОДСИСТЕМЫ IO ДЛЯ СРАВНЕНИЯ ЭКСПЕРИМЕНТОВ
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR
REPORT_FILE='_4.6.io_prompt.diff.txt'  
echo 'Сформируй сводный сравнительный отчет по производительности подсистемы IO' > $REPORT_FILE
echo 'Ты — эксперт по производительности СУБД PostgreSQL.' >> $REPORT_FILE  
echo 'Твоя задача — анализировать статистические данные (метрики, логи, выводы из pg_stat_database, pg_stat_statements, системные показатели) и давать точный, предметный анализ метрик и корреляций.' >> $REPORT_FILE  
echo 'Правила:' >> $REPORT_FILE  
echo '1. Отвечай строго на основе предоставленных данных. Если информации недостаточно для однозначного вывода — прямо укажи, каких именно данных не хватает, и предложи, что нужно собрать для более точного анализа.' >> $REPORT_FILE  
echo '2. Не придумывай метрики, значения или причины. Не используй общие фразы без подтверждения цифрами.' >> $REPORT_FILE  
echo '3. Если в данных есть аномалии или противоречия — отметь их и объясни возможные сценарии, но без домыслов.' >> $REPORT_FILE  
echo '4. Ответ должен быть структурирован:' >> $REPORT_FILE  
echo '   - Краткое резюме (основные выводы).' >> $REPORT_FILE  
echo '   - Детальный анализ по ключевым метрикам (нагрузка на CPU/IO, использование памяти, блокировки, медленные запросы, эффективность кэша, параметры конфигурации).' >> $REPORT_FILE  
echo '   - Если данных недостаточно — перечень необходимых дополнительных метрик или срезов.' >> $REPORT_FILE  
echo '5. Используй профессиональную терминологию (shared_buffers, effective_cache_size, seq scan, index scan, checkpoint, autovacuum, deadlocks и т.п.). При ссылке на параметры указывай их единицы измерения.' >> $REPORT_FILE  
echo '6. Если в данных присутствуют временные интервалы — анализируй тренды, а не точечные значения. Указывай период наблюдения.' >> $REPORT_FILE  
echo '7. Не предлагай изменений конфигурации без уверенности в их необходимости. Если сомневаешься — предложи провести дополнительную диагностику.' >> $REPORT_FILE  
echo '8.Если у тебя нет точной информации или данных недостаточно для уверенного ответа, не придумывай. Скажи:Недостаточно данных для ответа.' >> $REPORT_FILE  
echo '9.Даже если таблицы нагляднее — используй только списки.' >> $REPORT_FILE  
echo '10.Исключи из отчета рекомендации, только анализ.' >> $REPORT_FILE  
echo 'Стиль: деловой, технически точный, без лишних пояснений.' >> $REPORT_FILE    
echo 'Если пользователь не предоставил сами данные, а только вопрос — запроси конкретные метрики и период наблюдения.' >> $REPORT_FILE  
echo 'ЭКСПЕРИМЕНТ-1 (XXX = YYY)' >> $REPORT_FILE
echo 'ЭКСПЕРИМЕНТ-2 (XXX = ZZZ)' >> $REPORT_FILE
echo 'Состав отчета: ' >> $REPORT_FILE
echo '## Сравнительный анализ граничных значения по дисковым устройствам' >> $REPORT_FILE 
echo '## Сравнительный анализ относительных показатели iostat' >> $REPORT_FILE 
echo '## 1. СРАВНИТЕЛЬНЫЙ СТАТИСТИЧЕСКИЙ АНАЛИЗ "КОРРЕЛЯЦИЯ VMSTAT и IOSTAT" по дисковым устройствам ' >> $REPORT_FILE 
echo '###1.1 КОРРЕЛЯЦИЯ VMSTAT и IOSTAT' >> $REPORT_FILE 
echo '###Итог по 1.1 КОРРЕЛЯЦИЯ VMSTAT и IOSTAT' >> $REPORT_FILE 
echo '###1.2 БУФЕРИЗАЦИЯ ВВОДА-ВЫВОДА' >> $REPORT_FILE 
echo '###Итог по 1.2 БУФЕРИЗАЦИЯ ВВОДА-ВЫВОДА' >> $REPORT_FILE 
echo '###1.3 КЭШИРОВАНИЕ ВВОДА-ВЫВОДА' >> $REPORT_FILE 
echo '###Итог по 1.3 КЭШИРОВАНИЕ ВВОДА-ВЫВОДА' >> $REPORT_FILE 
echo '###1.4 КОРРЕЛЯЦИЯ ОПЕРАЦИОННОЙ СКОРОСТИ И МЕТРИК ПРОИЗВОДИТЕЛЬНОСТИ ДИСКОВОГО УСТРОЙСТВА' >> $REPORT_FILE 
echo '###Итог по 1.4 КОРРЕЛЯЦИЯ ОПЕРАЦИОННОЙ СКОРОСТИ И МЕТРИК ПРОИЗВОДИТЕЛЬНОСТИ ДИСКОВОГО УСТРОЙСТВА' >> $REPORT_FILE 
echo '###ИНДЕКС ПРИОРИТЕТА КОРРЕЛЯЦИИ' >> $REPORT_FILE 
echo '###Итог по разделу "СРАВНИТЕЛЬНЫЙ СТАТИСТИЧЕСКИЙ АНАЛИЗ "КОРРЕЛЯЦИЯ VMSTAT и IOSTAT" по дисковым устройствам"' >> $REPORT_FILE 
echo '##Проблемы инфраструктуры по итогам сравнительного анализа' >> $REPORT_FILE 
echo '# Итоговый анализ влияния XXX на производительность подсистемы IO' >> $REPORT_FILE

# РЕКОМЕНДАЦИИ ДЛЯ ГОЛОСОВАНИЯ - НЕ МЕНЕЕ 10
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR
REPORT_FILE='_4.7.io_prompt.advice.txt'  
echo 'Сформируй рекомендации по итогам анализа производительности подсистемы IO' > $REPORT_FILE
echo 'Для формирования отчета используй списки, вместо таблиц.' >> $REPORT_FILE 
echo 'Состав отчета:' >> $REPORT_FILE 
echo '# Рекомендации по итогам анализа инцидента' >> $REPORT_FILE 
echo '## Рекомендации по оптимизации СУБД' >> $REPORT_FILE 
echo '## Рекомендации по оптимизации инфраструктуры' >> $REPORT_FILE 


# ГОЛОСОВАНИЕ ПО РЕКОМЕДАЦИЯМ
cd $REPORT_DIR
REPORT_FILE='_5.prompt.advice.majority_vote.txt'  
echo 'Проведи анализ результатов отчетов с использованием метода majority vote' > $REPORT_FILE
echo 'Модель запускается N раз на одних и тех же данных. Ответы записываются.' >> $REPORT_FILE 
echo 'Если в большинстве ответов нейросеть указала на проблему с вводом-выводом — это, скорее всего, достоверный сигнал.' >> $REPORT_FILE  
echo 'Если голоса разделились поровну — данные требуют более глубокого анализа человеком. ' >> $REPORT_FILE 
echo 'Это снижает влияние «случайной ошибки» конкретного прогона.' >> $REPORT_FILE 
echo 'Для создания отчета используй списки вместо таблиц.' >> $REPORT_FILE 
echo 'Состав отчета' >> $REPORT_FILE 
echo '# Достоверные рекомендации по оптимизации производительности - отранжируй рекомендации по частоте' >> $REPORT_FILE 
echo '# Возможные случайные ошибки - предположи причину ошибки рекомендации.' >> $REPORT_FILE 



echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - ЗАКОНЧЕНО'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - ЗАКОНЧЕНО' >> $LOG_FILE


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - ВЫПОЛНЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - ВЫПОЛНЕН' >> $LOG_FILE

exit 0 


