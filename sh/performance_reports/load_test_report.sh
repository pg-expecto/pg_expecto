#!/bin/sh
########################################################################################################
# load_test_report.sh
# Отчет по нагрузочному тестированию
# version 5.0
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


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  СВЕЧНОЙ ГРАФИК ПРОИЗВОДИТЕЛЬНОСТИ СУБД В ХОДЕ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  СВЕЧНОЙ ГРАФИК ПРОИЗВОДИТЕЛЬНОСТИ СУБД В ХОДЕ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ' >> $LOG_FILE
REPORT_FILE='postgres._load_test.txt'
psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_load_test())" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ГРАФИК ИЗМЕНЕНИЯ НАГРУЗКИ В ХОДЕ ТЕСТИРОВАНИЯ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ГРАФИК ИЗМЕНЕНИЯ НАГРУЗКИ В ХОДЕ ТЕСТИРОВАНИЯ' >> $LOG_FILE
REPORT_FILE='postgres._load_test_loading.txt'
psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_load_test_loading())" > $REPORT_FILE 2>$ERR_FILE
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

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK ' >> $LOG_FILE

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
	  
	  REPORT_FILE=$current_path'/scenario.'$sc_count'.'$wait_event_type'.txt'
	  
	  psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_queryid_stat("$queryid" , '$wait_event_type' , '$start_timestamp' , '$finish_timestamp'))" > $REPORT_FILE 2>$ERR_FILE
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

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1.ВХОДНЫЕ ДАННЫЕ ДЛЯ НЕЙРОСЕТИ - СУБД И VMSTAT/IOSTAT '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1.ВХОДНЫЕ ДАННЫЕ ДЛЯ НЕЙРОСЕТИ - СУБД И VMSTAT/IOSTAT ' >> $LOG_FILE
REPORT_DIR='/tmp/pg_expecto_reports'
REPORT_FILE='_1.summary.txt'
cd $REPORT_DIR

devices_list=`$current_path'/'get_reports_param.sh $current_path devices_list 2>$ERR_FILE`
exit_code $? $LOG_FILE $ERR_FILE
array=($devices_list)

#Заголовок отчета 
title=$1

echo $title > $REPORT_FILE
echo $(date "+%d-%m-%Y %H:%M:%S") >> $REPORT_FILE
echo ' ' >> $REPORT_FILE


#файл postgresql.auto.conf
echo 'postgresql.auto.conf' >> $REPORT_FILE
data_directory=`psql -Aqtc  'SHOW data_directory'`
postgresql_auto_conf=$data_directory'/postgresql.auto.conf'
cat $postgresql_auto_conf >> $REPORT_FILE
echo ' ' >> $REPORT_FILE

#количество ядер CPU
echo 'CPU' >> $REPORT_FILE
lscpu >>  $REPORT_FILE
echo ' ' >> $REPORT_FILE

#размер RAM
echo 'RAM' >> $REPORT_FILE
free -b | awk '/^Mem:/ {printf "%.2f GB\n", $2/1024/1024/1024}' >> $REPORT_FILE
echo ' ' >> $REPORT_FILE

#IO
echo 'IO' >> $REPORT_FILE
lsblk >> $REPORT_FILE
echo 'devices='$devices_list >> $REPORT_FILE
echo ' ' >> $REPORT_FILE

cat 'postgres._load_test_loading.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
echo 'PostgreSQL' >> $REPORT_FILE
echo '-----------' >> $REPORT_FILE
cat 'postgres.1.cluster_report_meta.txt' >> $REPORT_FILE 
echo '-----------' >> $REPORT_FILE
cat 'postgres.1.cluster_report_4graph.txt' >> $REPORT_FILE 
echo '-----------' >> $REPORT_FILE
cat 'postgres.2.wait_event.txt' >> $REPORT_FILE 
echo '-----------' >> $REPORT_FILE
cat 'postgres.3.queryid.txt' >> $REPORT_FILE 
echo '-----------' >> $REPORT_FILE
cat 'postgres.x.sql_list.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
echo 'VMSTAT IOSTAT CORRELATION AND CHECKLISTS' >> $REPORT_FILE
echo '       ' >> $REPORT_FILE
cat 'linux.1.waitings_vmstat_corr.txt' >> $REPORT_FILE 
echo '-----------' >> $REPORT_FILE
cat 'linux.3.vmstat_io.txt' >> $REPORT_FILE 
echo '-----------' >> $REPORT_FILE
cat 'linux.4.vmstat_cpu.txt' >> $REPORT_FILE 
echo '-----------' >> $REPORT_FILE
cat 'linux.5.vmstat_ram.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
echo 'VMSTAT ' >> $REPORT_FILE
echo '       ' >> $REPORT_FILE
cat 'linux.x.vmstat_meta.txt' >> $REPORT_FILE 
echo '-----------' >> $REPORT_FILE
cat 'linux.x.vmstat_4graph.txt' >> $REPORT_FILE 
echo '-------------------------------------------------------------------------' >> $REPORT_FILE

REPORT_FILE='_1.prompt.txt'
echo 'Проанализируй данные по метрикам производительности и ожиданий СУБД , метрикам инфраструктуры vmstat/iostat. Подготовь итоговый отчет по результатам анализа.' > $REPORT_FILE

######################################################################################################
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.ВХОДНЫЕ ДАННЫЕ ДЛЯ НЕЙРОСЕТИ - IO PERFORMANCE '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.ВХОДНЫЕ ДАННЫЕ ДЛЯ НЕЙРОСЕТИ - IO PERFORMANCE ' >> $LOG_FILE
REPORT_DIR='/tmp/pg_expecto_reports'
REPORT_FILE='_2.io_performance.txt'
cd $REPORT_DIR

#количество ядер CPU
echo 'CPU' >> $REPORT_FILE
lscpu >>  $REPORT_FILE
echo ' ' >> $REPORT_FILE

#IO
echo 'IO' >> $REPORT_FILE
lsblk >> $REPORT_FILE
echo 'devices='$devices_list >> $REPORT_FILE
echo ' ' >> $REPORT_FILE
echo '-------------------------------------------------------------------------' >> $REPORT_FILE
echo 'VMSTAT/IOSTAT - CORRELATION' >> $REPORT_FILE
echo ' ' >> $REPORT_FILE
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
  
  CURRENT_REPORT_FILE='linux.2.vmstat_iostat_'$device'.txt'

  cat $CURRENT_REPORT_FILE >> $REPORT_FILE 
  echo '-----------' >> $REPORT_FILE
  
  let i=i+1
done

echo '-----------' >> $REPORT_FILE
cat 'linux.3.vmstat_io.txt' >> $REPORT_FILE 
echo '-----------' >> $REPORT_FILE

echo 'IOSTAT - PERFORMANCE' >> $REPORT_FILE
echo ' ' >> $REPORT_FILE
let i=0
while :
do  
  device=${array[$i]}
  size=${#device}
	
  if [ "$size" == 0 ];
  then 
   break
  fi   
	CURRENT_REPORT_FILE='linux.x.iostat_'$device'_meta.txt'
	cat $CURRENT_REPORT_FILE >> $REPORT_FILE 
	echo '-----------' >> $REPORT_FILE
	
	CURRENT_REPORT_FILE='linux.x.iostat_'$device'_4graph.txt'
	cat $CURRENT_REPORT_FILE >> $REPORT_FILE 
	echo '-----------' >> $REPORT_FILE
	
	CURRENT_REPORT_FILE='linux.x.iostat_'$device'_performance.txt'
	cat $CURRENT_REPORT_FILE >> $REPORT_FILE 
	echo '-----------' >> $REPORT_FILE
	
	  
  let i=i+1
done


REPORT_FILE='_2.io_performance_prompt.txt' > $REPORT_FILE
echo 'Подготовь отчет по результатам анализа производительности подсистемы IO' >> $REPORT_FILE 
echo 'для дисковых устройств, используемых для файловых систем /data /wal' >> $REPORT_FILE
echo '**Общая характеристика системы**' >> $REPORT_FILE
echo '- Период анализа' >> $REPORT_FILE
echo '- Основные устройства хранения' >> $REPORT_FILE
echo '- Тип нагрузки' >> $REPORT_FILE
echo 'Состав отчета по файловой системе:' >> $REPORT_FILE
echo '**Критические проблемы производительности по файловой системе**' >> $REPORT_FILE
echo '**Анализ корреляций и паттернов нагрузки по файловой системе**' >> $REPORT_FILE
echo '**Диагностика узких мест IO по файловой системе**' >> $REPORT_FILE
echo '- r_await(ms)' >> $REPORT_FILE
echo '- w_await(ms)' >> $REPORT_FILE
echo '- aqu_sz' >> $REPORT_FILE
echo '- proc_b' >> $REPORT_FILE
echo '- cpu_wa(%)' >> $REPORT_FILE
echo '- Корреляция speed с IOPS' >> $REPORT_FILE
echo '- Корреляция speed с пропускной способностью (MB/s)' >> $REPORT_FILE
echo '- Вывод по диагностике узких мест IO' >> $REPORT_FILE
echo '**Рекомендации по оптимизации файловой системы**' >> $REPORT_FILE
echo '**Итоговый вывод по производительности IO**' >> $REPORT_FILE


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - ВЫПОЛНЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - ВЫПОЛНЕН' >> $LOG_FILE

exit 0 


