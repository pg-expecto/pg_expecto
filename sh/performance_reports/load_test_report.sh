#!/bin/sh
########################################################################################################
# load_test_report.sh
# Отчет по нагрузочному тестированию
# version 4.0
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


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - ВЫПОЛНЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - ВЫПОЛНЕН' >> $LOG_FILE

exit 0 
