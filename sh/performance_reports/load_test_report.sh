#!/bin/sh
#!/usr/bin/env bash
# Copyright 2026 Ринат (pg_expecto)
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
########################################################################################################
# load_test_report.sh
# ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ
# version 8.1
# updated 17/04/2026
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
# 2.СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1.СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1.СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT' >> $LOG_FILE

REPORT_FILE='_2.postgresql_vmstat_iostat.txt'
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
# 2.СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT

##########################################################################################

##########################################################################################
# 3.СТАТИСТИКА VMSTAT - IOSTAT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.СТАТИСТИКА IOSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.СТАТИСТИКА IOSTAT' >> $LOG_FILE
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR

echo 'КОМПЛЕКСНЫЙ КОРРЕЛЯЦИОННЫЙ АНАЛИЗ МЕТРИК VMSTAT-IOSTAT' >> $REPORT_FILE 
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
# 3.СТАТИСТИКА VMSTAT - IOSTAT
##########################################################################################


##########################################################################################
# ИНСТРУКЦИЯ И ПРОМПТЫ
######################################################################################################
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ИНСТРУКЦИЯ И ПРОМПТЫ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ИНСТРУКЦИЯ И ПРОМПТЫ' >> $LOG_FILE

REPORT_DIR='/tmp/pg_expecto_reports' 
#Инструкция
cp $current_path'/_pg_expecto_instruction.txt' $REPORT_DIR'/'
cp $current_path'/prompt_header.txt' $REPORT_DIR'/_load_test_prompt.txt'
cat $current_path'/prompt_task.txt' >> $REPORT_DIR'/_load_test_prompt.txt'
cat $current_path'/prompt_body.txt' >> $REPORT_DIR'/_load_test_prompt.txt'
cp $current_path'/_philosophical_instruction_prompt.txt' $REPORT_DIR'/_load_test_philosophical_instruction_prompt.txt'


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ ZIP для DeepSeek'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ ZIP для DeepSeek' >> $LOG_FILE

zip load_test_4deepseek.zip  _1.settings.txt _2.postgresql_vmstat_iostat.txt _pg_expecto_instruction.txt _load_test_prompt.txt _load_test_philosophical_instruction_prompt.txt
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - ЗАКОНЧЕНО'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - ЗАКОНЧЕНО' >> $LOG_FILE

# ВХОДНЫЕ ДАННЫЕ ДЛЯ НЕЙРОСЕТИ
####################################################################################################################################################################################




echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - ВЫПОЛНЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО НАГРУЗОЧНОМУ ТЕСТИРОВАНИЮ - ВЫПОЛНЕН' >> $LOG_FILE

exit 0 


