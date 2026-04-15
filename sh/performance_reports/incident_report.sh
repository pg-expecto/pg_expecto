#!/bin/sh
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
# incident_report.sh
# ОТЧЕТ ПО ИНЦИДЕНТУ ПРОИЗВОДИТЕЛЬНОСТИ СУБД 
# version 8.1
# updated 15/04/2026
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

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО ИНЦИДЕНТУ ПРОИЗВОДИТЕЛЬНОСТИ СУБД - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО ИНЦИДЕНТУ ПРОИЗВОДИТЕЛЬНОСТИ СУБД - НАЧАТ ' > $LOG_FILE


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
# 1. КОНФИГУРАЦИЯ 
$current_path'/'current_settings.sh
exit_code $? $LOG_FILE $ERR_FILE
# 1. КОНФИГУРАЦИЯ 
##########################################################################################


##########################################################################################
# ИНЦИДЕНТ: 2.СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1.ИНЦИДЕНТ: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 1.ИНЦИДЕНТ: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT' >> $LOG_FILE

REPORT_FILE='_2.incident.postgresql_vmstat_iostat.txt'
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
# ИНЦИДЕНТ:  3.СТАТИСТИКА VMSTAT - IOSTAT
##########################################################################################
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.СТАТИСТИКА IOSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.СТАТИСТИКА IOSTAT' >> $LOG_FILE
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR

  echo 'ИНЦИДЕНТ ПРОИЗВОДИТЕЛЬНОСТИ СУБД: КОМПЛЕКСНЫЙ КОРРЕЛЯЦИОННЫЙ АНАЛИЗ МЕТРИК VMSTAT-IOSTAT' >> $REPORT_FILE
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
# ТЕСТОВЫЙ ОТРЕЗОК: 2.СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.ТЕСТОВЫЙ ОТРЕЗОК: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.ТЕСТОВЫЙ ОТРЕЗОК: СТАТИСТИКА ПРОИЗВОДИТЕЛЬНОСТИ/ОЖИДАНИЙ СУБД и VMSTAT' >> $LOG_FILE

REPORT_FILE='_2.1.test.postgresql_vmstat_iostat.txt'
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
# ТЕСТОВЫЙ ОТРЕЗОК:  3.СТАТИСТИКА VMSTAT - IOSTAT
##########################################################################################
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.СТАТИСТИКА IOSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : 2.СТАТИСТИКА IOSTAT' >> $LOG_FILE
REPORT_DIR='/tmp/pg_expecto_reports'
cd $REPORT_DIR

  echo 'ТЕСТОВЫЙ ОТРЕЗОК: КОМПЛЕКСНЫЙ КОРРЕЛЯЦИОННЫЙ АНАЛИЗ МЕТРИК VMSTAT-IOSTAT' >> $REPORT_FILE
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
  
  CURRENT_REPORT_FILE='2.1.1.test.vmstat_iostat_'$device'.txt'
  cat $CURRENT_REPORT_FILE >> $REPORT_FILE 
  echo '-------------------------------------------------------------------------' >> $REPORT_FILE
  
  let i=i+1
done
# СТАТИСТИКА VMSTAT - IOSTAT
##########################################################################################


##########################################################################################
# ИНСТРУКЦИЯ И ПРОМПТЫ
######################################################################################################
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ИНСТРУКЦИЯ И ПРОМПТЫ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ИНСТРУКЦИЯ И ПРОМПТЫ' >> $LOG_FILE

REPORT_DIR='/tmp/pg_expecto_reports' 
#Инструкция
cp $current_path'/_pg_expecto_instruction.txt' $REPORT_DIR'/'
cp $current_path'/prompt_header.txt' $REPORT_DIR'/_incident_prompt.txt'
echo 'Задача: cформируй сравнительный сводный отчет по производительности СУБД и инфраструктуры о заданным периодам - Тест:'"$start_test_timestamp"'-'"$finish_test_timestamp"' и Инцидент:'"$start_incindent_timestamp"' - '"$finish_incident_timestamp" >> $REPORT_DIR'/_incident_prompt.txt'
cat $current_path'/prompt_body.txt' >> $REPORT_DIR'/_incident_prompt.txt'
cp $current_path'/_philosophical_instruction_prompt.txt' $REPORT_DIR'/_incident_philosophical_instruction_prompt.txt'


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ ZIP для DeepSeek'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ ZIP для DeepSeek' >> $LOG_FILE

zip incident_4deepseek.zip  _1.settings.txt _2.incident.postgresql_vmstat_iostat.txt _2.1.test.postgresql_vmstat_iostat.txt _pg_expecto_instruction.txt _incident_prompt.txt _incident_philosophical_instruction_prompt.txt
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - ЗАКОНЧЕНО'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - ЗАКОНЧЕНО' >> $LOG_FILE

# ВХОДНЫЕ ДАННЫЕ ДЛЯ НЕЙРОСЕТИ
####################################################################################################################################################################################











echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - ЗАКОНЧЕНО'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ФОРМИРОВАНИЕ СТАТИСТИЧЕСКИХ ДАННЫХ ДЛЯ НЕЙРОСЕТИ - ЗАКОНЧЕНО' >> $LOG_FILE


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО ИНЦИДЕНТУ ПРОИЗВОДИТЕЛЬНОСТИ СУБД - ВЫПОЛНЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  ОТЧЕТ ПО ИНЦИДЕНТУ ПРОИЗВОДИТЕЛЬНОСТИ СУБД - ВЫПОЛНЕН' >> $LOG_FILE

exit 0 


