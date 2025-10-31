#!/bin/sh
########################################################################################################
# queryid_report.sh
# Отчет по истории выполнения и событиям ожидания отдельного SQL запроса 
# version 1.0
########################################################################################################

#Обработать код возврата 
function exit_code {
ecode=$1
if [[ $ecode != 0 ]];
then
	ecode=$1
	LOG_FILE=$2
	ERR_FILE=$3
	
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  queryid_report : Details in '$ERR_FILE
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  queryid_report : Details in '$ERR_FILE >> $LOG_FILE
	
    exit $ecode
fi
}

script=$(readlink -f $0)
current_path=`dirname $script`


LOG_FILE=$current_path'/queryid_report.log'
ERR_FILE=$current_path'/queryid_report.err'
REPORT_DIR='/tmp/pg_expecto_reports'

timestamp_label=$(date "+%Y%m%d")'T'$(date "+%H%M")

expecto_db='expecto_db'
expecto_user='expecto_user'


if [ $# -gt 3 ]
then
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  queryid_report :  MAX 3 PARAMETERS CAN BE SET '
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  queryid_report :  MAX 3 PARAMETERS CAN BE SET ' >> $LOG_FILE
  exit 0
fi 

queryid=$1
start_timestamp=$2 
finish_timestamp=$3

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  queryid_report : ОТЧЕТ ПО ИСТОРИИ ВЫПОЛНЕНИЯ И СОБЫТИЯМ ОЖИДАНИЯ ОТДЕЛЬНОГО SQL ЗАПРОСА - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  queryid_report : ОТЧЕТ ПО ИСТОРИИ ВЫПОЛНЕНИЯ И СОБЫТИЯМ ОЖИДАНИЯ ОТДЕЛЬНОГО SQL ЗАПРОСА - НАЧАТ ' >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  queryid_report : queryid = '$queryid
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  queryid_report : queryid = '$queryid >> $LOG_FILE


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  queryid_report : start_timestamp = '$start_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  queryid_report : start_timestamp = '$start_timestamp >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  queryid_report : finish_timestamp = '$finish_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  queryid_report : finish_timestamp = '$finish_timestamp >> $LOG_FILE

#####################################################################################################
## ОЖИДАНИЯ ПО queryid
for wait_event_type in 'BufferPin' 'Extension' 'IO' 'IPC' 'Lock' 'LWLock' 'Timeout'
do 
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : WAIT_EVENT_TYPE='$wait_event_type
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : WAIT_EVENT_TYPE='$wait_event_type >> $LOG_FILE
  
  REPORT_FILE=$current_path'/queryid.'$queryid'.'$wait_event_type'.txt'
  
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


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  queryid_report : ОТЧЕТ ПО ИСТОРИИ ВЫПОЛНЕНИЯ И СОБЫТИЯМ ОЖИДАНИЯ ОТДЕЛЬНОГО SQL ЗАПРОСА - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  queryid_report : ОТЧЕТ ПО ИСТОРИИ ВЫПОЛНЕНИЯ И СОБЫТИЯМ ОЖИДАНИЯ ОТДЕЛЬНОГО SQL ЗАПРОСА - ЗАКОНЧЕН' >> $LOG_FILE

exit 0 
