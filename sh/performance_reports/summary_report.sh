#!/bin/sh
########################################################################################################
# summary_report.sh
# Сводный отчет  производительности/ожиданиям СУБД и метрикам ОС 
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
	
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  summary_report : Details in '$ERR_FILE
	echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  summary_report : Details in '$ERR_FILE >> $LOG_FILE
	
    exit $ecode
fi
}

script=$(readlink -f $0)
current_path=`dirname $script`


LOG_FILE=$current_path'/summary_report.log'
ERR_FILE=$current_path'/summary_report.err'
REPORT_DIR='/tmp/pg_expecto_reports'

timestamp_label=$(date "+%Y%m%d")'T'$(date "+%H%M")

expecto_db='expecto_db'
expecto_user='expecto_user'

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : СТАРТ ' > $LOG_FILE

if [ $# -eq 0 ]
then
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  summary_report :  summary_report :  AT LEAST START_TIME MUST BE SET '
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  summary_report :  AT LEAST START_TIME MUST BE SET ' >> $LOG_FILE
  exit 0
fi 

if [ $# -gt 3 ]
then
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  summary_report :  MAX 3 PARAMETERS CAN BE SET '
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  summary_report :  MAX 3 PARAMETERS CAN BE SET ' >> $LOG_FILE
  exit 0
fi 

start_timestamp=$1 
finish_timestamp=$2
test_mode=$3

if [ "$test_mode" == "" ]
then
	test_mode='DEFAULT'
else
	test_mode='INCIDENT'
fi 


devices_list=`$current_path'/'get_reports_param.sh $current_path devices_list 2>$ERR_FILE`
exit_code $? $LOG_FILE $ERR_FILE
  
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  summary_report : devices_list = '$devices_list
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : devices_list = '$devices_list >> $LOG_FILE	

if [ "$devices_list" == "" ] 
then 
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  summary_report : devices_list MUST BE SET '
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : ERROR :  summary_report : devices_list MUST BE SET ' >> $LOG_FILE	
  exit 100
fi 

#if [ "$finish_timestamp" == "" ]
#then 
#  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : HOUR_BEFORE '
#  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : HOUR_BEFORE ' >> $LOG_FILE
#
#  finish_timestamp=$start_timestamp
#  start_timestamp=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT get_hour_before('$finish_timestamp')"` 2>$ERR_FILE  
#  exit_code $? $LOG_FILE $ERR_FILE
#fi

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : ================ SUMMARY_REPORT ================================ '

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : start_timestamp = '$start_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : start_timestamp = '$start_timestamp >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : finish_timestamp = '$finish_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : finish_timestamp = '$finish_timestamp >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : devices_list = '$devices_list
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : devices_list = '$devices_list >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : test_mode = '$test_mode
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : test_mode = '$test_mode >> $LOG_FILE


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK' >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : *** ОТЧЕТЫ ПО МЕТРИКАМ VMSTAT/IOSTAT '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : *** ОТЧЕТЫ ПО МЕТРИКАМ VMSTAT/IOSTAT ' >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : *** ОТЧЕТЫ ПО ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЯМ СУБД '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : *** ОТЧЕТЫ ПО ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЯМ СУБД ' >> $LOG_FILE

##################################################################################################################################
# CLUSTER PERFORMANCE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЯМ НА УРОВНЕ СУБД - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЯМ НА УРОВНЕ СУБД - НАЧАТ ' >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : КОМПЛЕКСНЫЙ СТАТИСТИЧЕСКИЙ ОТЧЕТ ПО ОЖИДАНИЯМ СУБД'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : КОМПЛЕКСНЫЙ СТАТИСТИЧЕСКИЙ ОТЧЕТ ПО ОЖИДАНИЯМ СУБД' >> $LOG_FILE
if [ "$test_mode" == "DEFAULT" ]
then
  REPORT_FILE=$current_path'/1.1.postgresql.wait_event_type.txt'
else
    REPORT_FILE=$current_path'/1.1.1.test.postgresql.wait_event_type.txt'
fi

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_postgresql_wait_event_type('$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE
# CLUSTER PERFORMANCE
##################################################################################################################################


##################################################################################################################################
# VMSTAT PERFORMANCE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО ПРОИЗВОДИТЕЛЬНОСТИ VMSTAT - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО ПРОИЗВОДИТЕЛЬНОСТИ VMSTAT - НАЧАТ ' >> $LOG_FILE

if [ "$test_mode" == "DEFAULT" ]
then
  REPORT_FILE=$current_path'/1.2.vmstat.performance.txt'
else
  REPORT_FILE=$current_path'/1.2.1.test.vmstat.performance.txt'
fi

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_vmstat_performance('$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

# CLUSTER PERFORMANCE
##################################################################################################################################

##################################################################################################################################
# КОРРЕЛЯЦИЯ ОЖИДАНИЙ СУБД и vmstat
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО ПРОИЗВОДИТЕЛЬНОСТИ VMSTAT - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО ПРОИЗВОДИТЕЛЬНОСТИ VMSTAT - НАЧАТ ' >> $LOG_FILE
cpu_count=`nproc --all`
ram_all=` free -m | head -2 | tail -1 | awk -F " " '{print $2}'`

if [ "$test_mode" == "DEFAULT" ]
then
  REPORT_FILE=$current_path'/1.3.wait_event_type_vmstat.txt'
else
  REPORT_FILE=$current_path'/1.3.1.test.wait_event_type_vmstat.txt'
fi

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_wait_event_type_vmstat($cpu_count , $ram_all , '$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

# КОРРЕЛЯЦИЯ СУБД и vmstat
##################################################################################################################################

##################################################################################################################################
# IOSTAT КОРРЕЛЯЦИЯ
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО IOSTAT - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО IOSTAT - НАЧАТ ' >> $LOG_FILE

array=($devices_list)

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
  
  if [ "$test_mode" == "DEFAULT" ]
  then
    REPORT_FILE=$current_path'/2.1.vmstat_iostat_'$device'.txt'
  else
    REPORT_FILE=$current_path'/2.1.1.vmstat_iostat_'$device'.txt'
  fi  
  
  
  psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_vmstat_iostat('$start_timestamp' , '$finish_timestamp' , '$device'))" > $REPORT_FILE 2>$ERR_FILE
  exit_code $? $LOG_FILE $ERR_FILE
  chmod 777 $REPORT_FILE
  mv $REPORT_FILE $REPORT_DIR
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

  let i=i+1
done

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО IOSTAT - ЗАКОНЧЕН '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО IOSTAT - ЗАКОНЧЕН ' >> $LOG_FILE
# IOSTAT КОРРЕЛЯЦИЯ
##################################################################################################################################

##################################################################################################################################
# ИСХОДНЫЕ ДАННЫЕ
######################################################################################################
#ИСХОДНЫЕ ДАННЫЕ ПО ОЖИДАНИЯМ И ПРОИЗВОДИТЕЛЬНОСТИ
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ИСХОДНЫЕ ДАННЫЕ ПО ОЖИДАНИЯМ И ПРОИЗВОДИТЕЛЬНОСТИ PostgreSQL'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ИСХОДНЫЕ ДАННЫЕ ПО ОЖИДАНИЯМ И ПРОИЗВОДИТЕЛЬНОСТИ PostgreSQL' >> $LOG_FILE

if [ "$test_mode" == "DEFAULT" ]
then
  REPORT_FILE=$current_path'/x.postgresql.cluster_performance.txt'
else
  REPORT_FILE=$current_path'/x.1.test.postgresql.cluster_performance.txt'
fi  


psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_postgresql_cluster_performance('$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE
#ИСХОДНЫЕ ДАННЫЕ ПО ОЖИДАНИЯМ И ПРОИЗВОДИТЕЛЬНОСТИ
######################################################################################################

##################################################################################################################################
# VMSTAT 
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ДАННЫЕ ДЛЯ ГРАФИКОВ VMSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ДАННЫЕ ДЛЯ ГРАФИКОВ VMSTAT' >> $LOG_FILE

if [ "$test_mode" == "DEFAULT" ]
then
  REPORT_FILE=$current_path'/x.vmstat.txt'
else
  REPORT_FILE=$current_path'/x.1.test.vmstat.txt'
fi  


psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_vmstat('$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE
# VMSTAT 
##################################################################################################################################

##################################################################################################################################
# IOSTAT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ИСХОДЫЕ ДАННЫЕ IOSTAT '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ИСХОДЫЕ ДАННЫЕ IOSTAT ' >> $LOG_FILE

array=($devices_list)

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

  if [ "$test_mode" == "DEFAULT" ]
  then
    REPORT_FILE=$current_path'/x.iostat_'$device'.txt'
  else
    REPORT_FILE=$current_path'/x.1.test.iostat_'$device'.txt'
  fi  

  
  psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_iostat('$start_timestamp' , '$finish_timestamp' , '$device'))" > $REPORT_FILE 2>$ERR_FILE
  exit_code $? $LOG_FILE $ERR_FILE
  
  chmod 777 $REPORT_FILE
  mv $REPORT_FILE $REPORT_DIR

  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

  let i=i+1
done
# IOSTAT КОРРЕЛЯЦИЯ
##################################################################################################################################



######################################################################################################
#СТАТИСТИКА dirty_ratio/dirty_background_ratio
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : REPORT_VM_DIRTY'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : REPORT_VM_DIRTY' >> $LOG_FILE


if [ "$test_mode" == "DEFAULT" ]
then
  REPORT_FILE=$current_path'/x.vm_dirty.txt'
else
  REPORT_FILE=$current_path'/x.1.test.vm_dirty.txt'
fi  



ram_all=` free -m | head -2 | tail -1 | awk -F " " '{print $2}'`

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_vm_dirty($ram_all  , '$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE
#СТАТИСТИКА dirty_ratio/dirty_background_ratio
##################################################################################################################################

######################################################################################################
#СТАТИСТИКА shared_buffers
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : REPORT_SHARED_BUFFERS'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : REPORT_SHARED_BUFFERS' >> $LOG_FILE


if [ "$test_mode" == "DEFAULT" ]
then
  REPORT_FILE=$current_path'/x.shared_buffers.txt'
else
  REPORT_FILE=$current_path'/x.1.test.shared_buffers.txt'
fi  


ram_all=` free -m | head -2 | tail -1 | awk -F " " '{print $2}'`

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_shared_buffers('$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE
#СТАТИСТИКА shared_buffers
##################################################################################################################################

##################################################################################################################################
# ПОДГОТОВИТЬ СТАТИСТИКУ ПО SQL
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : === РАСЧЕТ СТАТИСТИКИ ПО SQL - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : === РАСЧЕТ СТАТИСТИКИ ПО SQL - НАЧАТ' >> $LOG_FILE

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT statement_stat_median('$start_timestamp' , '$finish_timestamp' )" >>$LOG_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : РАСЧЕТ СТАТИСТИКИ ПО SQL - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : РАСЧЕТ СТАТИСТИКИ ПО SQL - ЗАКОНЧЕН' >> $LOG_FILE
# ПОДГОТОВИТЬ СТАТИСТИКУ ПО SQL
##################################################################################################################################

##################################################################################################################################
# WAIT_EVENT FOR PARETO
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ ПО WAIT_EVENT(ДИАГРАММА ПАРЕТО) - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ ПО WAIT_EVENT(ДИАГРАММА ПАРЕТО) - НАЧАТ' >> $LOG_FILE

if [ "$test_mode" == "DEFAULT" ]
then
  REPORT_FILE=$current_path'/1.4.wait_event_type_pareto.txt.txt'
else
  REPORT_FILE=$current_path'/1.4.1.test.wait_event_type_pareto.txt.txt'
fi  


psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_wait_event_type_for_pareto( '$start_timestamp' , '$finish_timestamp' )) " >>$REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

# WAIT_EVENT FOR PARETO
####################################################################################################################################

##################################################################################################################################
# QUERYID FOR PARETO
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ ПО SQL(ДИАГРАММА ПАРЕТО) - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ ПО SQL(ДИАГРАММА ПАРЕТО) - НАЧАТ' >> $LOG_FILE


if [ "$test_mode" == "DEFAULT" ]
then
  REPORT_FILE=$current_path'/1.5.queryid_pareto.txt'
else
  REPORT_FILE=$current_path'/1.5.1.test.queryid_pareto.txt'
fi  


psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_queryid_for_pareto( '$start_timestamp' , '$finish_timestamp' )) " >>$REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

# QUERYID FOR PARETO
####################################################################################################################################



##################################################################################################################################
# SQL LIST 
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ФОРМИРОВАНИЕ СПИСКА SQL - НАЧАТО'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ФОРМИРОВАНИЕ СПИСКА SQL - НАЧАТО' >> $LOG_FILE


if [ "$test_mode" == "DEFAULT" ]
then
  REPORT_FILE=$current_path'/x.sql_list.txt'
else
  REPORT_FILE=$current_path'/x.1.test.sql_list.txt'
fi  



psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_sql_list( '$start_timestamp' , '$finish_timestamp' )) " >>$REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ФОРМИРОВАНИЕ СПИСКА SQL - ЗАКОНЧЕНО'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ФОРМИРОВАНИЕ СПИСКА SQL - ЗАКОНЧЕНО' >> $LOG_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK ' >> $LOG_FILE

# SQL LIST 
####################################################################################################################################

exit 0 
