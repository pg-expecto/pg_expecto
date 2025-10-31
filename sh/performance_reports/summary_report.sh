#!/bin/sh
########################################################################################################
# summary_report.sh
# Сводный отчет  производительности/ожиданиям СУБД и метрикам ОС 
# version 3.0
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

if [ "$finish_timestamp" == "" ]
then 
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : HOUR_BEFORE '
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : HOUR_BEFORE ' >> $LOG_FILE

  finish_timestamp=$start_timestamp
  start_timestamp=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT get_hour_before('$finish_timestamp')"` 2>$ERR_FILE  
  exit_code $? $LOG_FILE $ERR_FILE
fi


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : start_timestamp = '$start_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : start_timestamp = '$start_timestamp >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : finish_timestamp = '$finish_timestamp
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : finish_timestamp = '$finish_timestamp >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : devices_list = '$devices_list
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : devices_list = '$devices_list >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK' >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : *** ОТЧЕТЫ ПО МЕТРИКАМ VMSTAT/IOSTAT '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : *** ОТЧЕТЫ ПО МЕТРИКАМ VMSTAT/IOSTAT ' >> $LOG_FILE

##################################################################################################################################
# 1. OS - VMSTAT CORRELATION
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО КОРРЕЛЯЦИИ ОЖИДАНИЙ СУБД И МЕТРИК VMSTAT - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО КОРРЕЛЯЦИИ ОЖИДАНИЙ СУБД И МЕТРИК VMSTAT - НАЧАТ ' >> $LOG_FILE

REPORT_FILE=$current_path'/linux.1.waitings_vmstat_corr.txt'

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_waitings_os_corr('$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО КОРРЕЛЯЦИИ ОЖИДАНИЙ СУБД И МЕТРИК VMSTAT - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО КОРРЕЛЯЦИИ ОЖИДАНИЙ СУБД И МЕТРИК VMSTAT - НАЧАТ ' >> $LOG_FILE
# 1. OS - VMSTAT CORRELATION
##################################################################################################################################

##################################################################################################################################
# 2. VMSTAT/IOSTAT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО КОРРЕЛЯЦИИ МЕТРИК VMSTAT И IOSTAT - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО КОРРЕЛЯЦИИ МЕТРИК VMSTAT И IOSTAT - НАЧАТ ' >> $LOG_FILE

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
  
  REPORT_FILE=$current_path'/linux.2.vmstat_iostat_'$device'.txt'
  
  psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_vmstat_iostat('$start_timestamp' , '$finish_timestamp' , '$device'))" > $REPORT_FILE 2>$ERR_FILE
  exit_code $? $LOG_FILE $ERR_FILE
  
  chmod 777 $REPORT_FILE
  mv $REPORT_FILE $REPORT_DIR

  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

  let i=i+1
done

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО КОРРЕЛЯЦИИ МЕТРИК VMSTAT И IOSTAT - ЗАКОНЧЕН '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО КОРРЕЛЯЦИИ МЕТРИК VMSTAT И IOSTAT - ЗАКОНЧЕН ' >> $LOG_FILE
# 2. VMSTAT/IOSTAT
##################################################################################################################################

##################################################################################################################################
# 3. IO CHECK
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ IO - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ IO - НАЧАТ' >> $LOG_FILE

REPORT_FILE=$current_path'/linux.3.vmstat_io.txt'
cpu_count=`nproc --all`

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_vmstat_io($cpu_count , '$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ IO - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ IO - ЗАКОНЧЕН' >> $LOG_FILE
# 3. IO CHECK
##################################################################################################################################

##################################################################################################################################
# 4. CPU CHECK
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ CPU - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ CPU - НАЧАТ' >> $LOG_FILE

REPORT_FILE=$current_path'/linux.4.vmstat_cpu.txt'
cpu_count=`nproc --all`

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_vmstat_cpu($cpu_count , '$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ CPU - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ CPU - ЗАКОНЧЕН' >> $LOG_FILE
# 4. CPU CHECK
##################################################################################################################################

##################################################################################################################################
# 5. RAM CHECK
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ RAM - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ RAM - НАЧАТ' >> $LOG_FILE

REPORT_FILE=$current_path'/linux.5.vmstat_ram.txt'
ram_all=` free -m | head -2 | tail -1 | awk -F " " '{print $2}'`

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_vmstat_ram($ram_all  , '$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ RAM - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ЧЕК-ЛИСТ RAM - ЗАКОНЧЕН' >> $LOG_FILE
# 5. RAM CHECK
##################################################################################################################################

##################################################################################################################################
# VMSTAT 
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : МЕТАДАННЫЕ VMSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : МЕТАДАННЫЕ VMSTAT' >> $LOG_FILE
REPORT_FILE=$current_path'/linux.x.vmstat_meta.txt'
psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_vmstat_meta('$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ДАННЫЕ ДЛЯ ГРАФИКОВ VMSTAT'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ДАННЫЕ ДЛЯ ГРАФИКОВ VMSTAT' >> $LOG_FILE
REPORT_FILE=$current_path'/linux.x.vmstat_4graph.txt'
psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_vmstat_4graph('$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE
# VMSTAT 
##################################################################################################################################


##################################################################################################################################
# IOSTAT
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
  
	  REPORT_FILE=$current_path'/linux.x.iostat_'$device'_meta.txt'
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : МЕТАДАННЫЕ IOSTAT : '$device
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : МЕТАДАННЫЕ IOSTAT : '$device >> $LOG_FILE
	  psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_iostat_device_meta('$start_timestamp' , '$finish_timestamp' , '$device'))" > $REPORT_FILE 2>$ERR_FILE
	  exit_code $? $LOG_FILE $ERR_FILE
	  chmod 777 $REPORT_FILE
	  mv $REPORT_FILE $REPORT_DIR
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE
	  
	  REPORT_FILE=$current_path'/linux.x.iostat_'$device'_4graph.txt'
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ДАННЫЕ ДЛЯ ГРАФИКОВ IOSTAT : '$device
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ДАННЫЕ ДЛЯ ГРАФИКОВ IOSTAT : '$device >> $LOG_FILE
	  psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_iostat_device_4graph('$start_timestamp' , '$finish_timestamp' , '$device'))" > $REPORT_FILE 2>$ERR_FILE
	  exit_code $? $LOG_FILE $ERR_FILE
	  chmod 777 $REPORT_FILE
	  mv $REPORT_FILE $REPORT_DIR
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
	  echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

  let i=i+1
done
# IOSTAT
##################################################################################################################################


echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK ' >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : *** ОТЧЕТЫ ПО ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЯМ СУБД '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : *** ОТЧЕТЫ ПО ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЯМ СУБД ' >> $LOG_FILE

##################################################################################################################################
# 1. CLUSTER PERFORMANCE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЯМ НА УРОВНЕ СУБД - НАЧАТ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЯМ НА УРОВНЕ СУБД - НАЧАТ ' >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : МЕТАДАННЫЕ ПО ОЖИДАНИЯМ И ПРОИЗВОДИТЕЛЬНОСТИ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : МЕТАДАННЫЕ ПО ОЖИДАНИЯМ И ПРОИЗВОДИТЕЛЬНОСТИ' >> $LOG_FILE
REPORT_FILE=$current_path'/postgres.1.cluster_report_meta.txt'
psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_cluster_report_meta('$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ДАННЫЕ ДЛЯ ГРАФИКОВ ПО ОЖИДАНИЯМ И ПРОИЗВОДИТЕЛЬНОСТИ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ДАННЫЕ ДЛЯ ГРАФИКОВ ПО ОЖИДАНИЯМ И ПРОИЗВОДИТЕЛЬНОСТИ' >> $LOG_FILE
REPORT_FILE=$current_path'/postgres.1.cluster_report_4graph.txt'
psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( reports_cluster_report_4graph('$start_timestamp' , '$finish_timestamp' ))" > $REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЯМ НА УРОВНЕ СУБД - ЗАКОНЧЕН '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : ОТЧЕТ ПО ПРОИЗВОДИТЕЛЬНОСТИ И ОЖИДАНИЯМ НА УРОВНЕ СУБД - ЗАКОНЧЕН ' >> $LOG_FILE
# CLUSTER PERFORMANCE
##################################################################################################################################

##################################################################################################################################
# ПОДГОТОВИТЬ СТАТИСТИКУ ПО SQL
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : РАСЧЕТ СТАТИСТИКИ ПО SQL - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report : РАСЧЕТ СТАТИСТИКИ ПО SQL - НАЧАТ' >> $LOG_FILE

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

REPORT_FILE=$current_path'/postgres.2.wait_event.txt'

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_wait_event_for_pareto( '$start_timestamp' , '$finish_timestamp' )) " >>$REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ ПО WAIT_EVENT(ДИАГРАММА ПАРЕТО)  - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ ПО WAIT_EVENT(ДИАГРАММА ПАРЕТО)  - ЗАКОНЧЕН' >> $LOG_FILE

# WAIT_EVENT FOR PARETO
####################################################################################################################################

####################################################################################################################################
# СЕМАНТИЧЕСКИЙ АНАЛИЗ ПО WAIT_EVENT
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : summary_report : ПОДГОТОВКА ПРОМПТОВ ПО WAIT_EVENT  '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : summary_report : ПОДГОТОВКА ПРОМПТОВ ПО WAIT_EVENT  ' >> $LOG_FILE

PROMPT_FILE=$current_path'/net.1.wait_event.prompt.txt' 
echo 'Выдели общие части из текста и найти смысловые совпадения. Сформируй краткий итог по необходимым мероприятиям в виде сводной таблицы.' > $PROMPT_FILE  
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : PROMPT FILE = '$PROMPT_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : PROMPT FILE = '$PROMPT_FILE >> $LOG_FILE
chmod 777 $PROMPT_FILE
mv $PROMPT_FILE $REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  FILE '$PROMPT_FILE' HAS BEEN MOVED to '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  FILE '$PROMPT_FILE' HAS BEEN MOVED to '$REPORT_DIR >> $LOG_FILE		

for wait_event_type in 'BufferPin' 'Extension' 'IO' 'IPC' 'Lock' 'LWLock' 'Timeout'
do 
echo  'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : '$wait_event_type
echo  'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : '$wait_event_type >> $LOG_FILE

  WAIT_EVENT_ADVICE_FILE=$current_path'/net.1.wait_event.'$wait_event_type'.txt'
  let min_id=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT get_min_id_4_tmp_wait_events('$wait_event_type')"` 2>$ERR_FILE
  let max_id=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT get_max_id_4_tmp_wait_events('$wait_event_type')"` 2>$ERR_FILE
  
  if [ $min_id -gt 0 ]
  then   
   
    for ((curr_id=$min_id; curr_id <= $max_id; curr_id++))
    do
      curr_wait_event=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT wait_event FROM tmp_wait_events WHERE id = $curr_id AND wait_event_type='$wait_event_type'"`
	  
      advice_text=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT advice_for_wait_event_by_id( $curr_id ) "` 2>$ERR_FILE
	  if [ "$advice_text" == "NEW" ]
	  then 
	    NEW_PROMPT_FILE=$current_path'/net.1.wait_event.new_prompt.'$curr_wait_event'.txt' 
		echo 'Как уменьшить количество событий ожидания wait_event= '$curr_wait_event' для СУБД PostgreSQL?' > $NEW_PROMPT_FILE    
		echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : NEW_PROMPT FILE = '$NEW_PROMPT_FILE
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : NEW_PROMPT FILE = '$NEW_PROMPT_FILE >> $LOG_FILE
		chmod 777 $NEW_PROMPT_FILE
        mv $NEW_PROMPT_FILE $REPORT_DIR
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  REPORT FILE '$NEW_PROMPT_FILE' HAS BEEN MOVED to '$REPORT_DIR
        echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  REPORT FILE '$NEW_PROMPT_FILE' HAS BEEN MOVED to '$REPORT_DIR >> $LOG_FILE		
	  else 
	   echo $advice_text >> $WAIT_EVENT_ADVICE_FILE
	   echo ' ' >> $WAIT_EVENT_ADVICE_FILE
	  fi 
    done	
	
   chmod 777 $WAIT_EVENT_ADVICE_FILE
   mv $WAIT_EVENT_ADVICE_FILE $REPORT_DIR
  
   echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  FILE '$WAIT_EVENT_ADVICE_FILE' HAS BEEN MOVED to '$REPORT_DIR
   echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  FILE '$WAIT_EVENT_ADVICE_FILE' HAS BEEN MOVED to '$REPORT_DIR >> $LOG_FILE		
  fi  

done

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ПОДГОТОВКА ПРОМПТОВ ПО WAIT_EVENT - ЗАКОНЧЕНА'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ПОДГОТОВКА ПРОМПТОВ ПО WAIT_EVENT - ЗАКОНЧЕНА' >> $LOG_FILE
# СЕМАНТИЧЕСКИЙ АНАЛИЗ ПО WAIT_EVENT
####################################################################################################################################


##################################################################################################################################
# QUERYID FOR PARETO
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ ПО SQL(ДИАГРАММА ПАРЕТО) - НАЧАТ'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ ПО SQL(ДИАГРАММА ПАРЕТО) - НАЧАТ' >> $LOG_FILE

REPORT_FILE=$current_path'/postgres.3.queryid.txt'

psql -d $expecto_db -U $expecto_user -Aqtc "SELECT unnest( report_queryid_for_pareto( '$start_timestamp' , '$finish_timestamp' )) " >>$REPORT_FILE 2>$ERR_FILE
exit_code $? $LOG_FILE $ERR_FILE

chmod 777 $REPORT_FILE
mv $REPORT_FILE $REPORT_DIR

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ '$REPORT_FILE' СОХРАНЕН В ПАПКЕ '$REPORT_DIR >> $LOG_FILE

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ ПО SQL(ДИАГРАММА ПАРЕТО)  - ЗАКОНЧЕН'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ОТЧЕТ ПО SQL(ДИАГРАММА ПАРЕТО)  - ЗАКОНЧЕН' >> $LOG_FILE

# QUERYID FOR PARETO
####################################################################################################################################

####################################################################################################################################
# СЕМАНТИЧЕСКИЙ АНАЛИЗ ПО SQL ЗАПРОСАМ
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : summary_report : ПОДГОТОВКА ПРОМПТОВ ПО SQL ЗАПРОСАМ '
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK : summary_report : ПОДГОТОВКА ПРОМПТОВ ПО SQL ЗАПРОСАМ  ' >> $LOG_FILE

PROMPT_FILE=$current_path'/net.2.sql.prompt.txt' 
echo 'Выдели ключевые паттерны SQL запросов , с уточнением - сколько раз встречается паттерн. Сформируй итоговую таблицу - какие паттерны используются для каждого queryid. Выдели ключевые особенности SQL запроса, использующего наибольшее количество паттернов.' > $PROMPT_FILE  
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : PROMPT FILE = '$PROMPT_FILE
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : PROMPT FILE = '$PROMPT_FILE >> $LOG_FILE
chmod 777 $PROMPT_FILE
mv $PROMPT_FILE $REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  FILE '$PROMPT_FILE' HAS BEEN MOVED to '$REPORT_DIR
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  FILE '$PROMPT_FILE' HAS BEEN MOVED to '$REPORT_DIR >> $LOG_FILE	

for wait_event_type in 'BufferPin' 'Extension' 'IO' 'IPC' 'Lock' 'LWLock' 'Timeout'
do 
echo  'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : '$wait_event_type
echo  'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : '$wait_event_type >> $LOG_FILE
  
  QUERYID_ADVICE_FILE=$current_path'/net.2.sql.'$wait_event_type'.txt'
  let min_id=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT get_min_id_tmp_queryid('$wait_event_type')"` 2>$ERR_FILE
  let max_id=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT get_max_id_tmp_queryid('$wait_event_type')"` 2>$ERR_FILE
  
  if [ $min_id -gt 0 ]
  then     
     
    for ((curr_id=$min_id; curr_id <= $max_id; curr_id++))
    do
		query_id=`psql -d $expecto_db -U $expecto_user -Aqtc "SELECT get_queryid_by_id("$curr_id")"` 2>$ERR_FILE
        SQL_QUERY=`psql -d $expecto_db -U $expecto_user -Aqtc  "SELECT get_sql_by_queryid($query_id)" 2>$ERR_FILE`
		echo ' ' >> $QUERYID_ADVICE_FILE 
		echo $query_id >> $QUERYID_ADVICE_FILE 
		echo ' ' >> $QUERYID_ADVICE_FILE 
		echo "$SQL_QUERY" >> $QUERYID_ADVICE_FILE 		
    done
	
	chmod 777 $QUERYID_ADVICE_FILE
    mv $QUERYID_ADVICE_FILE $REPORT_DIR

    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  REPORT FILE '$QUERYID_ADVICE_FILE' HAS BEEN MOVED to '$REPORT_DIR
    echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  REPORT FILE '$QUERYID_ADVICE_FILE' HAS BEEN MOVED to '$REPORT_DIR >> $LOG_FILE
  fi
done

echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ПОДГОТОВКА ПРОМПТОВ ПО SQL ЗАПРОСАМ - ЗАКОНЧЕНА'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ПОДГОТОВКА ПРОМПТОВ ПО SQL ЗАПРОСАМ - ЗАКОНЧЕНА' >> $LOG_FILE
# СЕМАНТИЧЕСКИЙ АНАЛИЗ ПО SQL ЗАПРОСАМ
####################################################################################################################################



##################################################################################################################################
# SQL LIST 
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ФОРМИРОВАНИЕ СПИСКА SQL - НАЧАТО'
echo 'TIMESTAMP : '$(date "+%d-%m-%Y %H:%M:%S") ' : OK :  summary_report :  ФОРМИРОВАНИЕ СПИСКА SQL - НАЧАТО' >> $LOG_FILE

REPORT_FILE=$current_path'/postgres.x.sql_list.txt'

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
