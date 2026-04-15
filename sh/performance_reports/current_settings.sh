#!/bin/sh
########################################################################################################
# current_settings.sh
# Текущие настройки PostgreSQL , Linux , VM
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


LOG_FILE=$current_path'/current_settings.log'
ERR_FILE=$current_path'/current_settings.err'
REPORT_DIR='/tmp/pg_expecto_reports'

expecto_db='expecto_db'
expecto_user='expecto_user'

cd $REPORT_DIR

##########################################################################################
# 1.НАСТРОЙКИ
REPORT_FILE='_1.settings.txt'
echo 'НАСТРОЙКИ СУБД и VM' > $REPORT_FILE 
psql -c 'select version()' >> $REPORT_FILE 
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
# МЕТОДОЛОГИЯ АНАЛИЗА PG_EXPECTO
cat $current_path'/'methodology.txt >> $REPORT_FILE
# НАСТРОЙКИ
##########################################################################################

exit 0 
