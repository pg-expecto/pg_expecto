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
# current_settings.sh
# Текущие настройки PostgreSQL , Linux , VM
# version 9.1
# updated 19/05/2026
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
psql -Aqtc "select name , setting from pg_settings where not pending_restart and name NOT IN ('log_filename','listen_addresses','port unix_socket_directories','ssl','ssl_cert_file','ssl_key_file','ssl_ciphers','password_encryption' )" >> $REPORT_FILE
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

####################################################################################################################################
# Linux Settings 
echo ' ' >> $REPORT_FILE
$current_path'/'Linux_details.sh
LINUX_SETTINGS_FILE=$current_path'/Linux_details.txt'
cat $LINUX_SETTINGS_FILE >> $REPORT_FILE
# Linux Settings 
####################################################################################################################################


##########################################################################################
# МЕТОДОЛОГИЯ АНАЛИЗА PG_EXPECTO
echo ' ' >> $REPORT_FILE
cat $current_path'/'methodology.txt >> $REPORT_FILE
# НАСТРОЙКИ
##########################################################################################

exit 0 
