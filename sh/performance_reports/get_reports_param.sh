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
# get_reports_param.sh 

script=$(readlink -f $0)
current_path=`dirname $script`

cfg_file=$current_path'/reports.conf'
parameter=$2
while read line
do
	first=`echo "$line" | awk -F " " '{print $1}'`
	if [[ $first == "#" ]];
	then 
		continue # next line
	fi
	
	if [[ $first == $parameter ]];
	then
	 value=`echo "$line" | awk -F "=" '{print $2}' |  sed 's/^[[:blank:]]*//;s/[[:blank:]]*$//'` 	 
	 echo "$value"
	 exit 0 
	fi
done <$cfg_file
echo 'ERROR : '$parameter' not found in the '$cfg_file
exit 1

