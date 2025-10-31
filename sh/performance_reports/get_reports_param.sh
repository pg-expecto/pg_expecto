#!/bin/sh
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

