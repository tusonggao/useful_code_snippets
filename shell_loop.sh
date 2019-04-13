#!/bin/bash

start_time=`date +%s`
now=$(date +"%Y-%m-%d %H:%M:%S")
echo "******************* START time: ${now}************************"

start_date='2019-03-29'
for((i=1;i<=1000;i++));
do
    yes_date=`date -d "-$i day" +%Y-%m-%d`
    if [[ ${yes_date} < ${start_date} ]]
    then
        break
    fi
    echo ${yes_date}
done

end_time=`date +%s`
total_runtime=$((end_time-start_time))
#echo "all finished, total process takes time: ${total_runtime} seconds"

total_runtime=10000
hours_num=`expr ${total_runtime} / 3600`
minutes_num=`expr ${total_runtime} % 3600 / 60`
seconds_num=`expr ${total_runtime} % 3600 % 60`

echo "all finished, total process takes time: ${hours_num} hours, ${minutes_num} minutes, ${seconds_num} seconds"
