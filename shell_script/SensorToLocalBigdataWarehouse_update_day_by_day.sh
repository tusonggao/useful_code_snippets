#!/bin/bash

start_time=`date +%s`
now=$(date +"%Y-%m-%d %H:%M:%S")

echo "******************* START time: ${now}************************"

start_date='2019-07-13'
end_date='2019-05-15'

#first_date='2018-03-29'

#month=$1

for((i=0;i<=10000;i++));
do
    while true 
    do
        curtime=`date "+%H:%M:%S"`
        curStamp=`date -d "$curtime" +%s` 
        limtime="23:00:00"
        limStamp=`date -d "$limtime" +%s` 
        limtimeup="09:00:00"
        limStampup=`date -d "$limtimeup" +%s` 
	
        if [ $curStamp -ge $limStamp -o $curStamp -lt $limStampup ]; then
	    echo limtime = $limtime
	    echo limtimeup = $limtimeup
	    echo curtime = $curtime
            echo 'not in proper working time, i sleep for 300 seconds'
            sleep 300    
        else
            echo 'in proper working time'
            break
        fi
    done 

    day=`date -d " ${start_date} -$i day" +%Y-%m-%d`
    echo "day is ${day}"
    if [[ ${day} < ${end_date} ]]
    then
        break
    fi

####################################       START      #############################################################
##########################################导入events表#############################################################
#导入数据到临时表 

impala-shell -q "

use rawdata;
drop table if  exists  default.events_tmp;
create  table if not exists default.events_tmp(
event string,
user_id bigint,
distinct_id string,
event_duration DOUBLE,
_date TIMESTAMP,
path string,
_url string,
productid string,
platformtype string,
_referrer string,
_url_query string,
_url_path string,
latest_referrer string,
latest_traffic_source_type string,
product_sort bigint,
current_pagenum bigint,
city string,
province string,
os string,
model string,
carrier string,
manufacturer string,
network_type string,
search_key string,
short_url_key string,
is_first_day bigint,
referrer_host string,
product_number double,
order_id string,
downloadchannel string,
_utm_source string,
_utm_campaign string,
_utm_term string,
_utm_content string,
_utm_medium string,
_latest_utm_campaign string,
_latest_utm_content string,
_latest_utm_medium string,
_latest_utm_source string,
_latest_utm_term string, 
time TIMESTAMP,
-----------------------------------------

currentPage string,
currentPage_title string,
currentPage_CH string,
currentPage_EN string,
roomAppURL string,
roomName string,
roomProductId double,
roomSort double,
roomWebURL string,
floorName string,
floorModelType string,
floorSort double,
tabTitle string,
algorithm string,
entrance string,
type string,
nodeType string,
previousModuleType string,
isLogin bigint,
returnState string,
key string,
searchType string,
searchResultPage double,
searchResultNum double,
hasRecommend bigint,
searchResultPageNum double,
customer_id string,
project_name string,
productShelfState string,
productName string,
itemType string,
filterCondition string,
resultNum double,
mainProductID string,
setProductId string,
setProductOrder double,
recommendedProductID string,
recommendedProductOrder double,
click_place string,
combineid string,
combineName string,
result string,
orderType string,
loginMethod string,
loginIcon string,
localLogin string,
loginOperator string,
_ip string,
app_version string,
pagetime string
-----------------------------------------
)
row format delimited fields terminated by '\u0001';

insert overwrite table  default.events_tmp
/*SA_BEGIN(production)*/
select 
event,
user_id,
distinct_id,
\`\$event_duration\`,
\`date\`,
nvl(path,'') path,
\`\$url\`,
productid,
platformtype,
\`\$referrer\`,
\`\$url_query\`,
\`\$url_path\`,
\`\$latest_referrer\`,
\`\$latest_traffic_source_type\`,
cast(productSort as bigint) as productSort,
cast(currentPageNum as bigint) currentPageNum,
\`\$city\`,
\`\$province\`,
\`\$os\`,
\`\$model\`,
\`\$carrier\`,
\`\$manufacturer\`,
\`\$network_type\`,
key,
\`\$short_url_key\`,
\`\$is_first_day\`,
\`\$referrer_host\`,
productnumber,
orderid,
downloadchannel,
\`\$utm_source\`,
\`\$utm_campaign\`,
\`\$utm_term\`,
\`\$utm_content\`,
\`\$utm_medium\`,
\`\$latest_utm_campaign\`,
\`\$latest_utm_content\`,
\`\$latest_utm_medium\`,
\`\$latest_utm_source\`,
\`\$latest_utm_term\`,
time,
--------------------------------------------------------------------
currentPage,
currentPage_title,
currentPage_CH,
currentPage_EN,
roomAppURL,
roomName,
roomProductId,
roomSort,
roomWebURL,
floorName,
floorModelType,
floorSort,
tabTitle,
algorithm,
entrance,
type,
nodeType,
previousModuleType,
isLogin,
returnState,
key,
searchType,
searchResultPage,
searchResultNum,
hasRecommend,
searchResultPageNum,
customer_id,
project_name,
productShelfState,
productName,
itemType,
filterCondition,
resultNum,
mainProductID,
setProductId,
setProductOrder,
recommendedProductID,
recommendedProductOrder,
click_place,
combineid,
combineName,
result,
orderType,
loginMethod,
loginIcon,
localLogin,
loginOperator,
\`\$ip\`,
\`\$app_version\`,
pageTime
--------------------------------------------------------------------
from 
events where substr(to_date(date),1,10) ='$day'
/*SA_END*/;
" -i 172.25.66.11

#删除路径中原有的数据
hadoop fs -rm -r -f hdfs://nameservice01:8020/data/sensorData_events/${day}

#数据传输
hadoop distcp \
-bandwidth 30 \
-overwrite  \
-m 20 \
hdfs://172.25.66.13:8020/user/hive/warehouse/events_tmp/ \
hdfs://nameservice01:8020/data/sensorData_events/${day}

#更改目录权限
/usr/bin/hdfs dfs -chmod -R 777 /data/sensorData_events/
#impala数据导入
impala-shell -q"
drop table if  exists  default.events_tmp;
create  table if not exists default.events_tmp(
event string,
user_id bigint,
distinct_id string,
event_duration DOUBLE,
_date TIMESTAMP,
path string,
_url string,
productid string,
platformtype string,
_referrer string,
_url_query string,
_url_path string,
latest_referrer string,
latest_traffic_source_type string,
product_sort bigint,
current_pagenum bigint,
city string,
province string,
os string,
model string,
carrier string,
manufacturer string,
network_type string,
search_key string,
short_url_key string,
is_first_day bigint,
referrer_host string,
product_number double,
order_id string,
downloadchannel string,
_utm_source string,
_utm_campaign string,
_utm_term string,
_utm_content string,
_utm_medium string,
_latest_utm_campaign string,
_latest_utm_content string,
_latest_utm_medium string,
_latest_utm_source string,
_latest_utm_term string, 
time timestamp,
--------------------------------------------------------------------

currentPage string,
currentPage_title string,
currentPage_CH string,
currentPage_EN string,
roomAppURL string,
roomName string,
roomProductId double,
roomSort double,
roomWebURL string,
floorName string,
floorModelType string,
floorSort double,
tabTitle string,
algorithm string,
entrance string,
type string,
nodeType string,
previousModuleType string,
isLogin bigint,
returnState string,
key string,
searchType string,
searchResultPage double,
searchResultNum double,
hasRecommend bigint,
searchResultPageNum double,
customer_id string,
project_name string,
productShelfState string,
productName string,
itemType string,
filterCondition string,
resultNum double,
mainProductID string,
setProductId string,
setProductOrder double,
recommendedProductID string,
recommendedProductOrder double,
click_place string,
combineid string,
combineName string,
result string,
orderType string,
loginMethod string,
loginIcon string,
localLogin string,
loginOperator string,
_ip string,
app_version string,
pagetime string
--------------------------------------------------------------------
)
row format delimited fields terminated by '\u0001';
load data  inpath '/data/sensorData_events/${day}' OVERWRITE INTO  TABLE default.events_tmp;

create table if not exists jkbd.ods_sa_events(
event string,
user_id bigint,
distinct_id string,
event_duration DOUBLE,
_date TIMESTAMP,
path string,
_url string,
productid string,
platformtype string,
_referrer string,
_url_query string,
_url_path string,
latest_referrer string,
latest_traffic_source_type string,
product_sort bigint,
current_pagenum bigint,
city string,
province string,
os string,
model string,
carrier string,
manufacturer string,
network_type string,
search_key string,
short_url_key string,
is_first_day bigint,
referrer_host string,
product_number double,
order_id string,
downloadchannel string,
_utm_source string,
_utm_campaign string,
_utm_term string,
_utm_content string,
_utm_medium string,
_latest_utm_campaign string,
_latest_utm_content string,
_latest_utm_medium string,
_latest_utm_source string,
_latest_utm_term string, 
time timestamp,
--------------------------------------------------------------------

currentPage string,
currentPage_title string,
currentPage_CH string,
currentPage_EN string,
roomAppURL string,
roomName string,
roomProductId double,
roomSort double,
roomWebURL string,
floorName string,
floorModelType string,
floorSort double,
tabTitle string,
algorithm string,
entrance string,
type string,
nodeType string,
previousModuleType string,
isLogin bigint,
returnState string,
key string,
searchType string,
searchResultPage double,
searchResultNum double,
hasRecommend bigint,
searchResultPageNum double,
customer_id string,
project_name string,
productShelfState string,
productName string,
itemType string,
filterCondition string,
resultNum double,
mainProductID string,
setProductId string,
setProductOrder double,
recommendedProductID string,
recommendedProductOrder double,
click_place string,
combineid string,
combineName string,
result string,
orderType string,
loginMethod string,
loginIcon string,
localLogin string,
loginOperator string,
_ip string,
app_version string,
pagetime string
--------------------------------------------------------------------
)
partitioned by(dt string)
row format delimited fields terminated by '\u0001'
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

alter table jkbd.ods_sa_events drop if exists partition(dt='${day}');

INSERT OVERWRITE TABLE jkbd.ods_sa_events partition(dt) SELECT *,substr(cast(time as string),1,10) dt  from default.events_tmp;
"

echo "end process day is ${day}"
######################################## END ###############################################
done

end_time=`date +%s`
total_runtime=$((end_time-start_time))
hours_num=`expr ${total_runtime} / 3600`
minutes_num=`expr ${total_runtime} % 3600 / 60`
seconds_num=`expr ${total_runtime} % 3600 % 60`
echo "all finished, total process takes time: ${hours_num} hours, ${minutes_num} minutes, ${seconds_num} seconds"

now=$(date +"%Y-%m-%d %H:%M:%S")
echo "********************************* END, CURRENT time: ${now} ********************************"


