#!/bin/bash

#指定hadoop用户
export HADOOP_USER_NAME=hdfs
export PYTHON_EGG_CACHE=./myeggs

#昨日日期，月份
yes_date=`date -d "-1 day" +%Y-%m-%d`
mon=`date -d "-1 day" +%Y-%m`
echo "mon is ${mon}, yes_date is ${yes_date}"

###########################################events表#############################################################

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
------------------------------------------

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

-----------------------------------------
time TIMESTAMP
)
row format delimited fields terminated by '\u0001';

insert overwrite table  default.events_tmp
/*SA_BEGIN(production)*/
select 
event,
user_id,
distinct_id,
event_duration,
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
-------------------------------------------------------------------

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

--------------------------------------------------------------------
time
from 
events where to_date(date) ='$yes_date'
/*SA_END*/;
" -i 172.25.66.11

#清空文件夹，以防止之前有该文件夹中有遗留的文件，而发生冲突
hadoop fs -rm -r -f hdfs://nameservice01:8020/data/sensorData_events/${mon}/dt=${yes_date}

#数据传输
hadoop distcp \
-bandwidth 30 \
-overwrite  \
-m 20 \
hdfs://172.25.66.11:8020/user/hive/warehouse/events_tmp/ \
hdfs://nameservice01:8020/data/sensorData_events/${mon}/dt=${yes_date}

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
--------------------------------------------------------------------

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

--------------------------------------------------------------------
time timestamp
)
row format delimited fields terminated by '\u0001';
load data  inpath '/data/sensorData_events/${mon}/dt=${yes_date}' OVERWRITE INTO  TABLE default.events_tmp;

create  table if not exists jkbd.ods_sa_events(
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
--------------------------------------------------------------------

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

--------------------------------------------------------------------
time timestamp
)
partitioned by(dt string)
row format delimited fields terminated by '\u0001'
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE jkbd.ods_sa_events partition(dt) SELECT *,substr(cast(time as string),1,10) dt from default.events_tmp;
"

###########################################users表#############################################################


#导入数据到临时表
impala-shell -q"
drop table if  exists  default.users_tmp;
create  table if not exists default.users_tmp(
id BIGINT,
first_id string,
second_id string,
idfa string,
imei string,
uuid string,
deviceid string,
validity_id string,
push_state bigint,
_first_visit_time bigint
)
row format delimited fields terminated by '\u0001';
use rawdata;
insert overwrite table  default.users_tmp 
/*SA_BEGIN(production)*/
SELECT
\`id\`,
\`first_id\`,
\`second_id\`,
\`idfa\`,
\`imei\`,
\`uuid\`,
deviceid,
validity_id,
pushState,
\`\$first_visit_time\`
FROM
users
/*SA_END*/;
" -i 172.25.66.11

#数据传输
hadoop distcp \
-bandwidth 30 \
-overwrite  \
-m 20 \
hdfs://172.25.66.11:8020/user/hive/warehouse/users_tmp/ \
hdfs://nameservice01:8020/data/sensorData_users/${yes_date}
#更改目录权限
/usr/bin/hdfs dfs -chmod -R 777 /data/sensorData_users/
#hive数据导入
impala-shell -q"
drop table if exists default.users_tmp;
create  table if not exists default.users_tmp(
id bigint,
first_id string,
second_id string,
idfa string,
imei string,
uuid string,
deviceid string,
validity_id string,
push_state bigint,
_first_visit_time bigint
)
row format delimited fields terminated by '\u0001';
load data  inpath '/data/sensorData_users/${yes_date}' OVERWRITE INTO  TABLE default.users_tmp;

drop table if exists jkbd.ods_sa_users;
create  table if not exists jkbd.ods_sa_users(
id BIGINT,
first_id string,
second_id string,
idfa string,
imei string,
uuid string,
deviceid string,
validity_id string,
push_state bigint,
_first_visit_time bigint
)
row format delimited fields terminated by '\u0001'
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');
insert overwrite table jkbd.ods_sa_users SELECT * from default.users_tmp;
"

