#!/bin/bash
#用户画像数据生成
export PYTHON_EGG_CACHE=./myeggs
time1=$(date "+%Y-%m-%d %H:%M:%S")
echo $time1
#今天
today_date=`date -d "-0 day" +%Y-%m-%d`
#昨天
yes_date=`date -d "-1 day" +%Y-%m-%d`
#上个月
last_month_date=`date -d "-30 day" +%Y-%m-%d`
#三个月前
three_month_date=`date -d "-90 day" +%Y-%m-%d`
#半年前
half_year_date=`date -d "-180 day" +%Y-%m-%d`
#去年今日
last_year_date=`date -d "-365 day" +%Y-%m-%d`
#
impala-shell -q "
USE jkbd;
DROP TABLE IF EXISTS default.USER_PROFILE_Identity_tmp;
CREATE TABLE IF NOT EXISTS default.USER_PROFILE_Identity_tmp(
key_id string,
user_id double,
account_id string,
dev_id string,
imei string,
idfa string,
phone_number_order string,
phone_number_register string
)
row format delimited fields terminated by '\u0001'
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');
INSERT OVERWRITE TABLE default.USER_PROFILE_Identity_tmp
SELECT 
uuid() key_id,
user_id,
su.second_id as account_id,
su.deviceid AS dev_id,
su.imei,
su.idfa,
phone_number_order,
ac.consignee_phone as phone_number_register
FROM
account_parquet ac 
full OUTER JOIN
(SELECT id as user_id,second_id,deviceid,imei,idfa from sa_users   group by id,second_id,deviceid,imei,idfa)su
on  ac.id=su.second_id
LEFT OUTER JOIN
(SELECT 
ors.aconsignee_phone2 as phone_number_order,ors.account_id 
FROM 
orders ors
JOIN
(SELECT account_id,max(order_time) as ls_md FROM orders WHERE order_status in (50, 70, 150) and aconsignee_phone2 is not null group by account_id)t1
ON 
(ors.account_id=t1.account_id  AND ors.order_time=t1.ls_md)
where ors.account_id is not null
group by ors.account_id,aconsignee_phone2
)t3
ON su.second_id=t3.account_id;
"
##########################################################################################购买行为
#SELECT count(1) as sum FROM USErs WHERE FROM_unixtime(cast($first_visit_time/1000 as bigint),'yyyy-MM-dd')='2019-03-12' AND deviceid is not null /*SA(production)*/;
#SELECT count(1) as sum FROM  events WHERE date='2019-03-12' /*SA(production)*/;
impala-shell -q "
DROP TABLE IF EXISTS default.USER_PROFILE_buy_events_tmp;
CREATE TABLE IF NOT EXISTS default.USER_PROFILE_buy_events_tmp(
account_id string,
buy_user_id string,
buy_amount_total double,
buy_amount_last_year double,
buy_amount_last_half_year double,
buy_amount_last_month double,
buy_times_total double,
buy_times_last_year bigint,
buy_times_last_half_year bigint,
buy_times_last_month bigint,
buy_time_last timestamp,
address_num bigint,
province_code string,
cate_tag_1 string,
cate_tag_2 string,
cate_tag_3 string
)
row format delimited fields terminated by '\u0001'
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');
USE jkbd;
INSERT OVERWRITE TABLE  default.USER_PROFILE_buy_events_tmp
SELECT
t11.account_id,
t11.buy_user_id,
buy_amount_total,
buy_amount_last_year,
buy_amount_last_half_year,
buy_amount_last_month,
buy_times_total,
buy_times_last_year,
buy_times_last_half_year,
buy_times_last_month,
buy_time_last,
address_num,
province_code,
'' as cate_tag_1,
'' as cate_tag_2,
'' as cate_tag_3
from
(SELECT sum(order_cost) buy_amount_total,count(orders_code) buy_times_total,count(distinct adelivery_address) address_num,account_id,buy_user_id FROM orders_parquet WHERE order_status in (50, 70, 150) and buy_user_id is not null group by account_id,buy_user_id)t11
LEFT OUTER JOIN
(SELECT sum(order_cost) buy_amount_last_year,buy_user_id FROM orders_parquet WHERE order_status in (50, 70, 150) AND order_time>='$last_year_date' AND order_time<'$today_date' and buy_user_id is not null group by buy_user_id)t22
ON t11.buy_user_id=t22.buy_user_id
LEFT OUTER JOIN
(SELECT sum(order_cost) buy_amount_last_half_year,buy_user_id FROM orders_parquet WHERE order_status in (50, 70, 150) AND order_time>='$half_year_date' AND order_time<'$today_date' and buy_user_id is not null group by buy_user_id)t33
ON t11.buy_user_id=t33.buy_user_id
LEFT OUTER JOIN
(SELECT sum(order_cost) buy_amount_last_month,buy_user_id FROM orders_parquet WHERE order_status in (50, 70, 150) AND order_time>='$last_month_date' AND order_time<'$today_date' and buy_user_id is not null group by buy_user_id)t44
ON t11.buy_user_id=t44.buy_user_id
LEFT OUTER JOIN
(SELECT count(orders_code) buy_times_last_year,buy_user_id FROM orders_parquet  WHERE order_status in (50, 70, 150) AND order_time>='$last_year_date' AND order_time<'$today_date'  and buy_user_id is not null group by buy_user_id)t55
ON t11.buy_user_id=t55.buy_user_id
LEFT OUTER JOIN
(SELECT count(orders_code) buy_times_last_half_year,buy_user_id FROM orders_parquet  WHERE order_status in (50, 70, 150) AND order_time>='$half_year_date' AND order_time<'$today_date' and buy_user_id is not null  group by buy_user_id)t66
ON t11.buy_user_id=t66.buy_user_id
LEFT OUTER JOIN
(SELECT count(orders_code) buy_times_last_month,buy_user_id FROM orders_parquet  WHERE order_status in (50, 70, 150) AND order_time>='$last_month_date' AND order_time<'$today_date' and buy_user_id is not null  group by buy_user_id)t77
ON t11.buy_user_id=t77.buy_user_id
LEFT OUTER JOIN
(SELECT buy_user_id,max(order_time) as buy_time_last FROM orders_parquet WHERE order_status in (50, 70, 150)  and buy_user_id is not null group by buy_user_id)t1
ON t11.buy_user_id=t1.buy_user_id
LEFT OUTER JOIN
(
SELECT 
ord1.buy_user_id as buy_user_id,ord1.region_code as province_code
FROM 
orders_parquet ord1
JOIN
(SELECT buy_user_id,max(order_time) max_region_time FROM orders_parquet WHERE order_status in (50, 70, 150) AND region_code is not null and order_time is not null and buy_user_id is not null group by buy_user_id)t2
on
(ord1.buy_user_id=t2.buy_user_id AND ord1.order_time=max_region_time)
group by buy_user_id,province_code
)t3
ON t11.buy_user_id=t3.buy_user_id
"
###################################################################################################在线行为计算
#在线行为部分
impala-shell -q "
DROP TABLE IF EXISTS default.user_profile_ol_events_tmp;
CREATE TABLE IF NOT EXISTS default.user_profile_ol_events_tmp(
user_id double,
account_id string,
three_months_start_app_cnt bigint,
last_year_start_app_cnt bigint ,
three_months_online_duratiON double,
last_year_online_duratiON double,
mobile_os_type int
)
row format delimited fields terminated by '\u0001'
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');
USE jkbd;
INSERT OVERWRITE TABLE default.user_profile_ol_events_tmp
SELECT 
t11.user_id,
t00.second_id as account_id,
three_months_start_app_cnt,
last_year_start_app_cnt,
three_months_online_duration,
last_year_online_duration,
mobile_os_type
from 
(SELECT user_id,count(distinct dt) last_year_start_app_cnt,sum(event_duration) last_year_online_duratiON  from sa_events WHERE dt>='${last_year_date}' and dt<'${today_date}' and user_id is not null group by user_id)t11
LEFT OUTER JOIN
(SELECT id,second_id from sa_users group by id,second_id)t00
on t11.user_id=t00.id
LEFT OUTER JOIN
(SELECT user_id,count(distinct dt) three_months_start_app_cnt,sum(event_duration) three_months_online_duratiON from sa_events WHERE dt>='${three_month_date}' and dt<'${today_date}' and user_id is not null group by user_id)t22
ON t11.user_id=t22.user_id
LEFT OUTER JOIN
(
SELECT 
account_id,
user_id,
case when idfa is not null then 1 when imei is not null then 0 else null end as mobile_os_type
from 
(SELECT 
id as user_id,second_id as account_id,idfa,imei 
from 
sa_users 
where 
imei is not null or idfa is not null 
group by id,second_id,idfa,imei
)t33
)t44
ON t11.user_id=t44.user_id
"
###############################################################################标签计算
#标签与类别
impala-shell -q "
DROP TABLE IF EXISTS default.tags_tmp;
CREATE TABLE IF NOT EXISTS default.tags_tmp(
user_id double,
account_id string,
\`_date\` timestamp,
date_weight double ,
product_code int,
event_type string,
event_weight int,
large_class string,
score double
)
row format delimited fields terminated by '\u0001'
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');
USE jkbd;
INSERT OVERWRITE TABLE default.tags_tmp 
SELECT e.*, pc.large_class, e.event_weight * e.date_weight score
from (
    SELECT 
        user_id,
        null as account_id,
        to_date('_date') as _date,
        case
            when datediff(now(), \`_date\`) <= 1 then 1
            when datediff(now(), \`_date\`) <= 7 and datediff(now(), \`_date\`) > 1 then 0.7
            when datediff(now(), \`_date\`) <= 30 and datediff(now(), \`_date\`) > 7 then 0.3
            else 0
        end as date_weight,
        cast(regexp_extract(_url, '/product/([0-9]+).html', 1) as int) product_code, 
        case 
            when parse_url(\`_referrer\`, 'PATH') like '/search%'
                or _url like 'https://search.jianke.com/prod%'
                or _url like 'http://search.jianke.com/prod%'   then '浏览商品（搜索）'
            else '浏览商品'
        end as event_type,
        case 
            when parse_url(\`_referrer\`, 'PATH') like '/search%'
                or _url like 'https://search.jianke.com/prod%'
                or _url like 'http://search.jianke.com/prod%'   then 5
            else 1
        end as event_weight
    from sa_events
    where  
	\`dt\` >= '$three_month_date'
	and parse_url(_url, 'PATH') like '/product%'
    and event = '$pageview'
    and regexp_extract(_url, '/product/([0-9]+).html', 1) is not null  
    union all
    SELECT 
        user_id,
        null as account_id,
        to_date(\`_date\`) as _date,
        case
            when datediff(now(), \`_date\`) <= 1 then 1
            when datediff(now(), \`_date\`) <= 7 and datediff(now(), \`_date\`) > 1 then 0.7
            when datediff(now(), \`_date\`) <= 30 and datediff(now(), \`_date\`) > 7 then 0.3
            else 0
        end as date_weight,
        cast(productid as int) as product_code,
        '浏览商品' event_type,
        1 event_weight
    from sa_events
    where 
	\`dt\` >= '$three_month_date'
    and event = 'viewProductDetail'
    and platformtype in ('商城APP_IOS', '商城APP_安卓')
    and productid is not null
    union all
    SELECT 
        user_id,
        null as account_id,
        to_date(\`_date\`) as _date,
        case
            when datediff(now(), \`_date\`) <= 1 then 1
            when datediff(now(), \`_date\`) <= 7 and datediff(now(), \`_date\`) > 1 then 0.7
            when datediff(now(), \`_date\`) <= 30 and datediff(now(), \`_date\`) > 7 then 0.3
            else 0
        end as date_weight,
        cast(regexp_extract(_url_query, 'productID=([0-9]+)', 1) as int) as product_code,
        '浏览商品' event_type,
        1 event_weight
    from sa_events
    where 
	dt >= '$three_month_date'
    and event = '$MPViewScreen'
    and _url_path = 'pages/productdetail/jkProductDetailsPage'
    and _url_query like '%productID%'
    union all 
    SELECT
        user_id,
        null as account_id,
        to_date(\`_date\`),
        case
            when datediff(now(), \`_date\`) <= 7 then 1
            when datediff(now(), \`_date\`) <= 30 and datediff(now(), \`_date\`) > 7 then 0.7
            when datediff(now(), \`_date\`) <= 90 and datediff(now(), \`_date\`) > 30 then 0.3
            else 0
        end as date_weight,
        cast(productid as int) as product_code,
        '加入购物车' event_type,
        15 event_weight
    from sa_events
    where 
	dt >= '$three_month_date'
    and event = 'addToShopcart'
    union all
    SELECT
        null as  user_id,
        account_id,
        to_date(order_time) as _date,
        case
            when datediff(now(), order_time) <= 7 then 1
            when datediff(now(), order_time) <= 30 and datediff(now(), order_time) > 7 then 0.7
            when datediff(now(), order_time) <= 90 and datediff(now(), order_time) > 30 then 0.3
            else 0
        end as date_weight,
        op.product_code,
        case 
            when order_status in (50, 70, 150) then '下单（完成）'
            when order_status in (0, 10, 100, 200, 60, 160, 80, 170, 180) then '下单（取消拒收）'
            else '下单（其它）'
        end  event_type,
        case 
            when order_status in (50, 70, 150) then 20
            when order_status in (0, 10, 100, 200, 60, 160, 80, 170, 180) then 6
            else 14
        end as event_weight
    from jkbd.orders_parquet o 
    join jkbd.order_products_parquet op on o.orders_code = op.orders_code 
	where o.order_time >= '$three_month_date'
    union all
    SELECT 
       null as user_id,
        account_id,
        to_date(creation_date) _date,
        case
            when datediff(now(), creation_date) <= 7 then 1
            when datediff(now(), creation_date) <= 30 and datediff(now(), creation_date) > 7 then 0.7
            when datediff(now(), creation_date) <= 90 and datediff(now(), creation_date) > 30 then 0.3
            else 0
        end as date_weight,
        product_code,
        '收藏商品' event_weight,
        10 as event_weight
    from jkbd.my_favorites_parquet 
	where creation_date >= '$three_month_date'
) e join jkbd.product_class_parquet pc on e.product_code = pc.product_code
"



time2=$(date "+%Y-%m-%d %H:%M:%S")
echo $time2














