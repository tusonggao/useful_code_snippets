#!/bin/bash
#用户画像数据合并
export PYTHON_EGG_CACHE=./myeggs
yes_date=`date -d "-1 day" +%Y-%m-%d`
mon=`date -d "-1 day" +%Y-%m`
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
time1=$(date "+%Y-%m-%d %H:%M:%S")
echo $time1
export HADOOP_USER_NAME=hdfs
#用户属性与购买行为联合
impala-shell -q "
drop table if exists default.upi_join_upb;
create table upi_join_upb as 
 select 
  uuid() key_id ,
  user_id ,
  COALESCE(upi1.account_id,upb1.account_id) account_id,
  dev_id ,
  imei ,
  idfa ,
  buy_user_id ,
  phone_number_order ,
  phone_number_register, 
  buy_amount_total,
  buy_amount_last_year,
  buy_amount_last_half_year,
  buy_amount_last_month,
  buy_times_total,
  buy_times_last_year,
  buy_times_last_half_year,
  buy_times_last_month,
  buy_time_first,
  buy_time_last,
  buy_last_large_class,
  buy_last_small_class,
  address_num,
  province_code
from 
   (select * from USER_PROFILE_Identity_tmp where account_id is not null )upi1
full outer join
   (select * from USER_PROFILE_buy_events_tmp  where  account_id !='00000000-0000-0000-0000-000000000000'  )upb1
on 
   upi1.account_id=upb1.account_id 
union all
 select
  uuid() key_id ,
  user_id ,
  account_id,
  dev_id ,
  imei ,
  idfa ,
  null as buy_user_id,
  phone_number_order ,
  phone_number_register, 
  0 as buy_amount_total,
  0 as buy_amount_last_year,
  0 as buy_amount_last_half_year,
  0 as buy_amount_last_month,
  0 as buy_times_total,
  0 as buy_times_last_year,
  0 as buy_times_last_half_year,
  0 as buy_times_last_month,
  null as buy_time_first,
  null as buy_time_last,
  null as buy_last_large_class,
  null as buy_last_small_class,
  0 as address_num,
  null as province_code
   from 
   USER_PROFILE_Identity_tmp 
   where 
   account_id is null
union all
 select 
  uuid() as key_id ,
  null as user_id ,
  null as account_id,
  null as dev_id ,
  null as imei ,
  null as idfa ,
  buy_user_id ,
  null as phone_number_order ,
  null as phone_number_register, 
  buy_amount_total,
  buy_amount_last_year,
  buy_amount_last_half_year,
  buy_amount_last_month,
  buy_times_total,
  buy_times_last_year,
  buy_times_last_half_year,
  buy_times_last_month,
  buy_time_first,
  buy_time_last,
  buy_last_large_class,
  buy_last_small_class,
  address_num,
  province_code
   from 
   USER_PROFILE_buy_events_tmp 
   where 
   account_id ='00000000-0000-0000-0000-000000000000' 
   ;
 " 

#上表与用户在线访问行为联合
impala-shell -q  "
drop table  IF EXISTS upi_join_upb_join_upo;
create table upi_join_upb_join_upo as
SELECT 
*
FROM
	(select 
			  key_id ,
			  COALESCE(t1.user_id,upo.user_id)  user_id ,
			  COALESCE(t1.account_id,upo.account_id) account_id,
			  dev_id ,
			  imei ,
			  idfa ,
			  buy_user_id ,
			  phone_number_order ,
			  phone_number_register,
			  buy_amount_total,
			  buy_amount_last_year,
			  buy_amount_last_half_year,
			  buy_amount_last_month,
			  buy_times_total,
			  buy_times_last_year,
			  buy_times_last_half_year,
			  buy_times_last_month,
			  buy_time_first,
			  buy_time_last,
                          buy_last_large_class,
                          buy_last_small_class,
			  address_num,
			  province_code,
			  three_months_start_app_cnt ,
			  last_year_start_app_cnt  ,
			  three_months_online_duration ,
			  last_year_online_duration ,
			  mobile_os_type 
		from  
			 (select * from upi_join_upb where user_id is not null)upo
		full outer join
			 (select * from user_profile_ol_events_tmp where user_id is not null)t1
		on t1.user_id=upo.user_id 
	union all
		select 
			  key_id ,
			  COALESCE(t2.user_id,upo2.user_id)  user_id ,
			  COALESCE(t2.account_id,upo2.account_id) account_id,
			  dev_id ,
			  imei ,
			  idfa ,
			  buy_user_id ,
			  phone_number_order ,
			  phone_number_register,
			  buy_amount_total,
			  buy_amount_last_year,
			  buy_amount_last_half_year,
			  buy_amount_last_month,
			  buy_times_total,
			  buy_times_last_year,
			  buy_times_last_half_year,
			  buy_times_last_month,
			  buy_time_first,
			  buy_time_last,
                          buy_last_large_class,
                          buy_last_small_class,
			  address_num,
			  province_code,
			  three_months_start_app_cnt ,
			  last_year_start_app_cnt  ,
			  three_months_online_duration ,
			  last_year_online_duration ,
			  mobile_os_type 
		from  
			 (select * from upi_join_upb where account_id is not null)upo2
	full outer join
			 (select * from user_profile_ol_events_tmp where account_id is not null)t2 
		on upo2.account_id=t2.account_id and upo2.user_id!=t2.user_id
	union all	
	select 
		  key_id ,
		  user_id,
		  account_id,
		  dev_id ,
		  imei ,
		  idfa ,
		  buy_user_id ,
		  phone_number_order ,
		  phone_number_register,
		  buy_amount_total,
		  buy_amount_last_year,
		  buy_amount_last_half_year,
		  buy_amount_last_month,
		  buy_times_total,
		  buy_times_last_year,
		  buy_times_last_half_year,
		  buy_times_last_month,
		  buy_time_first,
		  buy_time_last,
                  buy_last_large_class,
                  buy_last_small_class,
		  address_num,
		  province_code,
		  0 as three_months_start_app_cnt ,
		  0 as last_year_start_app_cnt  ,
		  0 as three_months_online_duration ,
		  0 as last_year_online_duration ,
		  null as mobile_os_type 
		from 
		  upi_join_upb
		where
		  account_id is null and user_id is null
		)t 
where buy_user_id is not null or account_id is not null or idfa is not null or imei is not null or dev_id is not null		
"

#上表（行为、购买、属性 ）与打分表联合
impala-shell -q  " 
drop table  IF EXISTS personas_tmp;
create table personas_tmp as
SELECT 
          key_id ,
          user_id ,
          account_id,
          dev_id ,
          imei ,
          idfa ,
          buy_user_id ,
          phone_number_order ,
          phone_number_register,
          buy_amount_total,
          buy_amount_last_year,
          buy_amount_last_half_year,
          buy_amount_last_month,
          buy_times_total,
          buy_times_last_year,
          buy_times_last_half_year,
          buy_times_last_month,
          buy_time_first,
          buy_time_last,
          buy_last_large_class,
          buy_last_small_class,
          address_num,
          province_code,
          three_months_start_app_cnt ,
          last_year_start_app_cnt  ,
          three_months_online_duration ,
          last_year_online_duration ,
          mobile_os_type,
          date_weight  ,
          product_code ,
          event_type ,
          event_weight ,
          large_class ,
          score 
FROM 
	(
	    select
		  key_id ,
		  COALESCE(t1.user_id,tt1.user_id)  user_id ,
		  COALESCE(t1.account_id,tt1.account_id) account_id,
		  dev_id ,
		  imei ,
		  idfa ,
		  buy_user_id ,
		  phone_number_order ,
		  phone_number_register,
		  buy_amount_total,
		  buy_amount_last_year,
		  buy_amount_last_half_year,
		  buy_amount_last_month,
		  buy_times_total,
		  buy_times_last_year,
		  buy_times_last_half_year,
		  buy_times_last_month,
		  buy_time_first,
		  buy_time_last,
                  buy_last_large_class,
                  buy_last_small_class,
		  address_num,
		  province_code,
		  three_months_start_app_cnt ,
		  last_year_start_app_cnt  ,
		  three_months_online_duration ,
		  last_year_online_duration ,
		  mobile_os_type,
		  date_weight  ,
		  product_code ,
		  event_type ,
		  event_weight ,
		  large_class ,
		  score 
		 from 
		 (select * from upi_join_upb_join_upo where user_id is not null)t1
		 full outer join
		 (select * from tags_tmp where user_id is not null)tt1
		 on  
		  t1.user_id=tt1.user_id 
		union all
		  select
		  key_id ,
		  user_id ,
		  account_id,
		  dev_id ,
		  imei ,
		  idfa ,
		  buy_user_id ,
		  phone_number_order ,
		  phone_number_register,
		  buy_amount_total,
		  buy_amount_last_year,
		  buy_amount_last_half_year,
		  buy_amount_last_month,
		  buy_times_total,
		  buy_times_last_year,
		  buy_times_last_half_year,
		  buy_times_last_month,
		  buy_time_first,
		  buy_time_last,
                  buy_last_large_class,
                  buy_last_small_class,
		  address_num,
		  province_code,
		  three_months_start_app_cnt ,
		  last_year_start_app_cnt  ,
		  three_months_online_duration ,
		  last_year_online_duration ,
		  mobile_os_type,
		  0 as date_weight  ,
		  null as product_code ,
		  null as event_type ,
		  0 as event_weight ,
		  null as large_class ,
		  0 as score 
		  from
		  upi_join_upb_join_upo
		  where user_id is null and account_id is null
		union all
		select
		  uuid() key_id ,
		  null as user_id ,
		  null as account_id,
		  null as dev_id ,
		  null as imei ,
		  null as idfa ,
		  null as buy_user_id ,
		  null as phone_number_order ,
		  null as phone_number_register,
		  0 as buy_amount_total,
		  0 as buy_amount_last_year,
		  0 as buy_amount_last_half_year,
		  0 as buy_amount_last_month,
		  0 as buy_times_total,
		  0 as buy_times_last_year,
		  0 as buy_times_last_half_year,
		  0 as buy_times_last_month,
		  null as buy_time_first,
		  null as buy_time_last,
                  null as buy_last_large_class,
                  null as buy_last_small_class,
		  0 as address_num,
		  null as province_code,
		  0 as three_months_start_app_cnt ,
		  0 as last_year_start_app_cnt  ,
		  0 as three_months_online_duration ,
		  0 as last_year_online_duration ,
		  null as mobile_os_type,
		  date_weight  ,
		  product_code ,
		  event_type ,
		  event_weight ,
		  large_class ,
		  score 
		 from 
		  tags_tmp
		  where user_id is null  and account_id is null
		union all
		   select
		  key_id ,
		  COALESCE(t2.user_id,tt2.user_id) as  user_id ,
		  COALESCE(t2.account_id,tt2.account_id) as account_id,
		  dev_id ,
		  imei ,
		  idfa ,
		  buy_user_id ,
		  phone_number_order ,
		  phone_number_register,
		  buy_amount_total,
		  buy_amount_last_year,
		  buy_amount_last_half_year,
		  buy_amount_last_month,
		  buy_times_total,
		  buy_times_last_year,
		  buy_times_last_half_year,
		  buy_times_last_month,
		  buy_time_first,
		  buy_time_last,
                  buy_last_large_class,
                  buy_last_small_class,
		  address_num,
		  province_code,
		  three_months_start_app_cnt ,
		  last_year_start_app_cnt  ,
		  three_months_online_duration ,
		  last_year_online_duration ,
		  mobile_os_type,
		  date_weight  ,
		  product_code ,
		  event_type ,
		  event_weight ,
		  large_class ,
		  score 
		 from 
		  (select * from upi_join_upb_join_upo where account_id is not null)t2
		 full outer join
		  (select * from tags_tmp where account_id is not null)tt2
		on  
		  t2.account_id=tt2.account_id and  t2.user_id!=tt2.user_id
		  )t
	GROUP BY
		  key_id ,
          user_id ,
          account_id,
          dev_id ,
          imei ,
          idfa ,
          buy_user_id ,
          phone_number_order ,
          phone_number_register,
          buy_amount_total,
          buy_amount_last_year,
          buy_amount_last_half_year,
          buy_amount_last_month,
          buy_times_total,
          buy_times_last_year,
          buy_times_last_half_year,
          buy_times_last_month,
          buy_time_first,
          buy_time_last,
          buy_last_large_class,
          buy_last_small_class,
          address_num,
          province_code,
          three_months_start_app_cnt ,
          last_year_start_app_cnt  ,
          three_months_online_duration ,
          last_year_online_duration ,
          mobile_os_type,
          date_weight  ,
          product_code ,
          event_type ,
          event_weight ,
          large_class ,
          score 
;"

#导入数据到最终表
#1.（如不存在）建表
impala-shell -q"
CREATE TABLE IF NOT EXISTS jkbd.ads_user_profile(
  \`key_id\` string COMMENT 'uuid()',
  \`dev_id\` string COMMENT 'device id',
  \`idfa\` string,
  \`imei\` string,
  \`account_id\` string,
  \`buy_user_id\` string,
  \`phone_number_order\` string COMMENT 'last order phone number',
  \`phone_number_register\` string COMMENT 'registe phone number',
  \`buy_amount_total\` double,
  \`buy_amount_last_year\` double,
  \`buy_amount_last_half_year\` double,
  \`buy_amount_last_month\` double,
  \`buy_times_total\` double,
  \`buy_times_last_year\` double,
  \`buy_times_last_half_year\` double,
  \`buy_times_last_month\` double,
  \`buy_time_first\` timestamp, -- 首次下单时间, added by tusonggao
  \`buy_time_last\` timestamp,
  \`buy_last_large_class\` string,  -- 最近一次下单购买商品的大类别(若有多个商品，则取花费金额最高的那个商品), added by tusonggao
  \`buy_last_small_class\` string,  -- 最近一次下单购买商品的小类别(若有多个商品，则取花费金额最高的那个商品), added by tusonggao
  \`address_num\` bigint COMMENT 'number of address',
  \`province_code\` string COMMENT 'code of province',
  \`cate_tag_1\` string COMMENT 'class1',
  \`cate_tag_2\` string COMMENT 'class2',
  \`cate_tag_3\` string COMMENT 'class3',
  \`browse_cate_1\` string COMMENT 'visit1',
  \`browse_cate_2\` string COMMENT 'visit2',
  \`browse_cate_3\` string COMMENT 'visit3',
  \`three_months_start_app_cnt\` bigint COMMENT 'days of starting APP in last three months',
  \`last_year_start_app_cnt\` double,
  \`three_months_online_duration\` double,
  \`last_year_online_duration\` double,
  \`mobile_os_type\` int COMMENT 'andriod: 0  ios: 1'
  )
PARTITIONED BY (\`dt\` string)
row format delimited fields terminated by '\u0001'
STORED AS PARQUET TBLPROPERTIES('parquet.compression'='SNAPPY');
"

#2.计算并导入结果
2.计算并导入结果
impala-shell -q"
insert overwrite table 
jkbd.ads_user_profile partition(dt='$yes_date')
SELECT 
*
FROM 
	(select
			pt.key_id,
			ifnull(dev_id,'') dev_id,
			ifnull(idfa,'')idfa,
			ifnull(imei,'') imei,
			ifnull(account_id,'') account_id,
			ifnull(buy_user_id,'') buy_user_id,
			ifnull(phone_number_order,'') phone_number_order,
			ifnull(phone_number_register,'') phone_number_register,
			ifnull(buy_amount_total,0) buy_amount_total,
			ifnull(buy_amount_last_year,0) buy_amount_last_year,
			ifnull(buy_amount_last_half_year,0) buy_amount_last_half_year,
			ifnull(buy_amount_last_month,0) buy_amount_last_month,
			ifnull(buy_times_total,0) buy_times_total,
			ifnull(buy_times_last_year,0) buy_times_last_year,
			ifnull(buy_times_last_half_year,0) buy_times_last_half_year,
			ifnull(buy_times_last_month,0) buy_times_last_month,
			ifnull(buy_time_first,'') buy_time_first,
			ifnull(buy_time_last,'') buy_time_last,
                        ifnull(buy_last_large_class, '') buy_last_large_class, 
                        ifnull(buy_last_small_class, '') buy_last_small_class,
			ifnull(address_num,0) address_num,
			ifnull(province_code,'') province_code,
			ifnull(cate_tag_1,'') cate_tag_1,
			ifnull(cate_tag_2,'') cate_tag_2,
			ifnull(cate_tag_3,'') cate_tag_3,
			ifnull(browse_cate_1,'') browse_cate_1,
			ifnull(browse_cate_2,'') browse_cate_2,
			ifnull(browse_cate_3,'') browse_cate_3,
			ifnull(three_months_start_app_cnt,0) three_months_start_app_cnt,
			ifnull(last_year_start_app_cnt,0) last_year_start_app_cnt,
			ifnull(three_months_online_duration,0) three_months_online_duration,
			ifnull(last_year_online_duration,0) last_year_online_duration,
			ifnull(mobile_os_type,-1) mobile_os_type
	from
		default.personas_tmp pt
	left outer join
		(
		select
			key_id,
			split_part(cate_tag_list,',',1) cate_tag_1,
			split_part(cate_tag_list,',',2) cate_tag_2,
			split_part(cate_tag_list,',',3)  cate_tag_3,
			split_part(cate_tag_score_list,',',1) cate_tag_score_1,
			split_part(cate_tag_score_list,',',2) cate_tag_score_2,
			split_part(cate_tag_score_list,',',3)  cate_tag_score_3
		from
			(
			select 
				key_id,
				group_concat(large_class, ',')  as cate_tag_list,
				group_concat(cast(score_rk as string), ',') as cate_tag_score_list
				from
				(select
				key_id,
				large_class,
				score_rk
			from
				(select
					key_id,
					large_class,
					row_number() over(partition by key_id order by class_score desc) as score_rk
				from
					(
					select
						key_id,
						large_class,
						sum(score) as class_score
					from
						default.personas_tmp 
					where 
						key_id is not null and large_class is not null and score is not null
					group by
						key_id,large_class
					)t0
				)t1
				where score_rk<=3 
				order by key_id,score_rk desc
			)t2
			group by key_id
			)t3
		)t4
	on pt.key_id=t4.key_id
	left outer join
			(
			select
			key_id,
			split_part(browse_cate_list,',',1) browse_cate_1,
			split_part(browse_cate_list,',',2) browse_cate_2,
			split_part(browse_cate_list,',',3)  browse_cate_3,
			split_part(browse_cate_score_list,',',1) browse_cate_score_1,
			split_part(browse_cate_score_list,',',2) browse_cate_score_2,
			split_part(browse_cate_score_list,',',3)  browse_cate_score_3
			from
			(
				select 
				key_id,
				group_concat(large_class, ',')  as browse_cate_list,
				group_concat(cast(times_rk as string), ',') as browse_cate_score_list
				from
				(select
				key_id,
				large_class,
				times_rk
				from
					(select
					key_id,
					large_class,
					row_number() over(partition by key_id order by class_visit_time desc) as times_rk
					from
						(
						select
							key_id,
							large_class,
							sum(case when event_type='浏览商品' or event_type='浏览商品（搜索）' then 1 else 0 end) as class_visit_time
						from
							default.personas_tmp 
						where 
							key_id is not null and large_class is not null and score is not null
						group by
							key_id,large_class
			)t5
			)t6
			where times_rk<=3 
			order by key_id,times_rk desc
			)t7
			group by key_id
			)t8
		)t9
			on pt.key_id=t9.key_id
	)t
where buy_user_id is not null or account_id is not null or idfa is not null or imei is not null or dev_id is not null	
" 


time2=$(date "+%Y-%m-%d %H:%M:%S") 
echo $time2
