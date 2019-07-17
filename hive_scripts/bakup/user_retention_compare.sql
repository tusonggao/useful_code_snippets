CREATE TABLE IF NOT EXISTS sjcm.ads_user_retention_compare(
	start_date timestamp comment '开始日期',
	end_date timestamp comment '结束日期',
	user_type  string comment '用户类型', 
	compare_type string comment '对比方式',
	compare_conetent string comment '对比内容',
	date_interval string comment '留存区间',
	retention_rate double comment '留存率'
)
partitioned by(dt string)
row format delimited fields terminated by '\u0001';


--产品访问情况
insert into table sjcm.ads_user_retention_compare partition(dt)
--根据平台--新用户
select
	'${var:start_date}' as start_date,
	'${var:end_date}' as end_date,
	'new' as user_type,
	'platform' as compare_type,
	platform as compare_conetent,
	'${var:t}' as date_interval,
	if(sum(if(t_1_times>0,1,0))=0,0,sum(if(t_times>0 and t_1_times>0,1,0))/sum(if(t_1_times>0,1,0))) retention_rate,
	'${var:end_date}' as dt
from	
	(select 
		event_user_id,
		platform,
		sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}',1,0)) t_1_times,
		sum(if(event_date>='${var:start_date}' and event_date<='${var:end_date}',1,0)) t_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			first_value(event_suborigin) over(partition by event_user_id order by event_date desc) platform
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and event_cn='App 启动'
			and event_user_id is not null
			and event_date>='${var:last_start_date}' and event_date<= '${var:end_date}'			
		)base
	where 	platform is not null	
	group by event_user_id,platform
	having sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}' and is_first_event_day=1,1,0))>0  --统计上一周期is_first_event_day这个字段如果不为0，则该周期是新用户，即可算作用户留存的新用户
	)ac
group by start_date,end_date,user_type,compare_type,compare_conetent,date_interval,dt
union
--老用户
select
	'${var:start_date}' as start_date,
	'${var:end_date}' as end_date,
	'old' as user_type,
	'platform' as compare_type,
	platform as compare_conetent,
	'${var:t}' as date_interval,
	if(sum(if(t_1_times>0,1,0))=0,0,sum(if(t_times>0 and t_1_times>0,1,0))/sum(if(t_1_times>0,1,0))) retention_rate,
	'${var:end_date}' as dt
from	
	(select 
		event_user_id,
		platform,
		sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}',1,0)) t_1_times,
		sum(if(event_date>='${var:start_date}' and event_date<='${var:end_date}',1,0)) t_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			first_value(event_suborigin) over(partition by event_user_id order by event_date desc) platform
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and event_cn='App 启动'
			and event_user_id is not null
			and event_date>='${var:last_start_date}' and event_date<= '${var:end_date}'			
		)base
	where 	platform is not null	
	group by event_user_id,platform
	having sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}' and is_first_event_day=1,1,0))=0
	)ac
group by start_date,end_date,user_type,compare_type,compare_conetent,date_interval,dt

union
--所有用户
select
	'${var:start_date}' as start_date,
	'${var:end_date}' as end_date,
	'all' as user_type,
	'platform' as compare_type,
	platform as compare_conetent,
	'${var:t}' as date_interval,
	if(sum(if(t_1_times>0,1,0))=0,0,sum(if(t_times>0 and t_1_times>0,1,0))/sum(if(t_1_times>0,1,0))) retention_rate,
	'${var:end_date}' as dt
from	
	(select 
		event_user_id,
		platform,
		sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}',1,0)) t_1_times,
		sum(if(event_date>='${var:start_date}' and event_date<='${var:end_date}',1,0)) t_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			first_value(event_suborigin) over(partition by event_user_id order by event_date desc) platform
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and event_cn='App 启动'
			and event_user_id is not null
			and event_date>='${var:last_start_date}' and event_date<= '${var:end_date}'			
		)base
	where 	platform is not null	
	group by event_user_id,platform
	)ac
group by start_date,end_date,user_type,compare_type,compare_conetent,date_interval,dt


union
--根据渠道--新用户
select
	'${var:start_date}' as start_date,
	'${var:end_date}' as end_date,
	'new' as user_type,
	'channel' as compare_type,
	channel as compare_conetent,
	'${var:t}' as date_interval,
	if(sum(if(t_1_times>0,1,0))=0,0,sum(if(t_times>0 and t_1_times>0,1,0))/sum(if(t_1_times>0,1,0))) retention_rate,
	'${var:end_date}' as dt
from	
	(select 
		event_user_id,
		channel,
		sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}',1,0)) t_1_times,
		sum(if(event_date>='${var:start_date}' and event_date<='${var:end_date}',1,0)) t_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			event_cn,
			first_value(download_channel) over(partition by event_user_id order by event_date desc) channel
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装') 
			and event_user_id is not null
			and event_date>='${var:last_start_date}' and event_date<= '${var:end_date}'			
		)base
	where 	channel is not null
	and event_cn='App 启动'
	group by event_user_id,channel
	having sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}' and is_first_event_day=1,1,0))>0
	)ac
group by start_date,end_date,user_type,compare_type,compare_conetent,date_interval,dt

union
--老用户
select
	'${var:start_date}' as start_date,
	'${var:end_date}' as end_date,
	'old' as user_type,
	'channel' as compare_type,
	channel as compare_conetent,
	'${var:t}' as date_interval,
	if(sum(if(t_1_times>0,1,0))=0,0,sum(if(t_times>0 and t_1_times>0,1,0))/sum(if(t_1_times>0,1,0))) retention_rate,
	'${var:end_date}' as dt
from	
	(select 
		event_user_id,
		channel,
		sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}',1,0)) t_1_times,
		sum(if(event_date>='${var:start_date}' and event_date<='${var:end_date}',1,0)) t_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			event_cn,
			first_value(download_channel) over(partition by event_user_id order by event_date desc) channel
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装') 
			and event_user_id is not null
			and event_date>='${var:last_start_date}' and event_date<= '${var:end_date}'			
		)base
	where 	channel is not null
	and event_cn='App 启动'
	group by event_user_id,channel
	having sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}' and is_first_event_day=1,1,0))=0
	)ac
group by start_date,end_date,user_type,compare_type,compare_conetent,date_interval,dt

union
--所有用户
select
	'${var:start_date}' as start_date,
	'${var:end_date}' as end_date,
	'all' as user_type,
	'channel' as compare_type,
	channel as compare_conetent,
	'${var:t}' as date_interval,
	if(sum(if(t_1_times>0,1,0))=0,0,sum(if(t_times>0 and t_1_times>0,1,0))/sum(if(t_1_times>0,1,0))) retention_rate,
	'${var:end_date}' as dt
from	
	(select 
		event_user_id,
		channel,
		sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}',1,0)) t_1_times,
		sum(if(event_date>='${var:start_date}' and event_date<='${var:end_date}',1,0)) t_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			event_cn,
			first_value(download_channel) over(partition by event_user_id order by event_date desc) channel
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装') 
			and event_user_id is not null
			and event_date>='${var:last_start_date}' and event_date<= '${var:end_date}'			
		)base
	where 	channel is not null
	and event_cn='App 启动'
	group by event_user_id,channel
	)ac
group by start_date,end_date,user_type,compare_type,compare_conetent,date_interval,dt




union
--根据版本--新用户
select
	'${var:start_date}' as start_date,
	'${var:end_date}' as end_date,
	'new' as user_type,
	'app_version' as compare_type,
	app_version as compare_conetent,
	'${var:t}' as date_interval,
	if(sum(if(t_1_times>0,1,0))=0,0,sum(if(t_times>0 and t_1_times>0,1,0))/sum(if(t_1_times>0,1,0))) retention_rate,
	'${var:end_date}' as dt
from	
	(select 
		event_user_id,
		app_version,
		sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}',1,0)) t_1_times,
		sum(if(event_date>='${var:start_date}' and event_date<='${var:end_date}',1,0)) t_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			first_value(app_version) over(partition by event_user_id order by event_date desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and event_cn='App 启动'
			and event_user_id is not null
			and event_date>='${var:last_start_date}' and event_date<= '${var:end_date}'			
		)base
	where 	app_version is not null
	group by event_user_id,app_version,is_first_event_day
	having sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}' and is_first_event_day=1,1,0))>0
	)ac
group by start_date,end_date,user_type,compare_type,compare_conetent,date_interval,dt


union
--老用户
select
	'${var:start_date}' as start_date,
	'${var:end_date}' as end_date,
	'old' as user_type,
	'app_version' as compare_type,
	app_version as compare_conetent,
	'${var:t}' as date_interval,
	if(sum(if(t_1_times>0,1,0))=0,0,sum(if(t_times>0 and t_1_times>0,1,0))/sum(if(t_1_times>0,1,0))) retention_rate,
	'${var:end_date}' as dt
from	
	(select 
		event_user_id,
		app_version,
		sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}',1,0)) t_1_times,
		sum(if(event_date>='${var:start_date}' and event_date<='${var:end_date}',1,0)) t_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			first_value(app_version) over(partition by event_user_id order by event_date desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and event_cn='App 启动'
			and event_user_id is not null
			and event_date>='${var:last_start_date}' and event_date<= '${var:end_date}'			
		)base
	where 	app_version is not null
	group by event_user_id,app_version,is_first_event_day
	having sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}' and is_first_event_day=1,1,0))=0
	)ac
group by start_date,end_date,user_type,compare_type,compare_conetent,date_interval,dt

union
--所有用户
select
	'${var:start_date}' as start_date,
	'${var:end_date}' as end_date,
	'all' as user_type,
	'app_version' as compare_type,
	app_version as compare_conetent,
	'${var:t}' as date_interval,
	if(sum(if(t_1_times>0,1,0))=0,0,sum(if(t_times>0 and t_1_times>0,1,0))/sum(if(t_1_times>0,1,0))) retention_rate,
	'${var:end_date}' as dt
from	
	(select 
		event_user_id,
		app_version,
		sum(if(event_date>='${var:last_start_date}' and event_date<='${var:last_end_date}',1,0)) t_1_times,
		sum(if(event_date>='${var:start_date}' and event_date<='${var:end_date}',1,0)) t_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			first_value(app_version) over(partition by event_user_id order by event_date desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and event_cn='App 启动'
			and event_user_id is not null
			and event_date>='${var:last_start_date}' and event_date<= '${var:end_date}'			
		)base
	where 	app_version is not null
	group by event_user_id,app_version,is_first_event_day
	)ac
group by start_date,end_date,user_type,compare_type,compare_conetent,date_interval,dt

