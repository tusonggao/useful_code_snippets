CREATE TABLE IF NOT EXISTS sjcm.ads_user_retention_trend(
	count_date timestamp comment '统计日期',
	install_user_cnt bigint comment '安装用户数',
	start_user_cnt bigint comment '启动用户数',
	user_type  string comment '用户类型', 
	platform string comment '平台',
	version string comment '版本',
	channel string comment '渠道',
	date_interval int comment '留存第几天',
	retention_rate double comment '留存率',
	retention_cnt bigint comment '留存用户数'
)
partitioned by(dt string)
row format delimited fields terminated by '\u0001';

drop view if exists views.visit_day_users_view;
create view if not exists views.visit_day_users_view
as
--1天
select 
	event_date,
	event_user_id,
	platform,
	channel,
	app_version,
	if(sum(is_first_event_day)=0,'old','new') user_type,
	sum(if(event_name='active',1,0)) t_active_times,
	sum(if(event_name='install',1,0)) t_install_times			
from
(select 	
	event_user_id,
	event_date,
	is_first_event_day,
	if(event_cn='App 启动','active','install') event_name,
	first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
	first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
	first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
from
	jkbd.fact_user_event_detail
WHERE
	1=1
	and (event_cn='App 启动' or event_cn='App 安装')
	and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)
)base
group by event_date,event_user_id,platform,channel,app_version
;






insert overwrite table sjcm.ads_user_retention_trend partition(dt)
--根据平台--新老用户
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	1 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,1))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval

union
--2天
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	2 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)		
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,2))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval

union
--3天
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	3 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)		
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,3))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval


union
--4天
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	4 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)		
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,4))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval


union
--5天
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	5 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)		
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,5))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval

union
--6天
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	6 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)		
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,6))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval

union
--7天
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	7 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)		
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,7))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval


union
--14天
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	14 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)		
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,14))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval


union
--30天
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	30 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)		
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,30))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval

union
--90天
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	90 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)		
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,90))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval

union
--180天
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	180 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)		
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,180))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval


union
--360天
select
	t0.event_date,
	sum(if(t_install_times>0,1,0)) install_user_cnt,
	sum(if(t_active_times>0,1,0)) start_user_cnt,
	t0.user_type,
	t0.platform,
	t0.app_version,
	t0.channel,
	360 as date_interval,
	if(sum(if(t_active_times>0,1,0))=0,0,sum(if(t1_active_times>0 and t_active_times>0,1,0))/sum(if(t_active_times>0,1,0))) retention_rate,
	sum(if(t1_active_times>0 and t_active_times>0,1,0)) retention_cnt,
	t0.event_date dt
from	
	views.visit_day_users_view t0
left join	
	(select 
		event_date,
		event_user_id,
		sum(if(event_name='active',1,0)) t1_active_times,
		sum(if(event_name='install',1,0)) t1_install_times
	from
		(select 	
			event_user_id,
			event_date,
			is_first_event_day,
			if(event_cn='App 启动','active','install') event_name,			
			first_value(event_suborigin) over(partition by event_user_id,event_date order by event_time desc) platform,
			first_value(download_channel) over(partition by event_user_id,event_date order by event_time desc) channel,
			first_value(app_version) over(partition by event_user_id,event_date order by event_time desc) app_version
		from
			jkbd.fact_user_event_detail
		WHERE
			1=1
			and (event_cn='App 启动' or event_cn='App 安装')
			and event_user_id is not null and event_date>=date_sub('${var:yes_date}',380)		
		)base
	group by event_date,event_user_id
	)t1
on t0.event_user_id=t1.event_user_id and t0.event_date=to_date(date_sub(t1.event_date,360))
group by t0.event_date,user_type,t0.platform,t0.app_version,t0.channel,date_interval

