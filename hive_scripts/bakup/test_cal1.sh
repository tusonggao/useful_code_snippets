#!/bin/bash
#时间获取
export PYTHON_EGG_CACHE=./myeggs
mon=`date -d "-1 day" +%Y-%m`
#今天
today_date=`date -d "-0 day" +%Y-%m-%d`
#昨天
yes_date=`date -d "-1 day" +%Y-%m-%d`


#每日销售汇总表 
impala-shell -q"
create external table if not exists default.gbl
    (product_code bigint,
	min_price float)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
	location '/user/tmp/';

select 
	mn.large_class '商品大类',
	mn.product_code '商品编号',
	mn.product_name '商品名称',
	cast(jg.purchase_price/100 as decimal(10,2)) '成本价',
	gp.safety_stock '安全库存',
	cast(nvl(jg.sku_price,0)/100 as decimal(10,2)) '官网页面价',
	cast(gbl.min_price as decimal(10,2))  '页面套餐最低价',
	1月销售数量,
	2月销售数量,
	3月销售数量,
	4月销售数量,
	5月销售数量,
	cast(nvl(1月成交价,0) as decimal(10,2)) '1月成交价',
	cast(nvl(2月成交价,0) as decimal(10,2)) '2月成交价',
	cast(nvl(3月成交价,0) as decimal(10,2)) '3月成交价',
	cast(nvl(4月成交价,0) as decimal(10,2)) '4月成交价',
	cast(nvl(5月成交价,0) as decimal(10,2)) '5月成交价',
	cast(nvl(1月成交金额,0) as decimal(10,2)) '1月成交金额',
	cast(nvl(2月成交金额,0) as decimal(10,2)) '2月成交金额',
	cast(nvl(3月成交金额,0) as decimal(10,2)) '3月成交金额',
	cast(nvl(4月成交金额,0) as decimal(10,2)) '4月成交金额',
	cast(nvl(5月成交金额,0) as decimal(10,2)) '5月成交金额',
	cast(nvl(1月毛利率,0) as decimal(10,2)) '1月毛利率',
	cast(nvl(2月毛利率,0) as decimal(10,2)) '2月毛利率',	
	cast(nvl(3月毛利率,0) as decimal(10,2)) '3月毛利率',	
	cast(nvl(4月毛利率,0) as decimal(10,2)) '4月毛利率',
	cast(nvl(5月毛利率,0) as decimal(10,2)) '5月毛利率',
	
	nvl(product_pv_1,0) '1月pv',
	nvl(product_pv_2,0) '2月pv',
	nvl(product_pv_3,0) '3月pv',
	nvl(product_pv_4,0) '4月pv',
	nvl(product_pv_5,0) '5月pv',
	
	nvl(product_uv_1,0) '1月uv',
	nvl(product_uv_2,0) '2月uv',
	nvl(product_uv_3,0) '3月uv',
	nvl(product_uv_4,0) '4月uv',
	nvl(product_uv_5,0) '5月uv',
	
	
	case when product_uv_1=0 then 1 else round(nvl(ord1.order_user_num,0)/nvl(product_uv_1,0),3) end  '1月转化率',
	case when product_uv_2=0 then 1 else round(nvl(ord2.order_user_num,0)/nvl(product_uv_2,0),3) end  '2月转化率',
	case when product_uv_3=0 then 1 else round(nvl(ord3.order_user_num,0)/nvl(product_uv_3,0),3) end  '3月转化率',
	case when product_uv_4=0 then 1 else round(nvl(ord4.order_user_num,0)/nvl(product_uv_4,0),3) end  '4月转化率',
	case when product_uv_5=0 then 1 else round(nvl(ord5.order_user_num,0)/nvl(product_uv_5,0),3) end  '5月转化率'
from	
(select 
		product_code,
		product_name,
		large_class,
		sum(if(mon=1,product_quantity,0)) '1月销售数量',
		sum(if(mon=2,product_quantity,0)) '2月销售数量',
		sum(if(mon=3,product_quantity,0)) '3月销售数量',
		sum(if(mon=4,product_quantity,0)) '4月销售数量',
		sum(if(mon=5,product_quantity,0)) '5月销售数量',
		
		if(sum(if(mon=1,1,0)) !=0 ,sum(if(mon=1,avg_product_amount,0))/sum(if(mon=1,1,0)),0) '1月成交价',
		if(sum(if(mon=2,1,0)) !=0 ,sum(if(mon=2,avg_product_amount,0))/sum(if(mon=2,1,0)),0) '2月成交价',
		if(sum(if(mon=3,1,0)) !=0 ,sum(if(mon=3,avg_product_amount,0))/sum(if(mon=3,1,0)),0) '3月成交价',
		if(sum(if(mon=4,1,0)) !=0 ,sum(if(mon=4,avg_product_amount,0))/sum(if(mon=4,1,0)),0)  '4月成交价',
		if(sum(if(mon=5,1,0)) !=0 ,sum(if(mon=5,avg_product_amount,0))/sum(if(mon=5,1,0)),0)  '5月成交价',
	
		sum(if(mon=1,product_amount,0))  '1月成交金额',
		sum(if(mon=2,product_amount,0))  '2月成交金额',
		sum(if(mon=3,product_amount,0))  '3月成交金额',
		sum(if(mon=4,product_amount,0))  '4月成交金额',
		sum(if(mon=5,product_amount,0))  '5月成交金额',
		
		case when sum(if(mon=1,product_amount,0))!=0 then sum(if(mon=1,gross_profit_amount,0))/sum(if(mon=1,product_amount,0))  else 0 end '1月毛利率',
		case when sum(if(mon=2,product_amount,0))!=0 then sum(if(mon=2,gross_profit_amount,0))/sum(if(mon=2,product_amount,0))  else 0 end '2月毛利率',
		case when sum(if(mon=3,product_amount,0))!=0 then sum(if(mon=3,gross_profit_amount,0))/sum(if(mon=3,product_amount,0))  else 0 end '3月毛利率',
		case when sum(if(mon=4,product_amount,0))!=0 then sum(if(mon=4,gross_profit_amount,0))/sum(if(mon=4,product_amount,0))   else 0 end '4月毛利率',
		case when sum(if(mon=5,product_amount,0))!=0 then sum(if(mon=5,gross_profit_amount,0))/sum(if(mon=5,product_amount,0))  else 0 end '5月毛利率'
				
from
		(
		--帐号 日期 订单号 订单金额 订单产品数量 毛利
			SELECT
				sls_platform,
				case when sls_origin like '慢病管理%' then '慢病管理'
					 when sls_origin like 'BD平台%' then 'BD平台'
					 else sls_origin end as sls_origin,
				region_code,			
				account_id,
				order_date,
				orders_code,
				product_code,
				product_name,
				large_class,
				small_class,
				nvl(product_amount,0) product_amount,
				nvl(product_quantity,0) product_quantity,
				nvl(product_cost,0) product_cost, 
				case when  product_quantity!=0 then product_amount/product_quantity else 0 end as avg_product_amount,
				nvl((product_amount-product_cost),0) gross_profit_amount,
				month(order_time) mon
			FROM 
				jkbd.fact_export_detail_full
			WHERE 
				order_date>='2019-01-01' and order_date<='2019-05-22'
				and EL_RECORD_STATE = 2 
				AND EF_RECORD_STATE = 2 
				AND order_status not in (0, 5, 100, 200)
				AND order_type not in('门店','批发','批零一体化')
				AND form_rank = 1
				AND export_form_type != '退货单'
				AND is_group = 0
				AND is_gift = '非赠品'
				AND product_code not in (857934,857932)	
				AND large_class='肝病用药'
				AND sls_origin not like '实体药店-广东%'
				AND sls_origin not like '实体药店-广州%'
				AND sls_origin not like '实体药店-武汉%'
				AND sls_origin not like '实体药店-重庆%'
				
		)fed	
	group by 
		product_code,
		product_name,
		large_class)mn
--官网页面价
left join		
	(select
		pt.product_code,
		max(pt.purchase_price) purchase_price,
		max(ym.sku_price) sku_price
	from jkbd.product pt
	left join 
	(SELECT jk_id,sku_price from    jkbd.hbase_product_price_realtime  where shop= '健客页面价')ym
	on pt.product_code=ym.jk_id
	group by pt.product_code
	)jg
on mn.product_code=jg.product_code
--安全库存
left join
	(select product_code,max(safety_stock) safety_stock from jkbd.goods_parquet  group by product_code) gp
on mn.product_code=gp.product_code	
--套餐最低价
left join 
	(select product_code,min(min_price) min_price from default.gbl  group by product_code) gbl
on 	mn.product_code=gbl.product_code
left join
--流量pv  独立访客数 uv
	(
	select 	
		product_code,
		count(1) product_pv_1,
		count(distinct sa_distinct_id) product_uv_1	
	from	
	    jkbd.fact_user_event_detail 
	    where trunc(event_date ,'MM')='2019-01-01' and event_cn in  ('浏览商品')
	group by
		product_code
	)vst1
on mn.product_code=vst1.product_code
left join
	(
	select 	
		product_code,
		count(1) product_pv_2,
		count(distinct sa_distinct_id) product_uv_2	
	from	
	    jkbd.fact_user_event_detail 
	    where trunc(event_date ,'MM')='2019-02-01' and event_cn in  ('浏览商品')
	group by
		product_code
	)vst2
on mn.product_code=vst2.product_code	
left join
	(
	select 	
		product_code,
		count(1) product_pv_3,
		count(distinct sa_distinct_id) product_uv_3	
	from	
	    jkbd.fact_user_event_detail 
	    where trunc(event_date ,'MM')='2019-02-01' and event_cn in  ('浏览商品')
	group by
		product_code
	)vst3
on mn.product_code=vst3.product_code
left join	
	(
	select 	
		product_code,
		count(1) product_pv_4,
		count(distinct sa_distinct_id) product_uv_4	
	from	
	    jkbd.fact_user_event_detail 
	    where trunc(event_date ,'MM')='2019-04-01' and event_cn in  ('浏览商品')
	group by
		product_code
	)vst4
on mn.product_code=vst4.product_code
left join
	(
	select 	
		product_code,
		count(1) product_pv_5,
		count(distinct sa_distinct_id) product_uv_5	
	from	
	    jkbd.fact_user_event_detail 
	    where trunc(event_date ,'MM')='2019-05-01' and event_cn in  ('浏览商品')
	group by
		product_code
	)vst5
on mn.product_code=vst5.product_code

left join		
--浏览并下单的用户数order_user_num
    (select
		ll.product_code product_code,
		count(distinct ll.sa_distinct_id) order_user_num
	from	
		(select 	
			product_code,
			event_origin as origin,
			event_date,
			sa_distinct_id
		from	
			jkbd.fact_user_event_detail
		where 1=1 
			and trunc(event_date ,'MM')='2019-01-01'
			and event_cn = '浏览商品'
			and product_code is not null
		)ll
	join
	   (         
		select 	
			product_code,
			event_origin as origin,
			event_date,
			sa_distinct_id	
		from	
			jkbd.fact_user_event_detail
		where 1=1 
			and trunc(event_date ,'MM')='2019-01-01'
			and event_cn = '提交订单'
			and product_code is not null
	   )tj	
    on ll.product_code=tj.product_code  and ll.event_date=tj.event_date and ll.sa_distinct_id=tj.sa_distinct_id
	group by product_code
	)ord1	
on mn.product_code=ord1.product_code
left join	
	    (select
		ll.product_code product_code,
		count(distinct ll.sa_distinct_id) order_user_num
	from	
		(select 	
			product_code,
			event_origin as origin,
			event_date,
			sa_distinct_id
		from	
			jkbd.fact_user_event_detail
		where 1=1 
			and trunc(event_date ,'MM')='2019-02-01'
			and event_cn = '浏览商品'
			and product_code is not null
		)ll
	join
	   (         
		select 	
			product_code,
			event_origin as origin,
			event_date,
			sa_distinct_id	
		from	
			jkbd.fact_user_event_detail
		where 1=1 
			and trunc(event_date ,'MM')='2019-02-01'
			and event_cn = '提交订单'
			and product_code is not null
	   )tj	
    on ll.product_code=tj.product_code  and ll.event_date=tj.event_date and ll.sa_distinct_id=tj.sa_distinct_id
	group by product_code
	)ord2
on mn.product_code=ord2.product_code
left join	
	(select
		ll.product_code product_code,
		count(distinct ll.sa_distinct_id) order_user_num
	from	
		(select 	
			product_code,
			event_origin as origin,
			event_date,
			sa_distinct_id
		from	
			jkbd.fact_user_event_detail
		where 1=1 
			and trunc(event_date ,'MM')='2019-03-01'
			and event_cn = '浏览商品'
			and product_code is not null
		)ll
	join
	   (         
		select 	
			product_code,
			event_origin as origin,
			event_date,
			sa_distinct_id	
		from	
			jkbd.fact_user_event_detail
		where 1=1 
			and trunc(event_date ,'MM')='2019-03-01'
			and event_cn = '提交订单'
			and product_code is not null
	   )tj	
    on ll.product_code=tj.product_code  and ll.event_date=tj.event_date and ll.sa_distinct_id=tj.sa_distinct_id
	group by product_code
	)ord3
on mn.product_code=ord3.product_code
left join
	
	    (select
		ll.product_code product_code,
		count(distinct ll.sa_distinct_id) order_user_num
	from	
		(select 	
			product_code,
			event_origin as origin,
			event_date,
			sa_distinct_id
		from	
			jkbd.fact_user_event_detail
		where 1=1 
			and trunc(event_date ,'MM')='2019-04-01'
			and event_cn = '浏览商品'
			and product_code is not null
		)ll
	join
	   (         
		select 	
			product_code,
			event_origin as origin,
			event_date,
			sa_distinct_id	
		from	
			jkbd.fact_user_event_detail
		where 1=1 
			and trunc(event_date ,'MM')='2019-04-01'
			and event_cn = '提交订单'
			and product_code is not null
	   )tj	
    on ll.product_code=tj.product_code  and ll.event_date=tj.event_date and ll.sa_distinct_id=tj.sa_distinct_id
	group by product_code
	)ord4
on mn.product_code=ord4.product_code
left join	
	(select
		ll.product_code product_code,
		count(distinct ll.sa_distinct_id) order_user_num
	from	
		(select 	
			product_code,
			event_origin as origin,
			event_date,
			sa_distinct_id
		from	
			jkbd.fact_user_event_detail
		where 1=1 
			and trunc(event_date ,'MM')='2019-05-01'
			and event_cn = '浏览商品'
			and product_code is not null
		)ll
	join
	   (         
		select 	
			product_code,
			event_origin as origin,
			event_date,
			sa_distinct_id	
		from	
			jkbd.fact_user_event_detail
		where 1=1 
			and trunc(event_date ,'MM')='2019-05-01'
			and event_cn = '提交订单'
			and product_code is not null
	   )tj	
    on ll.product_code=tj.product_code  and ll.event_date=tj.event_date and ll.sa_distinct_id=tj.sa_distinct_id
	group by product_code
	)ord5
on mn.product_code=ord5.product_code	
" -B --output_delimiter="," --print_header -o hepatopathy_单品_月份_to_秦澄蕴.csv