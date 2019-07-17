CREATE TABLE IF NOT EXISTS sjcm.dwd_product_overview_product_class_daily(
	product_class_name string comment '商品大、小类',	
	product_kind_num bigint comment '商品种类之和',
	sale_product_kind_num  bigint comment '商品动销量（有销售记录的商品种类之和）', 
	product_pv bigint comment '商品浏览量',
	product_uv bigint comment '商品访客数',
	cart_add_user_num bigint comment '添加到购物车的人数',
	cart_add_product_num bigint comment '添加到购物车的件数',
	favorite_add_user_num bigint comment '添加到收藏夹的人数',
	order_user_num bigint comment '下单客户数',
	order_num bigint comment '订单数',
	sale_amount decimal(13,4) comment '销售金额',
	product_num bigint comment '成交件数',
	cost_amount decimal(13,4)  comment '成本金额',
	gross_profit decimal(13,4)  comment '毛利金额',
	create_time timestamp  comment '时间'
)
partitioned by(dt string)
row format delimited fields terminated by '\u0001';

insert overwrite table sjcm.dwd_product_overview_product_class_daily partition(dt)
select 
	t1.large_class,
	nvl(pkn.product_kind_num,0),
	nvl(dxl.sale_product_kind_num,0),
	nvl(t1.product_pv,0),
	nvl(t1.product_uv,0),
	nvl(jg.cart_add_user_num,0),
	nvl(jg.cart_add_product_num,0),
	nvl(shc.favorite_add_user_num,0),
	nvl(t1.order_user_num,0),
	nvl(odn.order_num,0),
	cast(t1.sale_amount as decimal(13,4)),
	nvl(t1.product_num,0),
	cast(t1.cost_amount as decimal(13,4)),
	cast(t1.gross_profit as decimal(13,4)),
	t1.order_date,
	t1.order_date as dt	
from

	(
	select 
		coalesce(uss.order_date,vst.event_date)  order_date,
		coalesce(uss.large_class,vst.large_class) large_class,
		nvl(order_user_num,0) order_user_num,
		nvl(sale_amount,0) sale_amount,
		nvl(product_num,0) product_num,
		nvl(cost_amount,0) cost_amount,
		nvl(gross_profit,0) gross_profit,
		nvl(product_pv,0) product_pv,
		nvl(product_uv,0) product_uv
	from	
	--下单用户数....毛利额
		(SELECT 
			order_date,
			large_class,
			count(distinct account_id) order_user_num,
			sum(product_amount) sale_amount,
			sum(product_quantity) product_num,
			sum(product_cost) cost_amount ,
			sum(product_amount-product_cost) gross_profit
		FROM jkbd.fact_export_detail_full
		WHERE order_date>=date_sub('${var:yes_date}',${var:days}) and order_date<='${var:yes_date}'
			AND EL_RECORD_STATE = 2 
			AND EF_RECORD_STATE = 2 
			AND order_status not in (0, 5, 100, 200)
			AND order_type not in('门店','批发','批零一体化')
			AND form_rank = 1
			AND export_form_type != '退货单'
			AND product_code is not null
			AND is_group = 0
			AND is_gift = '非赠品'
			AND product_code!=857934
			AND product_code!=857932
		group by 	order_date,large_class
		)uss	
	full outer join		
	--流量pv  独立访客数 uv
		(select 	
			large_class,
			event_date,
			count(1) product_pv,
			count(distinct sa_distinct_id) product_uv	
		from	
			jkbd.fact_user_event_detail fd
		join jkbd.dim_product dp
		on   fd.product_code=dp.product_code and fd.product_code is not null and  event_cn in  ('浏览商品')
		group by
			large_class,event_date	

		)vst
	on uss.order_date= vst.event_date and uss.large_class=vst.large_class
	)t1	
left join
--商品种类之和
	(select count(distinct product_code) product_kind_num,large_class from jkbd.dim_product group by large_class)pkn
on t1.large_class=pkn.large_class
left join
--商品动销量（有销售记录的商品种类之和）
	(select count(distinct product_code) sale_product_kind_num,order_date,large_class from jkbd.fact_export_detail_full group by large_class,order_date)dxl	
on t1.large_class=dxl.large_class and t1.order_date=dxl.order_date
left join	
--加购人数  加购件数
	(select 	
		large_class,
		event_date,
		count(1) cart_add_product_num, 
		count(distinct sa_distinct_id) cart_add_user_num	
	from	
		jkbd.fact_user_event_detail fd
	join jkbd.dim_product dp
	on   fd.product_code=dp.product_code and fd.product_code is not null and  event_cn in  ('加入购物车')
	group by
		large_class,event_date

	)jg
on t1.large_class=jg.large_class and t1.order_date=jg.event_date
left join
--收藏人数
	(select 	
		large_class,
		event_date,
		count(distinct sa_distinct_id) favorite_add_user_num	
	from	
		jkbd.fact_user_event_detail fd
	join jkbd.dim_product dp
	on   fd.product_code=dp.product_code and fd.product_code is not null and  event_cn in  ('收藏商品')
	group by
		large_class,event_date
	)shc
on t1.large_class=shc.large_class and t1.order_date=shc.event_date	
left join	
--下单订单数
	(SELECT 
		order_date,
		large_class,
		count(distinct orders_code) order_num	
	FROM jkbd.fact_export_detail_full
	WHERE order_date>=date_sub('${var:yes_date}',${var:days}) and order_date<='${var:yes_date}'
		AND EL_RECORD_STATE = 2 
		AND EF_RECORD_STATE = 2 
		AND order_status not in (0, 5, 100, 200)
		AND order_type not in('门店','批发','批零一体化')
		AND form_rank = 1
		AND export_form_type != '退货单'
		AND product_code is not null
		AND sls_platform is not null
		AND sls_origin is not null
		AND is_group = 0
		AND is_gift = '非赠品'
		AND product_code!=857934
		AND product_code!=857932
	group by 	order_date,large_class
	)odn
on t1.large_class=odn.large_class and t1.order_date=odn.order_date


union all


select 
	t1.small_class,
	nvl(pkn.product_kind_num,0),
	nvl(dxl.sale_product_kind_num,0),
	t1.product_pv,
	t1.product_uv,
	nvl(jg.cart_add_user_num,0),
	nvl(jg.cart_add_product_num,0),
	nvl(shc.favorite_add_user_num,0),
	t1.order_user_num,
	odn.order_num,
	cast(t1.sale_amount as decimal(13,4)),
	t1.product_num,
	cast(t1.cost_amount as decimal(13,4)),
	cast(t1.gross_profit as decimal(13,4)),
	t1.order_date,
	t1.order_date as dt	
from

	(
	select 
		coalesce(uss.order_date,vst.event_date)  order_date,
		coalesce(uss.small_class,vst.small_class) small_class,
		nvl(order_user_num,0) order_user_num,
		nvl(sale_amount,0) sale_amount,
		nvl(product_num,0) product_num,
		nvl(cost_amount,0) cost_amount,
		nvl(gross_profit,0) gross_profit,
		nvl(product_pv,0) product_pv,
		nvl(product_uv,0) product_uv
	from	
	--下单用户数....毛利额
		(SELECT 
			order_date,
			small_class,
			count(distinct account_id) order_user_num,
			sum(product_amount) sale_amount,
			sum(product_quantity) product_num,
			sum(product_cost) cost_amount ,
			sum(product_amount-product_cost) gross_profit
		FROM jkbd.fact_export_detail_full
		WHERE order_date>=date_sub('${var:yes_date}',${var:days}) and order_date<='${var:yes_date}'
			AND EL_RECORD_STATE = 2 
			AND EF_RECORD_STATE = 2 
			AND order_status not in (0, 5, 100, 200)
			AND order_type not in('门店','批发','批零一体化')
			AND form_rank = 1
			AND export_form_type != '退货单'
			AND product_code is not null
			AND is_group = 0
			AND is_gift = '非赠品'
			AND product_code!=857934
			AND product_code!=857932
			and small_class not in ('中药饮片','医疗器械','其他')
		group by 	order_date,small_class
		)uss	
	full outer join		
	--流量pv  独立访客数 uv
		(select 	
			small_class,
			event_date,
			count(1) product_pv,
			count(distinct sa_distinct_id) product_uv	
		from	
			jkbd.fact_user_event_detail fd
		join jkbd.dim_product dp
		on   fd.product_code=dp.product_code and fd.product_code is not null and  event_cn in  ('浏览商品')
		group by
			small_class,event_date	

		)vst
	on uss.order_date= vst.event_date and uss.small_class=vst.small_class
	)t1	
left join
--商品种类之和
	(select count(distinct product_code) product_kind_num,small_class from jkbd.dim_product group by small_class)pkn
on t1.small_class=pkn.small_class
left join
--商品动销量（有销售记录的商品种类之和）
	(select count(distinct product_code) sale_product_kind_num,order_date,small_class from jkbd.fact_export_detail_full group by small_class,order_date)dxl	
on t1.small_class=dxl.small_class and t1.order_date=dxl.order_date
left join	
--加购人数  加购次数
	(select 	
		small_class,
		event_date,
		count(1) cart_add_product_num, 
		count(distinct sa_distinct_id) cart_add_user_num	
	from	
		jkbd.fact_user_event_detail fd
	join jkbd.dim_product dp
	on   fd.product_code=dp.product_code and fd.product_code is not null and  event_cn in  ('加入购物车')
	group by
		small_class,event_date

	)jg
on t1.small_class=jg.small_class and t1.order_date=jg.event_date
left join
--收藏人数
	(select 	
		small_class,
		event_date,
		count(distinct sa_distinct_id) favorite_add_user_num	
	from	
		jkbd.fact_user_event_detail fd
	join jkbd.dim_product dp
	on   fd.product_code=dp.product_code and fd.product_code is not null and  event_cn in  ('收藏商品')
	group by
		small_class,event_date
	)shc
on t1.small_class=shc.small_class and t1.order_date=shc.event_date	
left join	
--下单订单数
	(SELECT 
		order_date,
		small_class,
		count(distinct orders_code) order_num	
	FROM jkbd.fact_export_detail_full
	WHERE order_date>=date_sub('${var:yes_date}',${var:days}) and order_date<='${var:yes_date}'
		AND EL_RECORD_STATE = 2 
		AND EF_RECORD_STATE = 2 
		AND order_status not in (0, 5, 100, 200)
		AND order_type not in('门店','批发','批零一体化')
		AND form_rank = 1
		AND export_form_type != '退货单'
		AND product_code is not null
		AND sls_platform is not null
		AND sls_origin is not null
		AND is_group = 0
		AND is_gift = '非赠品'
		AND product_code!=857934
		AND product_code!=857932
	group by 	order_date,small_class
	)odn
on t1.small_class=odn.small_class and t1.order_date=odn.order_date;
