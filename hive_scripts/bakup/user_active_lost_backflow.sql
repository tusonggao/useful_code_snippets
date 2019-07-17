
CREATE TABLE IF NOT EXISTS sjcm.dwd_order_product_daily(
	create_time timestamp comment '时间 ',
	product_code	int comment '产品ID',
	product_name string comment '产品名称',
	product_large_class  string comment '产品大类',
	product_small_class  string comment '产品小类', 
	stock_in_num int comment '入库数量',
	stock_in_amount decimal(13,4) comment '入库金额',
	half_year_sale_num int comment '半年销量',
	sale_amount decimal(13,4) comment '销售额',
	order_num bigint comment '订单数',
	gross_profit_amount decimal(13,4) comment '毛利额',
	sale_num bigint  comment '销售量',
	inventory bigint comment '当日商品库存',
	inventory_amount double comment '库存金额',
	inventory_day double  comment '库存周转天数',
	sale_amount_new_user double comment '新用户下单金额',
	gross_profit_amount_new_user double comment '新用户下单毛利额',
	new_user_num bigint  comment '下单新用户数',
	sale_amount_old_user double comment '老用户下单金额',
	gross_profit_amount_old_user double comment '老用户下毛利额',
	old_user_num bigint comment '下单老用户数',
	prod_sale_amount_new_user double comment '属于商品的新用户下单销售额',
	prod_gross_profit_amount_new_user double comment '属于商品新用户下单毛利额',
	prod_new_user_num bigint comment '属于商品的下单新用户数',
	prod_sale_amount_old_user double comment '属于商品的老用户下单销售额',
	prod_gross_profit_amount_old_user double comment '属于商品老用户下单毛利额',
	prod_old_user_num bigint comment '属于商品下单老用户数',
	order_fill bigint comment '订单满足数',
	order_export_num bigint comment '订单成功并且出库数',
	order_fill_24 bigint 	comment '24小时订单满足数',
	order_fill_48 bigint 	comment '48小时订单满足数',
	order_fill_72 bigint 	comment '72小时订单满足数'
)
partitioned by(dt string)
row format delimited fields terminated by '\u0001';


--创建view(用户第一次购买某个商品product_code的时间first_date)
CREATE VIEW IF NOT EXISTS jkbd.product_purchased_first_time as 
select 
   account_id,
   product_code,
   first_date
from   
    (select 
    	account_id,
    	product_code,
    	first_value(order_date) over(partition by account_id,product_code order by order_date asc) as first_date
    from
    	jkbd.fact_export_detail_full	
    )t	
GROUP BY
    account_id,
    product_code,
    first_date
;


INSERT OVERWRITE TABLE sjcm.dwd_order_product_daily partition(dt)
SELECT 
	cast(t1.create_time as string) as create_time,
	cast(nvl(t1.product_code,0) as int) product_code,
	ifnull(product_name,'') product_name,
	ifnull(product_large_class,'') product_large_class,
	ifnull(product_small_class,'') product_small_class,	
	cast(nvl(stock_in_num,0) as int) stock_in_num,
	cast(nvl(stock_in_amount,0) as DECIMAL(13,4)) stock_in_amount,	
	cast(nvl(half_year_sale_num,0) as int ) half_year_sale_num,
	cast(nvl(sale_amount,0) as DECIMAL(13,4)),
	nvl(order_num,0),
	cast(nvl(gross_profit_amount,0) as DECIMAL(13,4)),
	nvl(sale_num,0),
	cast(nvl(product_inventory,0) as bigint),
	nvl(inventory_amount,0),
	nvl(product_inventory*30/month_sale_num,0) inventory_day,
	nvl(sale_amount_new_user,0),
	nvl(gross_profit_amount_new_user,0),
	nvl(new_user_num,0),
	nvl(sale_amount_old_user,0),
	nvl(gross_profit_amount_old_user,0),
	nvl(old_user_num,0),
	nvl(prod_sale_amount_new_user,0),
	nvl(prod_gross_profit_amount_new_user,0),
	nvl(prod_new_user_num,0),
	nvl(prod_sale_amount_old_user,0),
	nvl(prod_gross_profit_amount_old_user,0),
	nvl(prod_old_user_num,0),
	nvl(order_fill,0),
	nvl(order_num_export,0),
	nvl(order_fill_24,0),
	nvl(order_fill_48,0),
	nvl(order_fill_72,0),
	cast(t1.create_time as string) as dt
FROM			
				
	(select 
		coalesce(sl.create_time,stg.storage_date) create_time,
		coalesce(sl.product_code,stg.product_code) product_code,
		coalesce(sl.product_name,stg.product_name) product_name,
		coalesce(sl.product_large_class,stg.product_large_class) product_large_class,
		coalesce( sl.product_small_class,stg.product_small_class) product_small_class,
		month_sale_num,
		half_year_sale_num,
		stock_in_num,
		stock_in_amount
	from
			--近半年销量,近30天销量	
			(
						select 
							order_date create_time,
							product_code,
							product_name,
							large_class as product_large_class,
							small_class as product_small_class,
							(sum(sale_num) over(partition by product_code order by order_date asc rows between 29 preceding and current row)) month_sale_num,
							(sum(sale_num) over(partition by product_code order by order_date asc rows between 179 preceding and current row)) half_year_sale_num
						from	
							(select	
								order_date,
								pro_dt.product_code,
								product_name,
								nvl(sale_num,0) as sale_num	,
								large_class,
								small_class
							from	
								(select 
										order_date,
										product_code,
										product_name,
										large_class ,
										small_class
									from 
										(select distinct order_date order_date from jkbd.fact_export_detail_full where order_date>=date_sub('${var:yes_date}',${var:days}+180))dat
									join
										(select product_code,product_name,large_class,small_class from jkbd.fact_export_detail_full where	product_code is not null  group by product_code,product_name,large_class,small_class)proco
							  
								)pro_dt
							left join
								(SELECT 
									order_date create_time,
									product_code,
									SUM(ifnull(product_quantity,0)) as sale_num				
								FROM jkbd.fact_export_detail_full
								WHERE   
										order_date>=date_sub('${var:yes_date}',${var:days}+180)
										AND EL_RECORD_STATE = 2 
										AND EF_RECORD_STATE = 2 
										AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
										AND order_type not in('门店','批发','批零一体化')
										AND form_rank = 1
										AND export_form_type != '退货单'
										AND is_group = 0
										AND is_gift = '非赠品'
										AND product_code not in (857932,857934)

								GROUP BY 
								create_time,
								product_code
								)hlf			
							on pro_dt.product_code=hlf.product_code 
							and pro_dt.order_date=hlf.create_time
						)tt2			
			)sl
	full outer JOIN
	--入库数量 入库金额
		(
		select 
			storage_date, 
			product_code,
			product_name,
			large_class as product_large_class,
			small_class as product_small_class,
			sum(storage_actual_quantity) stock_in_num,
			sum(storage_actual_amount) stock_in_amount
		from 
			jkbd.fact_storage_detail_full
		where 
			storage_date >= date_sub('${var:yes_date}',${var:days})		 
			and sf_record_state = 2
			and sl_record_state = 2
			and storage_form_status in (6)
			and storage_form_type in (2)
		group by 
			storage_date,product_code,product_name,product_large_class,product_small_class
		)stg
		ON
			sl.create_time=stg.storage_date 
			AND sl.product_code=stg.product_code
			AND sl.product_name=stg.product_name
			AND sl.product_large_class=stg.product_large_class
			AND sl.product_small_class=stg.product_small_class
	)t1		

	LEFT JOIN
		--产品销售额 毛利额   订单数量 销售数量 订单满足数 订单成功且出库数量
			(SELECT
				create_time,
				product_code,
				SUM(product_amount) AS sale_amount,
				SUM(product_margin_valid) AS gross_profit_amount,
				COUNT(DISTINCT orders_code) AS order_num,
				SUM(ifnull(product_quantity,0)) as sale_num
			FROM
				(
					SELECT 
						order_date AS create_time,
						orders_code,
						product_code,
						nvl(product_amount,0) product_amount,
						(nvl(product_amount,0)-nvl(product_cost,0)) as product_margin_valid,
						is_short_supply,
						product_quantity,
						dept_code
					FROM jkbd.fact_export_detail_full
					WHERE 
					    order_date>=date_sub('${var:yes_date}',${var:days})
						AND EL_RECORD_STATE = 2 
						AND EF_RECORD_STATE = 2 
						AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
						AND order_type not in('门店','批发','批零一体化')
						AND form_rank = 1
						AND export_form_type != '退货单'
						AND product_code is not null
						AND is_group = 0
						AND is_gift = '非赠品'
						AND product_code not in (857934,857932)						
				)t11
				GROUP BY 
				create_time,
				product_code		
			)t12
	ON t1.create_time=t12.create_time and t1.product_code=t12.product_code	
	left join
	--该日期产品的库存数量 库存金额
				(
				SELECT
					gs.product_code,
					create_date,
					sum(ifnull(ending_quantity,0)) as product_inventory,
					sum(ending_amount) as inventory_amount
				FROM 
					jkbd.dwd_goods_inventory_daily_parquet as dt
				join
					jkbd.goods_parquet gs
				on dt.goods_id=gs.id
				where create_date>=date_sub('${var:yes_date}',${var:days})
				group by gs.product_code,create_date		
				)inv
	on t1.create_time=inv.create_date and t1.product_code=inv.product_code
	left join
	--新用户销售数据
			(SELECT
				fe.product_code,
				fe.create_time,
				sum(product_amount) sale_amount_new_user,
				sum(product_margin_valid) gross_profit_amount_new_user,
				count(distinct orders_code) order_num_new_user				
			FROM
					(
					SELECT 
						order_date AS create_time,
						orders_code,
						product_code,
						product_amount,
						(nvl(product_amount,0)-nvl(product_cost,0)) as product_margin_valid,
						account_id						
					FROM jkbd.fact_export_detail_full
					WHERE order_date>=date_sub('${var:yes_date}',${var:days})
						AND EL_RECORD_STATE = 2 
						AND EF_RECORD_STATE = 2 
						AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
						AND order_type not in('门店','批发','批零一体化')
						AND form_rank = 1
						AND export_form_type != '退货单'
						AND product_code is not null					
						AND is_group = 0
						AND is_gift = '非赠品'
						AND product_code not in (857934,857932)
						AND account_id is not null							
					)fe
			join
					(
					select 
						account_id,
						all_frist_order_time
					from
						jkbd.dwd_account_order_time 
					where account_id is not null and 	all_frist_order_time is not null		
					)da
			ON 
				fe.account_id=da.account_id and create_time=to_date(all_frist_order_time)
			group by
				fe.product_code,
				fe.create_time	
			)new
	on t1.create_time=new.create_time and t1.product_code=new.product_code	
	left join
	--老用户销售数据
			(SELECT
				fe.product_code,
				fe.create_time,
				sum(product_amount) sale_amount_old_user,
				sum(product_margin_valid) gross_profit_amount_old_user,
				count(distinct orders_code) order_num_old_user		
			FROM
					(
					SELECT 
						order_date AS create_time,
						orders_code,
						product_code,
						product_amount,
						(nvl(product_amount,0)-nvl(product_cost,0)) as product_margin_valid,
						is_short_supply,
						account_id,
						product_quantity						
					FROM jkbd.fact_export_detail_full
					WHERE order_date>=date_sub('${var:yes_date}',${var:days})
						AND EL_RECORD_STATE = 2 
						AND EF_RECORD_STATE = 2 
						AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
						AND order_type not in('门店','批发','批零一体化')
						AND form_rank = 1
						AND export_form_type != '退货单'
						AND product_code is not null						
						AND is_group = 0
						AND is_gift = '非赠品'
						AND product_code!=857934
						AND product_code!=857932
						AND account_id is not null
						
					)fe
			left join
					(
					select 
						account_id,
						all_frist_order_time
					from
						jkbd.dwd_account_order_time 
					where account_id is not null 
						  and all_frist_order_time is not null		
					)da
			ON 
				fe.account_id=da.account_id and create_time=to_date(all_frist_order_time)
			where 	
				da.account_id is null and da.all_frist_order_time is null
			group by
				fe.product_code,
				fe.create_time				
			)old
	on t1.create_time=old.create_time and t1.product_code=old.product_code	
	left join
	--产品新用户销售数据
			(SELECT
				fe.product_code,
				fe.create_time,
				sum(product_amount) prod_sale_amount_new_user,
				sum(product_margin_valid) prod_gross_profit_amount_new_user,
				count(distinct fe.account_id)	 prod_new_user_num		
			FROM
					(
					SELECT 
						order_date AS create_time,
						orders_code,
						product_code,
						product_amount,
						(nvl(product_amount,0)-nvl(product_cost,0)) as product_margin_valid,
						is_short_supply,
						account_id						
					FROM jkbd.fact_export_detail_full
					WHERE order_date>=date_sub('${var:yes_date}',${var:days})
						AND EL_RECORD_STATE = 2 
						AND EF_RECORD_STATE = 2 
						AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
						AND order_type not in('门店','批发','批零一体化')
						AND form_rank = 1
						AND export_form_type != '退货单'
						AND product_code is not null					
						AND is_group = 0
						AND is_gift = '非赠品'
						AND product_code not in (857934,857932)
						AND account_id is not null
						
					)fe
			join
					jkbd.product_purchased_first_time pp
			ON 
				fe.account_id=pp.account_id and fe.create_time=pp.first_date and fe.product_code=pp.product_code
			group by
				fe.product_code,
				fe.create_time	
			)pro_new
	on t1.create_time=pro_new.create_time and t1.product_code=pro_new.product_code	
	left join
	--产品老用户销售数据
			(SELECT
				fe.product_code,
				fe.create_time,
				sum(product_amount) prod_sale_amount_old_user,
				sum(product_margin_valid) prod_gross_profit_amount_old_user,
				count(distinct fe.account_id)	 prod_old_user_num		
			FROM
					(
					SELECT 
						order_date AS create_time,
						orders_code,
						product_code,
						product_amount,
						(nvl(product_amount,0)-nvl(product_cost,0)) as product_margin_valid,
						is_short_supply,
						account_id,
						product_quantity						
					FROM jkbd.fact_export_detail_full
					WHERE order_date>=date_sub('${var:yes_date}',${var:days})
						AND EL_RECORD_STATE = 2 
						AND EF_RECORD_STATE = 2 
						AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
						AND order_type not in('门店','批发','批零一体化')
						AND form_rank = 1
						AND export_form_type != '退货单'
						AND product_code is not null						
						AND is_group = 0
						AND is_gift = '非赠品'
						AND product_code!=857934
						AND product_code!=857932
						AND account_id is not null
						
					)fe
			left join
					jkbd.product_purchased_first_time da
			ON 
				fe.account_id=da.account_id and create_time=da.first_date and fe.product_code=da.product_code
			where 	
				da.account_id is null  and da.first_date is null
			group by
				fe.product_code,
				fe.create_time				
			)pro_old
	on t1.create_time=pro_old.create_time and t1.product_code=pro_old.product_code	

left join
	--新用户数
			(SELECT
				fe.product_code,
				fe.create_time,
				count(distinct fe.account_id)	 new_user_num		
			FROM
					(
					SELECT 
						order_date AS create_time,
						product_code,
						account_id						
					FROM jkbd.fact_export_detail_full
					WHERE order_date>=date_sub('${var:yes_date}',${var:days})
						AND EL_RECORD_STATE = 2 
						AND EF_RECORD_STATE = 2 
						AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
						AND order_type not in('门店','批发','批零一体化')
						AND form_rank = 1
						AND export_form_type != '退货单'
						AND product_code is not null					
						AND is_group = 0
						AND is_gift = '非赠品'
						AND product_code not in (857934,857932)
						AND account_id is not null							
					)fe
			join
					(
					select 
						account_id,
						all_frist_order_time
					from
						jkbd.dwd_account_order_time 
					where account_id is not null and 	all_frist_order_time is not null		
					)da
			ON 
				fe.account_id=da.account_id and create_time=to_date(all_frist_order_time)
			group by
				fe.product_code,
				fe.create_time	
			)new_num
	on t1.create_time=new_num.create_time and t1.product_code=new_num.product_code		
	
	left join
	--老用户数
			(SELECT
				fe.product_code,
				fe.create_time,
				count(distinct fe.account_id)	 old_user_num		
			FROM
					(
					SELECT 
						order_date AS create_time,
						product_code,					
						account_id					
					FROM jkbd.fact_export_detail_full
					WHERE order_date>=date_sub('${var:yes_date}',${var:days})
						AND EL_RECORD_STATE = 2 
						AND EF_RECORD_STATE = 2 
						AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
						AND order_type not in('门店','批发','批零一体化')
						AND form_rank = 1
						AND export_form_type != '退货单'
						AND product_code is not null						
						AND is_group = 0
						AND is_gift = '非赠品'
						AND product_code!=857934
						AND product_code!=857932
						AND account_id is not null
						
					)fe
			left join
					(
					select 
						account_id,
						all_frist_order_time
					from
						jkbd.dwd_account_order_time 
					where account_id is not null 
						  and all_frist_order_time is not null		
					)da
			ON 
				fe.account_id=da.account_id and create_time=to_date(all_frist_order_time)
			where 	
				da.account_id is null and da.all_frist_order_time is null
			group by
				fe.product_code,
				fe.create_time				
			)old_num
	on t1.create_time=old_num.create_time and t1.product_code=old_num.product_code	
	
	
	LEFT JOIN
	--订单满足,24、48、72小时即时满足
		(
		select 
			order_date, 
			product_code,
			sum(if(first_syn_status!=1,order_fill_i,0)) order_fill,
			sum(order_fill_24_i) order_fill_24,
			sum(order_fill_48_i) order_fill_48,
			sum(order_fill_72_i) order_fill_72,
			sum(order_fill_i) as order_num_export
		from
			(
			select
				order_date, 
				first_syn_status ,
				product_code,
				orders_code,
				count(first_syn_status) as order_fill_i,				
				sum(if(((unix_timestamp(cast(ed.shipping_time as string),'yyyy-MM-dd HH:mm:SS')-unix_timestamp(cast(ed.order_time as string),'yyyy-MM-dd HH:mm:SS'))/3600<=24 
					and (unix_timestamp(cast(ed.shipping_time as string),'yyyy-MM-dd HH:mm:SS')-unix_timestamp(cast(ed.order_time as string),'yyyy-MM-dd HH:mm:SS'))/3600>=0) or first_syn_status=0, 1, 0)) order_fill_24_i,
				sum(if(((unix_timestamp(cast(ed.shipping_time as string),'yyyy-MM-dd HH:mm:SS')-unix_timestamp(cast(ed.order_time as string),'yyyy-MM-dd HH:mm:SS'))/3600<=48
					and (unix_timestamp(cast(ed.shipping_time as string),'yyyy-MM-dd HH:mm:SS')-unix_timestamp(cast(ed.order_time as string),'yyyy-MM-dd HH:mm:SS'))/3600>=0) or first_syn_status=0,1,0)) order_fill_48_i,
				sum(if(((unix_timestamp(cast(ed.shipping_time as string),'yyyy-MM-dd HH:mm:SS')-unix_timestamp(cast(ed.order_time as string),'yyyy-MM-dd HH:mm:SS'))/3600<=72
					and (unix_timestamp(cast(ed.shipping_time as string),'yyyy-MM-dd HH:mm:SS')-unix_timestamp(cast(ed.order_time as string),'yyyy-MM-dd HH:mm:SS'))/3600>=0) or first_syn_status=0,1,0)) order_fill_72_i
			from 
				(
				select distinct product_code,order_date,orders_code, first_syn_status, shipping_time,order_time from jkbd.fact_export_detail_full ed
				where
				ed.order_date  >= date_sub('${var:yes_date}',${var:days})
				AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
				AND ed.EL_RECORD_STATE = 2 
				AND ed.EF_RECORD_STATE = 2 
				AND ed.order_type not in ('门店', '批零一体化', '批发')
				AND ed.export_form_type in ('出库单', '第三方发货无出库单')
				AND ed.form_rank = 1
				AND ed.is_group = 0
				AND ed.is_gift = '非赠品'
				AND ed.product_code not in (857932, 857934)
				and first_syn_status is not null 
				)ed		
			group by 
				order_date, product_code,first_syn_status,orders_code
		)tbl
		group by order_date,product_code
		)t11
	ON t1.create_time=t11.order_date and t1.product_code=t11.product_code
	where t1.create_time>=date_sub('${var:yes_date}',${var:days})
;