CREATE TABLE IF NOT EXISTS sjcm.dwd_order_boss_sale_daily_summary(
	create_time timestamp comment '统计-订单，时间',
	sale_amount decimal(13,4)  comment '销售额',	
	gross_profit_amount decimal(13,4) comment '毛利额',
	gross_profit_rate float comment '毛利率',
	inventory_amount decimal(13,4) comment '库存金额',
	order_num int comment '订单数量',
	order_average_amount decimal(13,4) comment '单均',
	order_fill_rate float comment '订单满足率',
	inventory_day int comment '库存周转天数',
	order_fill int comment '订单满足数',
	order_num_export int comment '出库订单数量',
	stock_in_amount decimal(13,4) comment '入库金额',
	sale_amount_send  decimal(13,4) comment '发货销售金额',
	gross_profit_amount_send  decimal(13,4) comment '发货毛利额',
	order_num_send BIGINT comment '发货订单数',
	sale_amount_sign  decimal(13,4) comment '签收销售金额',
	gross_profit_amount_sign  decimal(13,4) comment '签收毛利额',
	order_num_sign BIGINT comment '签收订单数',
	product_piece_num BIGINT comment '商品数量'
)partitioned by(dt string)
row format delimited fields terminated by '\u0001';

INSERT OVERWRITE TABLE sjcm.dwd_order_boss_sale_daily_summary partition(dt)
SELECT 
	t1.create_time,
	cast(nvl(sale_amount,0) as DECIMAL(13,4)) sale_amount,
	cast(gross_profit_amount as DECIMAL(13,4)) gross_profit_amount,
	cast(gross_profit_rate as float) gross_profit_rate ,
	cast(nvl(inventory_amount,0) as DECIMAL(13,4)) inventory_amount ,
	cast(order_num as int) order_num ,
	cast(order_average_amount as DECIMAL(13,4)) order_average_amount ,
	cast(order_fill/order_num_export as float) order_fill_rate,
	cast(round(t10.inventory_day) as int) inventory_day,
	nvl(cast(order_fill as int),0) order_fill,
	nvl(cast(order_num_export as int),0) order_num_export,
	cast(nvl(stock_in_amount,0) as DECIMAL(13,4)) stock_in_amount,
	nvl(t12.sale_amount_send ,0) as sale_amount_send,
	nvl(t12.gross_profit_amount_send,0) as gross_profit_amount_send,
	nvl(t12.order_num_send,0) as order_num_send,
	nvl(t13.sale_amount_sign,0) as sale_amount_sign,
	nvl(t13.gross_profit_amount_sign,0) as gross_profit_amount_sign,
	nvl(t13.order_num_sign,0) as order_num_sign,
	nvl(t1.product_quantity,0) as product_piece_num,
	t1.create_time as dt
FROM
		--每日销售金额 毛利额 毛利率 订单数 单均
		(
			SELECT 
				order_date AS create_time,
				sum(nvl(product_amount,0)) as sale_amount,
				sum(nvl(product_amount,0)-nvl(product_cost,0)) as gross_profit_amount,
				case when sum(nvl(product_amount,0))=0 then -1 else sum(nvl(product_amount,0)-nvl(product_cost,0))/sum(product_amount)  end as gross_profit_rate,
				count(distinct orders_code) as order_num,
				sum(nvl(product_amount,0))/count(distinct orders_code) as order_average_amount,
				sum(product_quantity) product_quantity
			FROM jkbd.fact_export_detail_full
			WHERE order_date>=date_sub('${var:yes_date}',${var:days})
				AND EL_RECORD_STATE = 2 
				AND EF_RECORD_STATE = 2 
				AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
				AND order_type not in('门店','批发','批零一体化')
				AND form_rank = 1
                AND export_form_type != '退货单'
				AND is_group = 0
				AND is_gift = '非赠品'
				AND product_code!=857934
				AND product_code!=857932
				GROUP BY order_date
		)t1
	JOIN 
	--库存周转天数
        ( 	   
		select 
			sum(inventory_amount) / sum(sls_amount)  inventory_day,
			sum(inventory_amount) as inventory_amount,
			order_date
				from (
					select 
						coalesce(create_date,order_date) order_date,
						sls_quantity, 
						sls_quantity * inventory_price as sls_amount,
						inv.inventory_quantity,
						inventory_price, 
						inventory_amount
					from 
					(											
						select 
							order_date,
							product_code,
							(sum(sale_num) over(partition by product_code order by order_date asc rows between 29 preceding and current row))/30 sls_quantity
						from	
							(select	
								order_date,
								pro_dt.product_code,
								nvl(sale_num,0) as sale_num	
							from	
								(select 
										order_date,
										product_code
									from 
										(select distinct order_date order_date from jkbd.fact_export_detail_full where order_date>=to_date(date_sub('${var:yes_date}',${var:days}+60)))dat
									join
										(select product_code from jkbd.fact_export_detail_full 
										where	
										 product_code is not null  
										group by product_code)proco
							  
								)pro_dt
							left join
								(SELECT 
									order_date create_time,
									product_code,
									SUM(ifnull(product_quantity,0)) as sale_num				
								FROM jkbd.fact_export_detail_full
								WHERE   
										order_date>=to_date(date_sub('${var:yes_date}',${var:days}+60))
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
					) sls 
					full outer join 
					(
						select create_date,g.product_code, sum(ending_quantity) as inventory_quantity, sum(ending_amount) inventory_amount, 
							sum(ending_amount) / sum(ending_quantity) as inventory_price
						from 
						 jkbd.dwd_goods_inventory_daily_parquet i
						join 
						jkbd.goods_parquet g on i.goods_id = g.id
						where create_date >= date_sub('${var:yes_date}',${var:days})
						group by g.product_code,create_date
					) inv
					on sls.product_code = inv.product_code and sls.order_date=inv.create_date
					where inventory_quantity != 0
			)tmp
			group by order_date	
        )t10
	ON t1.create_time=t10.order_date	
	LEFT JOIN
	--订单满足
		(
		select 
			order_date, 
			sum(if(first_syn_status is not null, order_cnt, 0))-sum(if(first_syn_status = 1, order_cnt, 0)) as order_fill,
			sum(if(first_syn_status is not null, order_cnt, 0)) as order_num_export
		from
			(
			select
				order_date, 
				first_syn_status ,
				count(distinct orders_code) order_cnt
			from jkbd.fact_export_detail_full ed
			where 
				ed.order_date >= date_sub('${var:yes_date}',${var:days})
				AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
				AND ed.EL_RECORD_STATE = 2 
				AND ed.EF_RECORD_STATE = 2 
				AND ed.order_type not in ('门店', '批零一体化', '批发')
				AND ed.export_form_type in ('出库单', '第三方发货无出库单')
				AND ed.form_rank = 1
				AND ed.is_group = 0
				AND ed.is_gift = '非赠品'
				AND ed.product_code not in (857932, 857934)
			group by 
				order_date, first_syn_status
		) tbl
		group by order_date
		)t11
	ON t1.create_time=t11.order_date
	LEFT JOIN
	--入库金额
		(
			select 
				storage_date, 
				sum(storage_actual_amount) stock_in_amount
			from 
				jkbd.fact_storage_detail
			where 
				storage_date >= date_sub('${var:yes_date}',${var:days})			 
				and sf_record_state = 2
				and sl_record_state = 2
				and storage_form_status in (6)
				and storage_form_type in (2)
			group by 
				storage_date
		)stg
	ON
		t1.create_time=stg.storage_date
	and 	t1.create_time>=date_sub('${var:yes_date}',${var:days}) 
	
	left join 
	(
	--发货销售金额，发货毛利额，发货订单数
		SELECT 
		to_date(shipped_time) AS create_time,
		cast(sum(nvl(product_amount,0)) as DECIMAL(13,4))  as sale_amount_send,
		cast(sum(nvl(product_amount,0)-nvl(product_cost,0)) as decimal(13,4)) as gross_profit_amount_send,
		count(distinct orders_code)  as order_num_send
		FROM jkbd.fact_export_detail_full
		WHERE order_date>=date_sub('${var:yes_date}',${var:days})
			AND EL_RECORD_STATE = 2 
			AND EF_RECORD_STATE = 2 
			AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
			AND order_type not in('门店','批发','批零一体化')
			AND form_rank = 1
			AND export_form_type != '退货单'
			AND is_group = 0
			AND is_gift = '非赠品'
			AND product_code!=857934
			AND product_code!=857932
			and shipped_time > '1900-01-01 00:00:00'
			GROUP BY to_date(shipped_time)
	) t12 on t1.create_time = t12.create_time
	left join 
	(
	   --签收销售金额，签收订单数，签收毛利额
		SELECT 
		to_date(signed_time) AS create_time,
		cast(sum(nvl(product_amount,0)) as DECIMAL(13,4))  as sale_amount_sign,
		cast(sum(nvl(product_amount,0)-nvl(product_cost,0)) as decimal(13,4)) as gross_profit_amount_sign,
		count(distinct orders_code)  as order_num_sign
		FROM jkbd.fact_export_detail_full
		WHERE order_date>=date_sub('${var:yes_date}',${var:days})
			AND EL_RECORD_STATE = 2 
			AND EF_RECORD_STATE = 2 
			AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
			AND order_type not in('门店','批发','批零一体化')
			AND form_rank = 1
			AND export_form_type != '退货单'
			AND is_group = 0
			AND is_gift = '非赠品'
			AND product_code!=857934
			AND product_code!=857932
			and signed_time > '1900-01-01 00:00:00'
			GROUP BY to_date(signed_time)	
	) t13 on t1.create_time = t13.create_time;