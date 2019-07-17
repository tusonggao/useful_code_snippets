#!/bin/bash
#时间获取
export PYTHON_EGG_CACHE=./myeggs

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

impala-shell -q "
use jkbd;
select 	
	product_small_class,
	product_large_class,
	product_code,
	product_name,
	sale_amount,
(select 
    t007.create_time ,
    t007.platform ,
    t007.sls_origin ,
    t007.product_code ,
    t007.product_name ,
    t007.product_large_class ,
    t007.product_small_class ,
    t007.product_inventory ,
    t007.sale_amount,
    t007.order_num ,
    t007.gross_profit_amount,
    t007.gross_profit_rate ,
    t007.inventory_amount,
    t007.refund_order_num ,
    t007.refund_amount,
    t007.refund_rate ,
    t007.product_cost,
    t007.stockout_num ,
    t007.sale_num ,
    t007.order_num_stockout ,
    t007.order_amount_stockout,
    nvl(cast (t008.sale_amount_new_user as decimal(13,4)),0),
    nvl(cast (t008.gross_profit_amount_new_user as decimal(13,4)),0),
    nvl(t008.order_num_new_user,0),
    nvl(t008.new_user_num,0),
    nvl(cast (t008.sale_amount_old_user as decimal(13,4)),0) ,
    nvl(cast (t008.gross_profit_amount_old_user as decimal(13,4)),0),
    nvl(t008.order_num_old_user,0),
    nvl(t008.old_user_num,0),
    t007.dt
from 
(
    SELECT 
    	cast(t1.create_time as string) as create_time,
    	nvl(t1.sls_platform,'') platform,
    	nvl(t1.sls_origin,'') sls_origin,
    	cast(nvl(t1.product_code,0) as string) product_code,
    	ifnull(product_name,'') product_name,
    	ifnull(product_large_class,'') product_large_class,
    	ifnull(product_small_class,'') product_small_class,
    	cast(nvl(product_inventory,0) as int) as product_inventory,
    	cast(nvl(product_amount,0) as DECIMAL(13,4)) as sale_amount,
    	cast(nvl(order_num,0) as int) order_num,
    	cast(nvl(product_margin_valid,0) as DECIMAL(13,4)) as gross_profit_amount,
    	cast(nvl((case when product_amount=0 then -1 else product_margin_valid/product_amount end),0) as float) as gross_profit_rate,
    	cast(nvl(inventory_amount,0) as DECIMAL(13,4)) as inventory_amount,
    	cast(nvl(refund_order_num,0) as int) as refund_order_num,
    	cast(nvl(product_amount_return,0) as DECIMAL(13,4)) as refund_amount,
    	cast(nvl(refund_order_num/order_num,0) as float) as refund_rate,
    	cast(nvl(product_cost,0) as DECIMAL(13,4)) as product_cost,
    	cast(nvl(stockout_num,0) as int) stockout_num,
    	cast(nvl(sale_num,0) as int) sale_num,
    	cast(order_num_stockout as int ) order_num_stockout,
    	cast(order_amount_stockout as DECIMAL(13,4)) order_amount_stockout,
    	cast(t1.create_time as string) as dt
    FROM
    		(
    		SELECT 
    			'${var:yes_date}' create_time,
    			ifnull(t0.product_code,t12.product_code) product_code,
    			ifnull(t0.sls_platform,t12.sls_platform) sls_platform,
    			ifnull(t0.sls_origin,t12.sls_origin) sls_origin,
    			ifnull(t0.refund_order_num,0) refund_order_num,
    			ifnull(t0.product_amount_return,0) product_amount_return,
    			ifnull(product_amount,0) product_amount,
    			ifnull(product_margin_valid,0) product_margin_valid,
    			ifnull(product_cost,0) product_cost,
    			ifnull(order_num,0) order_num,
    			ifnull(stockout_num,0) stockout_num,
    			ifnull(sale_num,0) sale_num,
    			ifnull(order_num_stockout,0) order_num_stockout,
    			ifnull(order_amount_stockout,0) order_amount_stockout
    			
    		FROM	
    			--产品退货订单 退货金额
    		    (
    			SELECT 
    				product_code,
    				COUNT(DISTINCT orders_code ) as refund_order_num,
    				SUM( product_amount) as product_amount_return
    			FROM jkbd.fact_export_detail_full
    			WHERE   order_date>=date_sub('${var:yes_date}',${var:days})
						and order_date<'${var:yes_date}'
    					AND EL_RECORD_STATE = 2 
    					AND EF_RECORD_STATE = 2 
    					AND order_status in (60, 160, 80, 170, 180)
    					AND order_type not in('门店','批发','批零一体化')
    					AND form_rank = 1
    					AND export_form_type != '退货单'
    					AND product_code is not null
    					AND sls_platform is not null
    					AND sls_origin is not null
    					AND is_group = 0
    					AND is_gift = '非赠品'
    					AND product_code not in (857932,857934)
						AND large_class='肝病用药'
    			GROUP BY 
    			product_code
    			)t0
    		FULL OUTER JOIN 
    			--产品销售额 毛利 成本  订单数量
    			(SELECT
    				product_code,
    				SUM(product_amount) AS product_amount,
    				SUM(product_margin_valid) AS product_margin_valid,
    				SUM(product_cost) AS product_cost,
    				COUNT(DISTINCT orders_code) AS order_num,
    				SUM(ifnull(product_quantity,0)) as sale_num
    			FROM
    				(
    					SELECT 
    						orders_code,
    						product_code,
    						nvl(product_amount,0) product_amount,
    						(nvl(product_amount,0)-nvl(product_cost,0)) as product_margin_valid,
    						nvl(product_cost,0) product_cost,
    						is_short_supply,
    						product_quantity
    					FROM jkbd.fact_export_detail_full
    					WHERE order_date>=date_sub('${var:yes_date}',${var:days})
							and order_date<'${var:yes_date}'
    						AND EL_RECORD_STATE = 2 
    						AND EF_RECORD_STATE = 2 
    						AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
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
    						AND large_class='肝病用药'
    				)t11
    				GROUP BY 
    				product_code			
    			)t12	
    		ON
    		t0.product_code=t12.product_code
    		--缺货金额 缺货数量 缺货订单数 			 
    		LEFT JOIN				 
    	        (SELECT
    				product_code,
    				SUM(product_amount) AS order_amount_stockout,
    				SUM(product_quantity) as stockout_num,
    				COUNT(DISTINCT orders_code) AS  order_num_stockout			
    			FROM
    				(
    					SELECT 
    						order_date AS create_time,
    						orders_code,
    						sls_platform ,
    						product_code,
    						sls_origin,
    						is_short_supply,
    						product_amount,
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
    					    AND sls_platform is not null
    					    AND sls_origin is not null
    						AND is_group = 0
    						AND is_gift = '非赠品'
    						AND product_code not in (857934,857932)	
    						AND is_short_supply=1
							AND large_class='肝病用药'
    				)tmp1
    			GROUP BY 
    				product_code		
    			)st_tb		
    		ON  t12.product_code=st_tb.product_code 								 			 
    		)t1	 
    	LEFT JOIN
    	--产品信息
    		(
    		SELECT
    			product_code,
    			product_name,
    			large_class as product_large_class,
    			small_class as product_small_class
    		FROM
    			jkbd.DIM_PRODUCT
    		)t6
    		ON t1.product_code=t6.product_code
    	LEFT JOIN 
    	--产品库存 库存金额
    		(
    
    		SELECT
    			product_code,
    			create_date,
    			sum(ending_quantity) as product_inventory,
    			sum(ending_amount) as inventory_amount
    		FROM
    			jkbd.dwd_goods_inventory_daily_parquet as dt
    		join
    			jkbd.goods_parquet gs
    		on dt.goods_id=gs.id
    		AND dt.create_date>=date_sub('${var:yes_date}',${var:days})
    		group by product_code,create_date		
    		)t8	
    		ON t1.product_code=t8.product_code
    		AND t1.create_time=t8.create_date  	
) t007
left join 
(
   select 
    ifnull(t3.create_time,t4.create_time) as create_time,
    ifnull(t3.platform,t4.platform) as platform,
    ifnull(t3.sls_origin,t4.sls_origin) as sls_origin,
    cast(ifnull(t3.product_code,t4.product_code) as string) as product_code,
    ifnull(t3.product_name,t4.product_name) as product_name,
    ifnull(t3.product_large_class,t4.product_large_class) as product_large_class,
    ifnull(t3.product_small_class,t4.product_small_class) as product_small_class,
    nvl(t3.sale_amount_new_user,0) as sale_amount_new_user,
    nvl(t3.gross_profit_amount_new_user,0) as gross_profit_amount_new_user,
    nvl(t3.order_num_new_user,0) as order_num_new_user,
    nvl(t3.new_user_num,0) as new_user_num,
    nvl(t4.sale_amount_old_user,0) as sale_amount_old_user,
    nvl(t4.gross_profit_amount_old_user,0) as gross_profit_amount_old_user,
    nvl(t4.order_num_old_user,0) as order_num_old_user,
    nvl(t4.old_user_num,0) as old_user_num
from
(
	--商品小类新客销售额，毛利额，用户数，订单数
    select 
        t1.*,
        t2.new_user_num
    from 
    (
    select 
        cast(order_date as string) as create_time,
        platform,
        sls_origin,
        t1.product_code,
        dp.product_name,
        t1.large_class as product_large_class,
        t1.small_class as product_small_class,
        sum(product_amount ) as sale_amount_new_user,
        sum(product_amount-product_cost) as gross_profit_amount_new_user,
        count(distinct t1.orders_code) as order_num_new_user
    from 
    dwd_product_class_sale t1
    left join 
    dim_product dp on t1.product_code=dp.product_code
    where rn=1
	and  order_date>=date_sub('${var:yes_date}',${var:days})
    group by 
        platform,
        sls_origin,
        t1.product_code,
        dp.product_name,
        t1.large_class,
        t1.small_class,
        order_date
    ) t1
    left join 
    (
	--一条语句不能多条count(distinct) 这里分开计算再join
    select 
        cast(order_date as string) as create_time,
        platform,
        sls_origin,
        t1.product_code,
        dp.product_name,
        t1.large_class as product_large_class,
        t1.small_class as product_small_class,
        count(distinct t1.account_id) as new_user_num
    from 
    dwd_product_class_sale t1
    left join 
    dim_product dp on t1.product_code=dp.product_code
    where rn=1
    and  order_date>=date_sub('${var:yes_date}',${var:days}) and order_date<='${var:yes_date}'
    group by 
        platform,
        sls_origin,
        t1.product_code,
        dp.product_name,
        t1.large_class,
        t1.small_class,
        order_date
    ) t2 on t1.platform =t2.platform
    and t1.sls_origin=t2.sls_origin
    and t1.product_code=t2.product_code
    and t1.product_name=t2.product_name
    and t1.product_large_class=t2.product_large_class
    and t1.product_small_class=t2.product_small_class
    and t1.create_time = t2.create_time
) t3
full join 
(	
--商品小类老客销售额，毛利额，用户数，订单数
    select 
        t1.*,
        t2.old_user_num
    from 
    (
    select 
        t1.product_code,
        dp.product_name,
        t1.large_class as product_large_class,
        t1.small_class as product_small_class,
        sum(product_amount ) as sale_amount_old_user,
        sum(product_amount-product_cost) as gross_profit_amount_old_user,
        count(distinct t1.orders_code) as order_num_old_user
    from 
    dwd_product_class_sale t1
    left join 
    dim_product dp on t1.product_code=dp.product_code
    where rn <> 1
    and  order_date>=date_sub('${var:yes_date}',${var:days}) and order_date<='${var:yes_date}'
    group by 
        t1.product_code,
        dp.product_name,
        t1.large_class,
        t1.small_class
    ) t1
    left join 
    (
	--一条语句不能多条count(distinct) 这里分开计算再join
    select 
        t1.product_code,
        dp.product_name,
        t1.large_class as product_large_class,
        t1.small_class as product_small_class,
        count(distinct t1.account_id) as old_user_num
    from 
    dwd_product_class_sale t1
    left join 
    dim_product dp on t1.product_code=dp.product_code
    where rn <> 1
	and  order_date>=date_sub('${var:yes_date}',${var:days}) and order_date<='${var:yes_date}'
    group by 
        t1.product_code,
        dp.product_name,
        t1.large_class,
        t1.small_class
    ) t2 on 
     t1.product_code=t2.product_code
    and t1.product_name=t2.product_name
    and t1.product_large_class=t2.product_large_class
    and t1.product_small_class=t2.product_small_class
)t4
    on  
     t3.product_code=t4.product_code
    and t3.product_name=t4.product_name
    and t3.product_large_class=t4.product_large_class
    and t3.product_small_class=t4.product_small_class

) t008 on 
and t007.product_code = t008.product_code
and t007.product_name = t008.product_name
and t007.product_large_class = t008.product_large_class
and t007.product_small_class = t008.product_small_class

"
-B --output_delimiter="," --print_header -o hepatopathy.csv