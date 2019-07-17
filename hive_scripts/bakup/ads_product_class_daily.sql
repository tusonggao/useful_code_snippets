use jkbd;
create table if not exists sjcm.ads_product_class_daily (
    create_time timestamp comment '平台',
    platform string comment '平台',
    sls_origin string comment '来源',
    product_class_name string comment '类目名称',
    sale_amount decimal(13,4) comment '销售金额',
    gross_profit_amount decimal(13,4) comment '毛利额',
    sale_amount_new_user decimal(13,4) comment '新用户销售额',
    gross_profit_amount_new_user  decimal(13,4) comment '新用户毛利额',
    order_num_new_user bigint comment '新用户订单数',
    new_user_num bigint comment '新用户数',
    sale_amount_old_user decimal(13,4) comment '老用户销售额',
    gross_profit_amount_old_user  decimal(13,4)  comment '老用户毛利额',
    order_num_old_user bigint comment '老用户订单数',
    old_user_num bigint comment '老用户数',
    order_num bigint comment '订单数',
    refund_order_num bigint comment '退单订单数',
    refund_amount decimal(13,4) comment '退款金额'
)
partitioned by (dt string)
;


insert overwrite table sjcm.ads_product_class_daily partition(dt)
select 
    t10.order_date as create_time,
    nvl(t10.sls_platform ,'') as platform,
    nvl(t10.sls_origin,'') as sls_origin,
    nvl(t10.product_class_name,'') product_class_name,
    cast(nvl(t10.sale_amount,0) as decimal(13,4)),
     cast(nvl(t10.gross_profit_amount,0) as decimal(13,4)),
     nvl(t11.sale_amount_new_user,0),
     nvl(t11.gross_profit_amount_new_user,0),
     nvl(t11.order_num_new_user,0),
    nvl( t11.new_user_num,0),
     nvl(t11.sale_amount_old_user,0),
     nvl(t11.gross_profit_amount_old_user,0),
     nvl(t11.order_num_old_user,0),
     nvl(t11.old_user_num,0),
     nvl(t10.order_num,0),
     nvl(t11.refund_order_num,0),
     nvl(t11.refund_amount,0),
    t10.order_date
from 

(
select 
    sls_platform as sls_platform,
    sls_origin as sls_origin,
    small_class as product_class_name,
    order_date,
    order_num,
	sale_amount,
	gross_profit_amount
    from
    (select 
         sls_platform,
        sls_origin,
        large_class,
        small_class,
        order_date,
        count(distinct orders_code) as order_num,
		   sum(product_amount ) as sale_amount,
        sum(product_amount-product_cost) as gross_profit_amount
    from jkbd.fact_export_detail_full
    WHERE   
        order_date >= date_sub( '${var:yes_date}',360)
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
		and small_class not in ('中药饮片','医疗器械','其他')
       	group by 
       	sls_platform,
        sls_origin,
        large_class,
        small_class,
        order_date
        ) t8
    
union all 

select 
     sls_platform,
    sls_origin,
    large_class as product_class_name,
     order_date,
    count(distinct orders_code) order_num,
	        sum(product_amount ) as sale_amount,
        sum(product_amount-product_cost) as gross_profit_amount
from jkbd.fact_export_detail_full
WHERE   
    order_date >= date_sub( '${var:yes_date}',360)
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
   	group by 
   	sls_platform,
    sls_origin,
    large_class,
    order_date
) t10

left join 

(
select 
    ifnull(t3.create_time,t4.create_time) as create_time,
    ifnull(t3.platform,t4.platform) as platform,
    ifnull(t3.sls_origin,t4.sls_origin) as sls_origin,
    ifnull(t3.product_small_class,t4.product_small_class) as product_class_name,

    cast(nvl(t3.sale_amount_new_user,0) as decimal(13,4)) as sale_amount_new_user,
    cast(nvl(t3.gross_profit_amount_new_user,0) as decimal(13,4)) as gross_profit_amount_new_user,
    nvl(t3.order_num_new_user,0) as order_num_new_user,
    nvl(t3.new_user_num,0) as new_user_num,
    cast(nvl(t4.sale_amount_old_user,0) as decimal(13,4)) as sale_amount_old_user,
    cast(nvl(t4.gross_profit_amount_old_user,0) as decimal(13,4)) as gross_profit_amount_old_user,
    nvl(t4.order_num_old_user,0) as order_num_old_user,
    nvl(t4.old_user_num,0) as old_user_num,
    nvl(t5.refund_order_num,0) as refund_order_num,
    nvl(cast(t5.product_amount_return as decimal(13,4)),0) as refund_amount,
    ifnull(t3.create_time,t4.create_time)
from 
(
--商品类别新客销售额，毛利额，用户数，订单数
    select 
        t1.create_time,
        t1.platform,
        t1.sls_origin,
        t1.product_large_class,
        t1.product_small_class,
        sale_amount_new_user,
        gross_profit_amount_new_user,
        order_num_new_user,
        t2.new_user_num
    from 
    (
    select 
        cast(t1.order_date as string) as create_time,
        sls_platform as platform,
        sls_origin,
        t1.large_class as product_large_class,
        t1.small_class as product_small_class,
        sum(product_amount ) as sale_amount_new_user,
        sum(product_amount-product_cost) as gross_profit_amount_new_user,
        count(distinct t1.orders_code) as order_num_new_user
    from 
    (
            select 
                product_code,
                order_date,
                sls_origin,
                sls_platform,
                account_id,
                product_amount,
                product_cost,
                orders_code,
				 large_class,
                small_class 
                
            from 
            jkbd.fact_export_detail_full
            where  order_date >= date_sub( '${var:yes_date}',360)
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
				and small_class not in ('中药饮片','医疗器械','其他')
        ) t1

    left join dwd_product_class2_new_account t2  
    on t1.order_date = t2.order_date
    and t1.large_class = t2.large_class
    and t1.small_class = t2.small_class
    and t1.account_id = t2.account_id
    where t2.account_id is not null

	and  t1.order_date >= date_sub( '${var:yes_date}',360)
    group by 
        platform,
        sls_origin,
        t1.large_class,
        t1.small_class,
        t1.order_date
    ) t1
    left join 
    (
	--一条语句不能多条count(distinct) 这里分开计算再join
    select 
        cast(t1.order_date as string) as create_time,
        sls_platform as platform,
        sls_origin,
        t1.large_class as product_large_class,
        t1.small_class as product_small_class,
        count(distinct t1.account_id) as new_user_num
    from 
    (
            select 
                product_code,
                order_date,
                sls_origin,
                sls_platform,
                account_id,
                product_amount,
                product_cost,
                orders_code,
				 large_class,
                small_class 
                
            from 
            jkbd.fact_export_detail_full
            where  order_date >= date_sub( '${var:yes_date}',360)
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
				and small_class not in ('中药饮片','医疗器械','其他')
        ) t1

    left join dwd_product_class2_new_account  t2
    on t1.order_date = t2.order_date
    and t1.large_class = t2.large_class
    and t1.small_class = t2.small_class
    and t1.account_id = t2.account_id
     where t2.account_id is  null

    and  t1.order_date >= date_sub( '${var:yes_date}',360)
    group by 
        platform,
        sls_origin,
        t1.large_class,
        t1.small_class,
        t1.order_date
    ) t2 on t1.platform =t2.platform
    and t1.sls_origin=t2.sls_origin
    and t1.product_large_class=t2.product_large_class
    and t1.product_small_class=t2.product_small_class
    and t1.create_time = t2.create_time
) t3
full outer join
(
--商品小类老客销售额，毛利额，用户数，订单数
    select 
        t1.create_time,
        t1.platform,
        t1.sls_origin,
        t1.product_large_class,
        t1.product_small_class,
        sale_amount_old_user,
        gross_profit_amount_old_user,
        order_num_old_user,
        t2.old_user_num
    from 
    (
    select 
        cast(t1.order_date as string) as create_time,
         sls_platform as platform,
        sls_origin,
          t1.large_class as product_large_class,
        t1.small_class as product_small_class,
        sum(product_amount ) as sale_amount_old_user,
        sum(product_amount-product_cost) as gross_profit_amount_old_user,
        count(distinct t1.orders_code) as order_num_old_user
    from 
   (
            select 
                product_code,
                order_date,
                sls_origin,
                sls_platform,
                account_id,
                product_amount,
                product_cost,
                orders_code,
				 large_class,
                small_class 
            from 
            jkbd.fact_export_detail_full
            where  order_date >= date_sub( '${var:yes_date}',360)
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
				and small_class not in ('中药饮片','医疗器械','其他')
        ) t1

    left join dwd_product_class2_new_account t2  
    on t1.order_date = t2.order_date
    and t1.large_class = t2.large_class
    and t1.small_class = t2.small_class
    and t1.account_id = t2.account_id
    where t2.account_id is  null
	and  t1.order_date >= date_sub( '${var:yes_date}',360)
    group by 
        platform,
        sls_origin,
        t1.large_class ,
        t1.small_class ,
        t1.order_date
    ) t1
    left join 
    (
	--一条语句不能多条count(distinct) 这里分开计算再join
    select 
        cast(t1.order_date as string) as create_time,
         sls_platform as platform,
        sls_origin,
        t1.large_class as product_large_class,
        t1.small_class as product_small_class,
        count(distinct t1.account_id) as old_user_num
    from 
   (
            select 
                product_code,
                order_date,
                sls_origin,
                sls_platform,
                account_id,
                product_amount,
                product_cost,
                orders_code,
				 large_class,
                small_class 
                
            from 
            jkbd.fact_export_detail_full
            where  order_date >= date_sub( '${var:yes_date}',360)
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
				and small_class not in ('中药饮片','医疗器械','其他')
        ) t1

    left join dwd_product_class2_new_account t2  
    on t1.order_date = t2.order_date
    and t1.large_class = t2.large_class
    and t1.small_class = t2.small_class
    and t1.account_id = t2.account_id
    where t2.account_id is  null
	and  t1.order_date >= date_sub( '${var:yes_date}',360)
    group by 
        platform,
        sls_origin,
        t1.large_class ,
        t1.small_class ,
        t1.order_date
    ) t2 on t1.platform =t2.platform
    and t1.sls_origin=t2.sls_origin
    and t1.product_large_class=t2.product_large_class
    and t1.product_small_class=t2.product_small_class
    and t1.create_time = t2.create_time
) t4
 on  t3.platform =t4.platform
    and t3.sls_origin=t4.sls_origin
    and t3.product_large_class=t4.product_large_class
    and t3.product_small_class=t4.product_small_class
    and t3.create_time = t4.create_time
left join 
(
 SELECT 
    				
    				sls_platform as platform,
    				sls_origin ,
					order_date as create_time,
					large_class as product_large_class,
					small_class as product_small_class,
    				COUNT(DISTINCT orders_code ) as refund_order_num,
    				SUM(product_amount) as product_amount_return
    			FROM jkbd.fact_export_detail_full
    			WHERE   
				order_date >= date_sub( '${var:yes_date}',360)
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
						and small_class not in ('中药饮片','医疗器械','其他')
    			GROUP BY 
    			order_date,
    			sls_platform,
    			sls_origin,
				large_class,
				small_class
)t5
  on  ifnull(t3.platform,t4.platform) =t5.platform
    and ifnull(t3.sls_origin,t4.sls_origin)=t5.sls_origin
    and ifnull(t3.product_large_class,t4.product_large_class)=t5.product_large_class
	and ifnull(t3.product_small_class,t4.product_small_class)=t5.product_small_class
    and ifnull(t3.create_time,t4.create_time) = t5.create_time

	
union all 

select 
    ifnull(t3.create_time,t4.create_time) as create_time,
    ifnull(t3.platform,t4.platform) as platform,
    ifnull(t3.sls_origin,t4.sls_origin) as sls_origin,
    ifnull(t3.product_large_class,t4.product_large_class) as product_class_name,

    cast(nvl(t3.sale_amount_new_user,0) as decimal(13,4)) as sale_amount_new_user,
    cast(nvl(t3.gross_profit_amount_new_user,0) as decimal(13,4)) as gross_profit_amount_new_user,
    nvl(t3.order_num_new_user,0) as order_num_new_user,
    nvl(t3.new_user_num,0) as new_user_num,
    cast(nvl(t4.sale_amount_old_user,0) as decimal(13,4)) as sale_amount_old_user,
    cast(nvl(t4.gross_profit_amount_old_user,0) as decimal(13,4)) as gross_profit_amount_old_user,
    nvl(t4.order_num_old_user,0) as order_num_old_user,
    nvl(t4.old_user_num,0) as old_user_num,
    nvl(t5.refund_order_num,0) as refund_order_num,
    nvl(cast(t5.product_amount_return as decimal(13,4)),0) as refund_amount,
    ifnull(t3.create_time,t4.create_time)
from 
(
--商品类别新客销售额，毛利额，用户数，订单数
    select 
        t1.create_time,
        t1.platform,
        t1.sls_origin,
        t1.product_large_class,
        sale_amount_new_user,
        gross_profit_amount_new_user,
        order_num_new_user,
        t2.new_user_num
    from 
    (
    select 
        cast(t1.order_date as string) as create_time,
        sls_platform as platform,
        sls_origin,
        t1.large_class as product_large_class,
        sum(product_amount ) as sale_amount_new_user,
        sum(product_amount-product_cost) as gross_profit_amount_new_user,
        count(distinct t1.orders_code) as order_num_new_user
    from 
    (
            select 
                product_code,
                order_date,
                sls_origin as sls_origin,
                sls_platform as sls_platform,
                account_id,
                product_amount,
                product_cost,
                orders_code,
				 large_class,
                small_class 
                
            from 
            jkbd.fact_export_detail_full
            where  order_date >= date_sub( '${var:yes_date}',360)
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
        ) t1

    left join dwd_product_class1_new_account t2  
    on t1.order_date = t2.order_date
    and t1.large_class = t2.large_class
    and t1.account_id = t2.account_id
    where t2.account_id is not null
	and  t1.order_date >= date_sub( '${var:yes_date}',360)
    group by 
        platform,
        sls_origin,
        t1.large_class,
        t1.order_date
    ) t1
    left join 
    (
	--一条语句不能多条count(distinct) 这里分开计算再join
    select 
        cast(t1.order_date as string) as create_time,
        sls_platform as platform,
        sls_origin,
        t1.large_class as product_large_class,
        count(distinct t1.account_id) as new_user_num
    from 
    (
            select 
                product_code,
                order_date,
                sls_origin as sls_origin,
                sls_platform as sls_platform,
                account_id,
                product_amount,
                product_cost,
                orders_code,
				 large_class,
                small_class 
                
            from 
            jkbd.fact_export_detail_full
            where  order_date >= date_sub( '${var:yes_date}',360)
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
        ) t1

    left join dwd_product_class1_new_account  t2
    on t1.order_date = t2.order_date
    and t1.large_class = t2.large_class
    and t1.account_id = t2.account_id
     where t2.account_id is  null
    and  t1.order_date >= date_sub( '${var:yes_date}',360)
    group by 
        platform,
        sls_origin,
        t1.large_class,
        t1.order_date
    ) t2 on t1.platform =t2.platform
    and t1.sls_origin=t2.sls_origin
    and t1.product_large_class=t2.product_large_class
    and t1.create_time = t2.create_time
) t3
full outer join
(
--商品小类老客销售额，毛利额，用户数，订单数
    select 
        t1.create_time,
        t1.platform,
        t1.sls_origin,
        t1.product_large_class,
        sale_amount_old_user,
        gross_profit_amount_old_user,
        order_num_old_user,
        t2.old_user_num
    from 
    (
    select 
        cast(t1.order_date as string) as create_time,
         sls_platform as platform,
        sls_origin,
        t1.large_class as product_large_class,
        sum(product_amount ) as sale_amount_old_user,
        sum(product_amount-product_cost) as gross_profit_amount_old_user,
        count(distinct t1.orders_code) as order_num_old_user
    from 
   (
            select 
                product_code,
                order_date,
                 sls_origin as sls_origin,
                sls_platform as sls_platform,
                account_id,
                product_amount,
                product_cost,
                orders_code,
				 large_class,
                small_class 
                
            from 
            jkbd.fact_export_detail_full
            where  order_date >= date_sub( '${var:yes_date}',360)
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
        ) t1

    left join dwd_product_class1_new_account t2  
    on t1.order_date = t2.order_date
    and t1.large_class = t2.large_class
    and t1.account_id = t2.account_id
    where t2.account_id is  null
	and  t1.order_date >= date_sub( '${var:yes_date}',360)
    group by 
        platform,
        sls_origin,
        t1.large_class ,
        t1.order_date
    ) t1
    left join 
    (
	--一条语句不能多条count(distinct) 这里分开计算再join
    select 
        cast(t1.order_date as string) as create_time,
         sls_platform as platform,
        sls_origin,
        t1.large_class as product_large_class,
        count(distinct t1.account_id) as old_user_num
    from 
        (
            select 
                product_code,
                order_date,
                 sls_origin as sls_origin,
                sls_platform as sls_platform,
                account_id,
                product_amount,
                product_cost,
                orders_code,
				 large_class,
                small_class 
                
            from 
            jkbd.fact_export_detail_full
            where  order_date >= date_sub( '${var:yes_date}',360)
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
        )t1

    left join dwd_product_class1_new_account t2  
    on t1.order_date = t2.order_date
    and t1.large_class = t2.large_class
    and t1.account_id = t2.account_id
    where t2.account_id is  null

    group by 
        platform,
        sls_origin,
        t1.large_class ,
        t1.order_date
    ) t2 on t1.platform =t2.platform
    and t1.sls_origin=t2.sls_origin
    and t1.product_large_class=t2.product_large_class
    and t1.create_time = t2.create_time
) t4
 on  t3.platform =t4.platform
    and t3.sls_origin=t4.sls_origin
    and t3.product_large_class=t4.product_large_class
    and t3.create_time = t4.create_time 
left join 
(
 SELECT 
    			sls_origin as sls_origin,
                sls_platform as platform,
					order_date as create_time,
					large_class as product_large_class,
    				COUNT(DISTINCT orders_code ) as refund_order_num,
    				SUM( product_amount) as product_amount_return
    			FROM jkbd.fact_export_detail_full
    			WHERE   
				order_date >= date_sub( '${var:yes_date}',360)
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
    			GROUP BY 
    			order_date,
    			sls_platform,
    			sls_origin,
				large_class	
)t5
  on  ifnull(t3.platform,t4.platform) =t5.platform
    and ifnull(t3.sls_origin,t4.sls_origin)=t5.sls_origin
    and ifnull(t3.product_large_class,t4.product_large_class)=t5.product_large_class
    and ifnull(t3.create_time,t4.create_time) = t5.create_time


) t11
on t10.sls_platform =t11.platform
    and t10.sls_origin=t11.sls_origin
    and t10.product_class_name=t11.product_class_name
    and t10.order_date = t11.create_time;