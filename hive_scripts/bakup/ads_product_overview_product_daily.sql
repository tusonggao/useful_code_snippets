create table if not exists sjcm.ads_product_overview_product_daily(
create_time timestamp comment '时间',
product_code int comment '商品编号',
product_name string comment '商品名称',
product_img string comment '商品图片',
product_large_class string comment '商品大类',
product_small_class string comment '商品小类',
product_pv bigint comment '商品pv',
product_uv bigint comment '商品uv',
detail_page_access_times bigint comment '详情页访问数',
detail_page_stay_interval bigint comment '详情页停留总时间，单位秒',
detail_page_exit_app_times bigint comment '详情也离开app数',
cart_add_user_num bigint comment '添加到购物车人数',
cart_add_product_num bigint comment '添加到购物车次数',
favorite_add_user_num bigint comment '添加到收藏夹人数',
search_guide_times bigint comment '搜索引导次数',
order_user_num bigint comment '下单客户数',
order_num bigint comment '订单数',
total_product_order_num bigint comment '所有商品订单数',
sale_amount decimal(13,4) comment '订单金额',
product_num bigint comment '商品数',
cost_amount decimal(13,4) comment '成本金额',
gross_profit decimal(13,4) comment '毛利金额'
)partitioned by (dt string);
use jkbd;
INSERT OVERWRITE TABLE sjcm.ads_product_overview_product_daily PARTITION (dt)
select 
ifnull(t1.event_date,t7.order_date) as create_time,
ifnull(t1.product_code,t7.product_code) as product_code,
ifnull(p.product_name,t7.product_name) as product_name,
concat('http://image.jianke.com',ifnull(pi.image_url,t7.image_url)) as product_img,
ifnull(ifnull(dpc.large_class,t7.large_class),'') as product_large_class,
ifnull(ifnull(dpc.small_class,t7.small_class),'') as product_small_class,
nvl(t1.product_pv,0) as product_pv,
nvl(t2.product_uv,0) as product_uv,
nvl(t1.product_pv,0) as detail_page_access_times,
nvl(t3.detail_page_stay_interval,0) as detail_page_stay_interval,
nvl(t4.detail_page_exit_app_times,0) as detail_page_exit_app_times,
nvl(t10.cart_add_user_num,0) as cart_add_user_num,
nvl(t1.cart_add_product_num,0) as cart_add_product_num,
nvl(t11.favorite_add_user_num,0) as favorite_add_user_num,
nvl(t1.search_guide_times,0) as search_guide_times,
nvl(t7.order_user_num,0) as order_user_num,
nvl(t7.order_num,0) as order_num,
nvl(t6.total_product_order_num,0) as total_product_order_num,
cast(nvl(t7.sale_amount,0) as decimal(13,4)) as sale_amount,
nvl(t7.product_num,0) as product_num,
cast(nvl(t7.cost_amount,0) as decimal(13,4)) as cost_amount,
cast(nvl(t7.gross_profit,0) as decimal(13,4)) as gross_profit,
ifnull(t1.event_date,t7.order_date) as create_time
from 
(
    select 
        product_code,
        event_date,
        sum(case when event_cn ='浏览商品' then 1 else 0 end) as product_pv,
        sum(case when event_cn ='加入购物车' then 1 else 0 end) as cart_add_product_num,
        sum(case when event_cn ='搜索' then 1 else 0 end) as search_guide_times
        --sum(case when event_cn ='收藏' then 1 else 0 end) as favorite_add_user_num
    from jkbd.fact_user_event_detail
    where event_date >='${var:yes_date}'
    and product_code is not null
    group by product_code,
    event_date
    
) t1 
left join 
(
    select 
        product_code,
        event_date,
        count(distinct event_user_id) as product_uv
    from jkbd.fact_user_event_detail
    WHERE event_cn='浏览商品'
	    and  event_date >='${var:yes_date}'
    group by product_code,
    
    event_date
) t2 on t1.product_code=t2.product_code and t1.event_date=t2.event_date

left join 
(
    select 
    product_code,
    dt,
    sum(unix_timestamp(cast(time as string),'yyyy-MM-dd HH:mm:ss')-unix_timestamp(cast(next_time as string),'yyyy-MM-dd HH:mm:ss')) as detail_page_stay_interval
    from 
    (
        select 
             case
                -- App
                when event = 'viewProductDetail'
                    and platformtype in ('商城APP_IOS', '商城APP_安卓')
                    and productid is not null
                    then cast(productid as int)
                -- Web
                when event = '$pageview'
                    and parse_url(_url, "PATH") rlike '/product/[0-9]+.html' 
                    and regexp_extract(_url, '/product/([0-9]+).html', 1) is not null
                    then cast(regexp_extract(_url, '/product/([0-9]+).html', 1) as int) 
                -- 小程序
                when event = '$MPViewScreen'
                    and _url_path = 'pages/productdetail/jkProductDetailsPage'
                    and _url_query like '%productID%'
                    then cast(regexp_extract(_url, '/product/([0-9]+).html', 1) as int) 
                when event in ('searchResultClick', 'addToShopcart') then cast(productid as int)
                else null
            end as product_code,
            dt,
            distinct_id,
            event,
            platformtype,
            _url,
            _url_path,
            time,
            lag(`time`,1) over(partition by distinct_id,`_url` order by time) as next_time
            from ods_sa_events
        where dt >= '${var:yes_date}'
    ) t
    where  ((event = 'viewProductDetail'
             and platformtype in ('商城APP_IOS', '商城APP_安卓'))
    or (event = '$pageview'
                and parse_url(_url, "PATH") rlike '/product/[0-9]+.html' )
    or (event = '$MPViewScreen'
                    and _url_path in (
                        -- 拼团产品页
                        'pagescollage/pages/productdetails/productdetails',
                        -- 普通产品页
                        'pages/productdetail/jkProductDetailsPage'
                    )))
    and time is not null and next_time is not null
    group by product_code,
    dt
) t3 on t1.product_code=t3.product_code and t1.event_date=t3.dt

left join 
(
    select
    product_code,
    dt,
    count(1) as detail_page_exit_app_times
    from 
    (
        select 
            case
                -- App
                when event = 'viewProductDetail'
                    and platformtype in ('商城APP_IOS', '商城APP_安卓')
                    and productid is not null
                    then cast(productid as int)
                -- Web
                when event = '$pageview'
                    and parse_url(_url, "PATH") rlike '/product/[0-9]+.html' 
                    and regexp_extract(_url, '/product/([0-9]+).html', 1) is not null
                    then cast(regexp_extract(_url, '/product/([0-9]+).html', 1) as int) 
                -- 小程序
                when event = '$MPViewScreen'
                    and _url_path = 'pages/productdetail/jkProductDetailsPage'
                    and _url_query like '%productID%'
                    then cast(regexp_extract(_url, '/product/([0-9]+).html', 1) as int) 
                when event in ('searchResultClick', 'addToShopcart') then cast(productid as int)
                else null
            end as product_code,
        distinct_id,
        dt,
            event,
            platformtype,
            _url,
            _url_path,
            time,
            lag(event,1) over(partition by distinct_id,`_url` order by time) as next_event
            -- unix_timestamp(next_time,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(`_date`,'yyyy-MM-dd HH:mm:ss')
            from ods_sa_events
        where dt >= '${var:yes_date}'
    ) t1 
    where  ((event = 'viewProductDetail'
             and platformtype in ('商城APP_IOS', '商城APP_安卓'))
    or (event = '$pageview'
                and parse_url(_url, "PATH") rlike '/product/[0-9]+.html' )
    or (event = '$MPViewScreen'
                    and _url_path in (
                        -- 拼团产品页
                        'pagescollage/pages/productdetails/productdetails',
                        -- 普通产品页
                        'pages/productdetail/jkProductDetailsPage'
                    )))
    and next_event ='AppEnd'
    group by product_code,
    dt

) t4 on t1.product_code=t4.product_code and t1.event_date=t4.dt
left join dim_product_class dpc on t1.product_code = dpc.product_code
left join dim_product p on t1.product_code = p.product_code
left join (select * from ods_product_images_parquet where is_main is true) pi on t1.product_code = pi.product_code
left join (

    select 
        product_code,
        event_date,
        count(distinct event_user_id) as cart_add_user_num
    from jkbd.fact_user_event_detail
	where event_cn ='加入购物车'
	   and event_date >='${var:yes_date}'
    group by product_code,
    event_date
) t10 on t1.product_code = t10.product_code and t1.event_date = t10.event_date

left join (

    select 
        product_code,
        event_date,
        count(distinct event_user_id) as favorite_add_user_num
    from jkbd.fact_user_event_detail
	where event_cn ='收藏商品'
	  and event_date >='${var:yes_date}'
    group by product_code,
    event_date
) t11 on t1.product_code = t11.product_code and t1.event_date = t11.event_date
full join
(
    select 
        t5.product_code,
        t5.product_name,
        pi.image_url,
        t5.large_class,
        t5.small_class,
        t5.order_date,
        t5.order_num,
        t5.sale_amount,
        t5.product_num,
        t5.cost_amount,
        t5.gross_profit,
        t8.order_user_num
        from 
    (
        select 
            product_code,
            product_name,
            large_class,
            small_class,
            order_date,
            count(distinct orders_code) as order_num,
            sum(product_amount) as sale_amount,
            sum(product_quantity) as product_num,
            sum(product_cost) as cost_amount,
            sum(product_amount)-sum(product_cost) as gross_profit
        from jkbd.fact_export_detail_full
        where 
        	 EL_RECORD_STATE = 2 
				AND EF_RECORD_STATE = 2 
				AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
				AND order_type not in('门店','批发','批零一体化')
				AND form_rank = 1
                AND export_form_type != '退货单'
				AND is_group = 0
				AND is_gift = '非赠品'
				AND product_code!=857934
				AND product_code!=857932
        and order_date>='${var:yes_date}'
        group by product_code,
                large_class,
                small_class,
                product_name,
            order_date
    ) t5
  
    left join (select * from ods_product_images_parquet where is_main is true) pi on t5.product_code=pi.product_code
    left join 
    (
    select 
            product_code,
            product_name,
            large_class,
            small_class,
            order_date,
            count(distinct account_id) as order_user_num
        from jkbd.fact_export_detail_full
        where 
        	 EL_RECORD_STATE = 2 
				AND EF_RECORD_STATE = 2 
				AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
				AND order_type not in('门店','批发','批零一体化')
				AND form_rank = 1
                AND export_form_type != '退货单'
				AND is_group = 0
				AND is_gift = '非赠品'
				AND product_code!=857934
				AND product_code!=857932
        and order_date>='${var:yes_date}'
        group by product_code,
                large_class,
                small_class,
                product_name,
            order_date
    ) t8 on t5.product_code=t8.product_code and t5.order_date = t8.order_date
) t7 on t1.product_code=t7.product_code and t1.event_date=t7.order_date
  left join   
    (
        select  
            order_date,
            count(distinct orders_code) as total_product_order_num
        from jkbd.fact_export_detail_full
        where order_date >= '${var:yes_date}'
  
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
        group by order_date
    ) t6 on t1.event_date=t6.order_date or t7.order_date=t6.order_date;