-- 用户事件模型
-- 用以计算漏斗，应涉及用户生命周期的关键事件

-- 包括事件
-- 安装 APP、启动 APP
-- 登录、注册
-- 搜索、浏览商品、加入购物车、添加收藏
-- 提交订单、支付订单、领券

drop table jkbd.fact_user_event_detail;
create table jkbd.fact_user_event_detail (
    sa_user_id double comment '神策用户自增 ID',
    sa_distinct_id string comment '神策 distinct ID',
    event_user_id string comment '事件用户ID：如果 sa_distinct_id 为空取 sa_user_id',
    event_time timestamp comment '行为发生时间',
    is_first_event_day int comment '是否首日（浏览行为）',
    is_first_order_day int comment '是否首日（下单行为）',
    event_cn string comment "当前行为",
    event_platform string comment "当前行为所在平台：对应 sls_platform",
    event_origin string comment "当前行为所在来源：对应 sls_origin",
    event_suborigin string comment "浏览等行为：用于区分 iOS 和 Android",
    -- 行为属性比较稀疏
    referrer_event_cn string comment "行为属性：来源行为",
    -- latest string comment "行为属性：订单编号",
    product_code int comment "浏览/下单/加入购物车 属性：商品编号",
    orders_code string comment "下单 属性：订单编号",
    coupon_ids string comment "下单 属性：优惠券编号",
    search_keyword string comment "搜索 属性：搜索关键词",
    event_duration double comment "APP 退出 属性：APP 使用时长"
) partitioned by (event_date string comment "行为发生日期（分区值）")
comment "用户事件表"
stored as parquet;


-- 安装 APP、启动 APP
-- 小程序启动
-- 登录、注册
-- 搜索、浏览商品、加入购物车
insert overwrite table jkbd.fact_user_event_detail (
    sa_user_id,
    sa_distinct_id,
    event_user_id,
    event_time,
    event_cn,
    event_platform,
    event_origin,
    event_suborigin,
    referrer_event_cn,
    product_code,
    search_keyword,
    event_duration
) partition(event_date)
select 
    sa_user_id,
    sa_distinct_id,
    if(sa_distinct_id is null, cast(sa_user_id as string), sa_distinct_id) as event_user_id,
    event_time,
    is_first_event_day,
    event_cn,
    event_platform,
    event_origin,
    event_suborigin,
    referrer_event_cn,
    product_code,
    search_keyword,
    event_duration,
    event_date
from (
    select 
        e.user_id as sa_user_id,
        upper(
            case
                when u.second_id is not null then u.second_id
                when u.first_id is not null then u.first_id
                else e.distinct_id
            end
        ) as sa_distinct_id,
        e.time as event_time,
        is_first_day as is_first_event_day,
        cast(to_date(e._date) as string) as event_date,

        -- 当前行为 ------------------------------
        case
            when e.event = '$AppStart' then 'App 启动'
            when e.event = 'AppStartPassively' then 'App 后台启动'
            when e.event = 'AppInstall' then 'App 安装'
            when e.event = 'login' then '登录'
            when e.event = 'AppEnd' then 'App 退出'
            when e.event = '$AppChannelMatching' then '点击链接'

            when e.event = '$MPShow' then '小程序启动'

            -- 浏览商品
            -- -- APP
            when e.event = 'viewProductDetail'
                and platformtype in ('商城APP_IOS', '商城APP_安卓')
                then '浏览商品'
            -- -- WEB
            when e.event = '$pageview'
                and parse_url(_url, "PATH") rlike '/product/[0-9]+.html' 
                then '浏览商品'
            -- -- 小程序
            when e.event = '$MPViewScreen'
                and _url_path in (
                    -- 拼团产品页
                    'pagescollage/pages/productdetails/productdetails',
                    -- 普通产品页
                    'pages/productdetail/jkProductDetailsPage'
                )
                -- and _url_query like '%productID%'
                then '浏览商品'

            -- 搜索
            -- -- APP / WEB
            when e.event = 'search' then '搜索'
            -- -- 小程序
            when e.event = '$MPViewScreen'
                and _url_path like '%search%'
                and _url_query like '%searchKey%'
                then '搜索'
            
            -- 搜索结果点击
            -- -- APP / WEB
            when e.event = 'searchResultClick' then '点击搜索结果'

            -- 加入购物车
            -- APP / WEB 
            when e.event = 'addToShopcart' then '加入购物车'
            else '未知'
        end as event_cn,

        -- 行为所属平台 ------------------------------
        "官网" as event_platform,
        case
            when e.event in ('$MPViewScreen', '$MPShow') then '官网-小程序（健客网上药店+）'
            when e.platformtype = '商城APP_IOS' then '官网-APP（健客网上药店）'
            when e.platformtype = '商城APP_安卓' then '官网-APP（健客网上药店）'
            when parse_url(e._url, "HOST") in ('m.jianke.com', 'm-zt.jianke.com') or e.platformtype = '商城M端_M' then '官网-M端'
            when parse_url(e._url, "HOST") = 'www.jianke.com' or e.platformtype = '商城PC端_PC' then '官网-PC端'
            when parse_url(e._url, "HOST") in ('v.jianke.com', 'v-zt.jianke.com') or e.platformtype = '商城V端_V' then '官网-V端'
            when parse_url(e._url, "HOST") = 'baidu-m.jianke.com' or e.platformtype = '百度合作商城' then '百度合作商城'
            else '未知'
        end as event_origin,
        case
            when e.event in ('$MPViewScreen', '$MPShow') then '官网-小程序（健客网上药店+）'
            when platformtype = '商城APP_IOS' then '官网-APP iOS（健客网上药店）'
            when platformtype = '商城APP_安卓' then '官网-APP Android（健客网上药店）'
            when parse_url(_url, "HOST") in ('m.jianke.com', 'm-zt.jianke.com') or platformtype = '商城M端_M' then '官网-M端'
            when parse_url(_url, "HOST") = 'www.jianke.com' or platformtype = '商城PC端_PC' then '官网-PC端'
            when parse_url(_url, "HOST") in ('v.jianke.com', 'v-zt.jianke.com') or platformtype = '商城V端_V' then '官网-V端'
            when parse_url(_url, "HOST") = 'baidu-m.jianke.com' or platformtype = '百度合作商城' then '百度合作商城'
            else '未知'
        end as event_suborigin,

        -- 当前行为来源 ------------------------------
        case
            -- 搜索
            when parse_url(`_referrer`, 'PATH') like '/search%'
                or _url like 'https://search.jianke.com/prod%'
                or _url like 'http://search.jianke.com/prod%' 
                then "搜索"
            else null
        end as referrer_event_cn,

        -- 商品编号  ------------------------------
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
                then cast(regexp_extract(_url_query, 'productID=([0-9]+)', 1) as int) 
            when event in ('searchResultClick', 'addToShopcart') then cast(productid as int)
            else null
        end as product_code,

        -- 优惠券 ------------------------------


        -- 搜索关键词 ------------------------------
        case
            -- -- APP / WEB
            when event in ('search', 'searchResultClick') then search_key
            -- -- 小程序
            when event = '$MPViewScreen'
                and _url_path like '%search%'
                and _url_query like '%searchKey%'
                then regexp_extract(_url, 'searchKey=(.*?)&', 1) 
        end as search_keyword,

        -- 持续时间 ------------------------------
        case
            when event = 'AppEnd' then event_duration
        end as event_duration

    from jkbd.ods_sa_events e
    left join jkbd.ods_sa_users u on e.user_id = u.id
    where 1=1
    and event in (
        '$AppStart', 'AppInstall', 'login', 
        'AppStartPassively', 'AppEnd', '$AppChannelMatching',
        'viewProductDetail', '$pageview',
        'AppInstall', 'installOtherAPP', 'search', 'searchResultClick',
        '$MPViewScreen', 'addToShopcart', '$MPShow'
    )
    and _date >= '2018-01-01'
) tbl
where event_cn != '未知';



--  每小时首次启动 Web
insert into table jkbd.fact_user_event_detail (
    sa_user_id,
    sa_distinct_id,
    event_user_id,
    event_time,
    is_first_event_day,
    event_cn,
    event_platform,
    event_origin,
    event_suborigin
) partition(event_date)
select 
    sa_user_id,
    sa_distinct_id,
    if(sa_distinct_id is null, cast(sa_user_id as string), sa_distinct_id) as event_user_id,
    event_time,
    is_first_day as is_first_event_day,
    event_cn,
    event_platform,
    event_origin,
    event_suborigin,
    event_date
from (
    select 
        e.user_id as sa_user_id,
        upper(
            case
                when u.second_id is not null then u.second_id
                else u.first_id
            end
        ) as sa_distinct_id,
        min(e.time) as event_time,
        "Web 首次启动" as event_cn,
        "官网" as event_platform,
        case
            when parse_url(e._url, "HOST") in ('m.jianke.com', 'm-zt.jianke.com') or e.platformtype = '商城M端_M' then '官网-M端'
            when parse_url(e._url, "HOST") = 'www.jianke.com' or e.platformtype = '商城PC端_PC' then '官网-PC端'
            when parse_url(e._url, "HOST") in ('v.jianke.com', 'v-zt.jianke.com') or e.platformtype = '商城V端_V' then '官网-V端'
            when parse_url(e._url, "HOST") = 'baidu-m.jianke.com' or e.platformtype = '百度合作商城' then '百度合作商城'
            else '未知'
        end as event_origin,
        case
            when parse_url(_url, "HOST") in ('m.jianke.com', 'm-zt.jianke.com') or platformtype = '商城M端_M' then '官网-M端'
            when parse_url(_url, "HOST") = 'www.jianke.com' or platformtype = '商城PC端_PC' then '官网-PC端'
            when parse_url(_url, "HOST") in ('v.jianke.com', 'v-zt.jianke.com') or platformtype = '商城V端_V' then '官网-V端'
            when parse_url(_url, "HOST") = 'baidu-m.jianke.com' or platformtype = '百度合作商城' then '百度合作商城'
            else '未知'
        end as event_suborigin,
        cast(to_date(e._date) as string) as event_date
    from jkbd.ods_sa_events e
    left join jkbd.ods_sa_users u on e.user_id = u.id
    where 1=1
    and event in ('$pageview')
    and _date >= '2018-01-01'
    group by 
        sa_user_id,
        sa_distinct_id,
        event_cn,
        event_platform,
        event_origin,
        event_suborigin,
        event_date,
        trunc(e.time, 'HH')
) tbl;




-- 订单 --------------------------
-- -- 提交订单
insert into table jkbd.fact_user_event_detail (
    sa_distinct_id,
    event_user_id,
    event_time,
    event_cn,
    event_platform,
    event_origin,
    event_suborigin,
    product_code,
    orders_code,
    coupon_ids
) partition(event_date)
select
    upper(account_id) as sa_distinct_id,
    upper(account_id) as event_user_id,
    create_time as event_time,
    case 
        when to_date(all_frist_order_time) = to_date(create_time) then 1
        else 0
    end as is_first_order_day,
    "提交订单" as event_cn,
    sls_platform as event_platform,
    sls_origin as event_origin,
    case 
        when sls_origin = '官网-APP（健客网上药店）' 
            and origin_type_cn in ('健客IOS版APP', '健客医生IOS版')
            then '官网-APP iOS（健客网上药店）'
        when sls_origin = '官网-APP（健客网上药店）' 
            and origin_type_cn in ('健客Android版APP', '健客医生Android版')
            then '官网-APP Android（健客网上药店）'
    end as event_suborigin,
    product_code,
    orders_code,
    coupon_ids,
    to_date(create_time) as event_date
from jkbd.fact_export_detail_full ed
left join jkbd.dwd_account_order_time a on ed.account_id = a.account_id
where 1=1
and to_date(create_time) >= '2018-01-01'
and sls_platform = '官网';


-- -- 支付订单
insert into table jkbd.fact_user_event_detail (
    sa_distinct_id,
    event_user_id,
    event_time,
    is_first_order_day
    event_cn,
    event_platform,
    event_origin,
    event_suborigin,
    product_code,
    orders_code,
    coupon_ids
) partition(event_date)
select
    upper(account_id) as sa_distinct_id,
    upper(account_id) as event_user_id,
    pay_time as event_time,
    case 
        when to_date(all_frist_order_time) = to_date(pay_time) then 1
        else 0
    end as is_first_order_day,
    "支付订单" as event_cn,
    sls_platform as event_platform,
    sls_origin as event_origin,
    case 
        when sls_origin = '官网-APP（健客网上药店）' 
            and origin_type_cn in ('健客IOS版APP', '健客医生IOS版')
            then '官网-APP iOS（健客网上药店）'
        when sls_origin = '官网-APP（健客网上药店）' 
            and origin_type_cn in ('健客Android版APP', '健客医生Android版')
            then '官网-APP Android（健客网上药店）'
    end as event_suborigin,
    product_code,
    orders_code,
    coupon_ids,
    to_date(pay_time) as event_date
from jkbd.fact_export_detail_full ed
left join jkbd.dwd_account_order_time a on ed.account_id = a.account_id
where 1=1
and to_date(pay_time) >= '2018-01-01'
and sls_platform = '官网';





-- 收藏
insert into table jkbd.fact_user_event_detail (
    sa_distinct_id,
    event_user_id,
    event_time,
    event_cn,
    product_code
) partition(event_date)
select 
    upper(account_id) as sa_distinct_id,
    upper(account_id) as event_user_id,
    creation_date as event_time,
    "收藏商品" as event_cn,
    product_code,
    to_date(creation_date) as event_date
from jkbd.my_favorites_parquet
where to_date(creation_date) >= '2018-01-01'
and type in (1, 2);







-- 领取优惠券
insert into table jkbd.fact_user_event_detail (
    sa_distinct_id,
    event_user_id,
    event_time,
    event_cn,
    event_platform,
    event_origin,
    coupon_ids
) partition(event_date)
select 
    upper(c.account_id) as sa_distinct_id,
    upper(c.account_id) as event_user_id,
    cast(c.created_date as timestamp) as event_time,
    "领取优惠券" as event_cn,
    "官网" as event_platform,
    case ca.device_type
        when "1" then "官网-PC端"
        when "2" then "官网-M端"
        when "3" then "官网-APP（健客网上药店）"
        when "4" then "官网-V端"
        else "其它"
    end as event_origin,
    cast(c.activity_id as string) as coupon_ids,
    to_date(c.created_date) as event_date
from jkbd.coupon_parquet c 
left join jkbd.coupon_activity_parquet ca on c.activity_id = ca.id
where to_date(c.created_date) >= '2018-01-01';