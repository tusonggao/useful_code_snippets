-- jgcm.platform_compare
-- for pi


create table if not exists jgcm.platform_compare (
    platform_other string comment "",
    product_code int comment "商品编号",
    large_class string comment "大类",
    platform_other_price DOUBLE comment "",
    platform_own string comment "",
    platform_own_price DOUBLE comment "",
    platform_own_product_num DOUBLE comment "",
    platform_own_class_product_num DOUBLE comment ""
) partitioned by (count_date string comment "日期")
comment "公司平台VS友商平台每日爬虫价"
stored as parquet;






-- 3.统计各平台产品每日爬虫价

with platform_sku_price AS 
(
	select count_date, 
			platform, 
			is_own_shop, 
			product_code,
			large_class,
			avg(product_price) AS avg_product_price
	from
	(
		select to_date(sp.update_time) AS count_date,
				sp.update_time,
				sp.jk_id AS product_code,
				case when pc.large_class is not null then pc.large_class else '其他' end AS large_class,
				case when pc.small_class is not null then pc.small_class else '其他' end AS small_class,
				sp.sku_price/100.0 AS product_price,
				case when sp.shop = '健客页面价' and sp.source = '官网' then '官网页面价'
					when sp.shop = '云医惠药大药房旗舰店' and sp.source = '天猫' then '天猫云医'
					when sp.shop = '健客大药房旗舰店' and sp.source = '天猫' then '天猫健客'
					when sp.shop = '康爱多大药房旗舰店' and sp.source = '天猫' then '康爱多'
					when sp.shop = '泉源堂大药房旗舰店' and sp.source = '天猫' then '泉源堂'
					when sp.shop = '瑞人堂大药房旗舰店' and sp.source = '天猫' then '瑞人堂'
					when sp.shop = '乡亲大药房旗舰店' and sp.source = '天猫' then '乡亲'
					when sp.shop = '壹号大药房旗舰店' and sp.source = '天猫' then '壹号大药房'
					when sp.shop = '阿里健康大药房' and sp.source = '天猫' then '阿里健康'
					when sp.shop = '国大药房旗舰店' and sp.source = '天猫' then '国大'
					when sp.shop = '华佗大药房旗舰店' and sp.source = '天猫' then '华佗'
					else '其他' end AS platform,
				case when sp.shop = '健客页面价' and sp.source = '官网' then 1
					when sp.shop = '健客网上药店' and sp.source = '官网' then 1
					when sp.shop = '健客大药房旗舰店' and sp.source = '天猫' then 1
					when sp.shop = '云医惠药大药房旗舰店' and sp.source = '天猫' then 1
					else 0 end AS is_own_shop
		from jkbd.hbase_product_price_full_parquet sp
		left join jkbd.product_parquet pd on sp.jk_id = pd.product_code
		left join jkbd.product_class_parquet pc on sp.jk_id = pc.product_code
		where sp.sku_price > 0
	) A
	where platform != '其他'	
	group by count_date, platform, is_own_shop, product_code, large_class
)


-- 4.匹配公司平台VS友商平台每日爬虫价
insert overwrite jgcm.platform_compare partition(count_date) 

select 
        plat_other.platform AS platform_other,
        plat_other.product_code,
        plat_other.large_class,
        plat_other.avg_product_price AS platform_other_price,
        plat_own.platform AS platform_own,
        plat_own.avg_product_price AS platform_own_price,
        plat_summary.platform_product_num AS platform_own_product_num,
        plat_class_summary.platform_class_product_num AS platform_own_class_product_num,
        cast(plat_other.count_date as string) as count_date
from
(
    select *
    from platform_sku_price
    where is_own_shop = 0
) plat_other
inner join
(
    select *
    from platform_sku_price
    where is_own_shop = 1
) plat_own on plat_other.count_date = plat_own.count_date and plat_other.product_code = plat_own.product_code
left join
(
    select count_date,
            platform,
            count(distinct product_code) AS platform_product_num
    from platform_sku_price
    group by count_date, platform
) plat_summary on plat_own.count_date = plat_summary.count_date and plat_own.platform = plat_summary.platform
left join
(
    select count_date,
            platform,
            large_class,
            count(distinct product_code) AS platform_class_product_num
    from platform_sku_price
    group by count_date, platform, large_class
) plat_class_summary on plat_own.count_date = plat_class_summary.count_date 
    and plat_own.platform = plat_class_summary.platform and plat_own.large_class = plat_class_summary.large_class
where plat_other.avg_product_price >= plat_own.avg_product_price*0.5 
and plat_other.avg_product_price <= plat_own.avg_product_price*2
