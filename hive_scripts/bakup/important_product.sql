CREATE TABLE IF NOT EXISTS sjcm.dwd_order_important_product(
	product_code bigint comment '商品ID',
	product_name string comment '商品名',
	product_large_class  string comment '大类',
	product_small_class  string comment '小类', 
	product_packing string comment '包装规格',
	product_inventory int comment '库存数量',
	inventory_amount decimal(13,4) comment '库存金额',
	inventory_day int comment '库存周转天数',
	status int comment '0-正常,1-滞销,2-滞销预警',
	avg_sale_amount decimal(13,4) comment '平均销售额',
	is_onsale_official	int comment '是否官网上架',
	is_near_validity int comment '是否近效期',
	near_validity_inventory int comment '近效期在库数量',
	near_validity_inventory_amount decimal(13,4) comment '近效期库存金额',
	product_img string comment '商品图片链接',
	last_supplier string comment '最后一次采购供应商',
	purchase_price decimal(13,4) comment '采购价',
	last_month_purchase_amount double comment '上一自然月采购金额'
)
partitioned by(dt string)
row format delimited fields terminated by '\u0001';
truncate table sjcm.dwd_order_product;

drop view if exists views.product_sale_30_days_view;
create view if not exists views.product_sale_30_days_view
as
SELECT 
	product_code,
	sum(product_quantity) product_quantity,
	sum(product_amount) product_amount,
	sum(nvl(product_amount,0)-nvl(product_cost,0)) gross_profit
FROM
	jkbd.fact_export_detail_full
WHERE 							
		order_date <= '${var:yes_date}'
		AND order_date >= to_date(date_sub('${var:yes_date}',29))
		AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
		AND EL_RECORD_STATE = 2 
		AND EF_RECORD_STATE = 2 
		AND order_type not in ('门店', '批零一体化', '批发', '第三方发货')
		AND export_form_type in ('出库单')
		AND form_rank = 1
		AND is_group = 0
		AND is_gift = '非赠品'
		AND product_code not in (857932, 857934)				
group by 
		product_code	
;


INSERT OVERWRITE TABLE sjcm.dwd_order_important_product partition(dt)
	SELECT 
			cast(t5.product_code as bigint) product_code,
			nvl(product_name,'') product_name,
			nvl(product_large_class,'') product_large_class,
			nvl(product_small_class,'') product_small_class,
			nvl(packing,'') packing,
			cast(ifnull(product_inventory,0) as int) product_inventory,
			cast(ifnull(inventory_amount,0.0) as DECIMAL(13,4)) inventory_amount,
			cast(round(nvl(product_inventory/product_quantity*30,0)) as int) inventory_day,
			case  
				 when (storage_age>90 and product_quantity=0) or ( storage_age>90 and product_quantity is null) or (product_inventory/product_quantity*30 >90 and storage_age>90)  then 1
			     when product_inventory/product_quantity*30 >=60 then 2
				 when product_inventory/product_quantity*30<60 then 0
				 else 0
				 end as status,
			cast(nvl(avg_sale_amount,0) as DECIMAL(13,4))  avg_sale_amount,
			if(ofc.product_code is null,0,1) is_onsale_official,
			if(nv.product_code is null ,0,1) is_near_validaty,
			cast(nvl(if(near_validaty_inventory<0,0,near_validaty_inventory),0) as int) near_validaty_inventory,
			cast(nvl(if(near_validaty_inventory_amount<0,0,near_validaty_inventory_amount),0) as  DECIMAL(13,4)) near_validaty_inventory_amount,
			product_image_url,
			nvl(lsp.supplier_name,''),
			cast(nvl(purchase_price,0) as DECIMAL(13,4)),
			nvl(last_month_purchase_amount,0),
			'${var:yes_date}' as dt
		FROM		
			(
		SELECT
			COALESCE(t3.product_code,t4.product_code) product_code,
			'${var:yes_date}' create_date,
			product_inventory,
			inventory_amount,
			product_quantity,
			inventory_price*product_quantity/30  avg_sale_amount
		FROM
			--该日期下产品库存 库存金额
				(
				SELECT
					gs.product_code,
					create_date,
					sum(ifnull(ending_quantity,0)) as product_inventory,
					sum(ending_amount) as inventory_amount,
					if(sum(ending_quantity)=0,0,sum(ending_amount)/sum(ending_quantity)) as inventory_price
				FROM 
					jkbd.dwd_goods_inventory_daily_parquet as dt
				join
					jkbd.goods_parquet gs
				on dt.goods_id=gs.id
				AND dt.create_date='${var:yes_date}'
				group by gs.product_code,create_date		
				)t3	
	    FULL OUTER JOIN
			--产品30天总销售量
				(
				SELECT 
					product_code,
					sum(ifnull(product_quantity,0)) product_quantity
				FROM
					jkbd.fact_export_detail_full
				WHERE 							
						order_date <= '${var:yes_date}'
						AND order_date >= to_date(date_sub('${var:yes_date}',29))
						AND (order_status not in (0, 5, 100, 200) or (sls_platform = '全球购' and pay_time is not null ))
						AND EL_RECORD_STATE = 2 
						AND EF_RECORD_STATE = 2 
						AND order_type not in ('门店', '批零一体化', '批发', '第三方发货')
						AND export_form_type in ('出库单')
						AND form_rank = 1
						AND is_group = 0
						AND is_gift = '非赠品'
						AND product_code not in (857932, 857934)				
				group by 
						product_code	
				)t4
			on t3.product_code=t4.product_code			
		)t5
	left JOIN 
	--产品信息
		(
		SELECT
			product_code,
			product_name,
			large_class as product_large_class,
			small_class as product_small_class,
			product_image_url,
			packing
		FROM
			jkbd.DIM_PRODUCT
		)t1
	ON t5.product_code=t1.product_code
	LEFT JOIN
	--是否近效期  近效期库存数量 近效期库存金额
		(
		select 
			product_code,
			sum(inventory) near_validaty_inventory,
			sum(inventory_amount) near_validaty_inventory_amount
		from
		(
			select 
				gd.product_code,
				mass_date,
				sum(number_balance) inventory,  
				sum(gd.number_balance * i.cost_price) inventory_amount
			from 
				jkbd.goods_detail_parquet gd
			join
			(
				select
					goods_id, 
					cost_price 
				from 
					jkbd.dwd_goods_inventory_daily_parquet
				where
					create_date = '${var:yes_date}'
			)i 
			on 
				i.goods_id = gd.goods_id
				and  gd.record_state = 2
				and  gd.number_balance != 0
				and  datediff(mass_date, now()) < 180	
			group by  
				gd.product_code, mass_date
				having sum(number_balance) != 0	
			)val
			group by product_code
		)nv
	ON t5.product_code=nv.product_code
	LEFT JOIN
	--是否是官网上架
		(
		select 
			product_code,
			product_status_type
		from 
			jkbd.product_parquet
		where 
			product_status_type in (2, 4)
		)ofc
	ON t5.product_code=ofc.product_code	
	--最后一次采购供应商
left join
	(
	select 
		product_code,
		supplier_name,
		purchase_price
	from
	(
		select 
            product_code,
    		first_value(storage_supplier_name) over(partition by product_code order by reviewer_date_time desc) supplier_name,
    		first_value(storage_purchase_price) over(partition by product_code order by reviewer_date_time desc) purchase_price
        from jkbd.fact_storage_detail_full 
        where  1=1
			and storage_form_status_cn='完成'
			and storage_form_type_cn='业务采购单入库'
			and storage_supplier_name not like '%健客%'
			and storage_supplier_name not like '%广州云医%'
			and storage_actual_quantity>0     
	)tmp
		group by product_code ,supplier_name,purchase_price
	)lsp
on t5.product_code=lsp.product_code
	--上一自然月采购金额
left join
	(
	select 
		product_code,
		sum(storage_actual_amount) last_month_purchase_amount
	from jkbd.fact_storage_detail_full 
	where  1=1
		and trunc(storage_date,'MM')=trunc(months_sub(now(), 1),'MM') 
		and storage_form_status_cn='完成'
		and storage_form_type_cn='业务采购单入库'
		and storage_actual_quantity>0   
	group by product_code
	)lmp		
on t5.product_code=lmp.product_code	
join
--重点商品
(select	
     distinct product_code
from	
	(
	--按年出库数量前80%
	select  product_code  from
		(select product_code,month_1,product_name, product_quantity,product_amount,
			sum(product_quantity) over (partition by month_1 order by product_quantity  desc range between unbounded preceding and unbounded following) total_quantity,
			sum(product_quantity) over (partition by month_1 order by product_quantity desc range between unbounded preceding and current row) now_quantity,
			sum(product_quantity) over (partition by month_1 order by product_quantity desc range between unbounded preceding and current row)/sum(product_quantity) over (partition by month_1 order by product_quantity  desc range between unbounded preceding and unbounded following) rate
		from
		(SELECT ed.product_code,substr(ed.order_date,1,4) month_1,ed.product_name,sum(ed.product_quantity) product_quantity,sum(ed.product_amount) product_amount
		from jkbd.fact_export_detail_full ed
		where 1=1
			and ed.EL_RECORD_STATE = 2 
			and ed.EF_RECORD_STATE = 2 
			and ed.order_date >= trunc('${var:yes_date}','yy')
			and ed.order_date <trunc('${var:yes_date}','MM')
			and ed.order_type not in ('门店', '批零一体化', '批发')
			and ed.export_form_type in ('出库单')
			and ed.form_rank = 1
			and ed.is_group = 0
			and ed.product_code not in (857932, 857934,857933)
			and ed.order_status not in (0, 5, 100, 200)
		GROUP BY substr(ed.order_date,1,4),ed.product_code,ed.product_name) a
		) b
		where rate <= 0.8

	union 
	--按年出库金额前80%
	select  product_code from
		(select product_code,product_name, product_quantity,product_amount,
			sum(product_amount) over (partition by month_1 order by product_amount  desc range between unbounded preceding and unbounded following) total_amount,
			sum(product_amount) over (partition by month_1 order by product_amount desc range between unbounded preceding and current row) now_amount,
			sum(product_amount) over (partition by month_1 order by product_amount desc range between unbounded preceding and current row)/sum(product_amount) over (partition by month_1 order by product_amount  desc range between unbounded preceding and unbounded following) rate
		from
		(SELECT ed.product_code,year(ed.order_date) as month_1,ed.product_name,sum(ed.product_quantity) product_quantity,sum(ed.product_amount) product_amount
		from jkbd.fact_export_detail_full ed
		where 1=1
			and ed.EL_RECORD_STATE = 2 
			and ed.EF_RECORD_STATE = 2 
			and ed.order_date >= trunc('${var:yes_date}','yy')
			and ed.order_date <trunc('${var:yes_date}','MM')
			and ed.order_type not in ('门店', '批零一体化', '批发')
			and ed.export_form_type in ('出库单')
			and ed.form_rank = 1
			and ed.is_group = 0
			and ed.product_code not in (857932, 857934,857933)
			and ed.order_status not in (0, 5, 100, 200)
		GROUP BY year(ed.order_date) ,ed.product_code,ed.product_name) a
		) b
		where rate <= 0.8
	)tmp
)top
on t5.product_code=top.product_code	
--库龄
left join
(
	SELECT
	DISTINCT product_code,
	storage_age
from
    (SELECT
        product_code,
        datediff('${var:yes_date}',(first_value(create_date) OVER(PARTITION BY product_code ORDER BY create_date desc))) storage_age
    from
        (
        SELECT 
            product_code,
            storage_quantity,
            create_date,
            first_value(ending_quantity) over(PARTITION BY product_code ORDER BY create_date desc) ending_quantity,
            sum(storage_quantity) over(PARTITION BY product_code ORDER BY create_date desc ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW) last_storage_quantity
        from 
            jkbd.dwd_goods_inventory_daily_parquet
        WHERE 1=1
        and product_code is not null   
        and create_date<='${var:yes_date}'
        )tmp
    WHERE last_storage_quantity>=ending_quantity
    )t
)kl
on  t5.product_code=kl.product_code		
;







