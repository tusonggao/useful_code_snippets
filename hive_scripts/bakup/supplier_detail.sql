create  table if not exists sjcm.dwd_supplier_detail(
supplier_type string comment '供应商类型',
supplier_name string comment '名称',
supplier_id string comment 'ID',
company string comment '供应商所属公司',
create_date timestamp comment '创建时间',
is_active int comment '1是活跃，0不活跃',
is_rebate int comment '1是返利供应商，0不是',
purchase_amount_month double comment '月采购金额',
month_tb double comment '月同比',
month_hb double comment '月环比',
updata_time  timestamp comment '更新时间',
is_used int comment '1是启用，0停用',
is_new int comment '是否是今年新增，1是，0否',
purchase_product_month bigint comment '月采购商品数',
product_month_tb double comment '商品数月同比',
product_month_hb double comment '商品数月环比',
purchase_product_quantity_month bigint comment '月采购产品件数',
quantity_month_tb double comment '产品件数月同比',
quantity_month_hb double comment '产品件数月环比'
)
partitioned by(dt string)
comment '供应商详情表'
row format delimited fields terminated by '\u0001';


create view if not exists views.supplier_detail_view
as
select 
	wh.company_info_id,
	storage_supplier_id,
	to_date(reviewer_date_time) purchase_date,
	storage_actual_amount,
	storage_actual_quantity,
	product_code,
	trunc('${var:yes_date}','MM') month_first, --这个月初
	months_sub(to_date('${var:yes_date}'), 1) last_month_today, --上个月的今天
	months_sub(to_date('${var:yes_date}'), 12) last_year_today, --去年的今天
	trunc(months_sub('${var:yes_date}', 1),'MM') last_month_first, --上个月初
	trunc(months_sub('${var:yes_date}', 12),'MM') last_year_first, --去年月初
	trunc(now(),'yyyy') this_year,								--今年初2019-01-01
	months_sub('${var:yes_date}',6) half_year --六个月前日期
from jkbd.fact_storage_detail_full fsd
left join
	jkbd.warehouse_parquet wh
on 	fsd.warehouse_id=wh.id
where
    1=1
	and storage_form_status_cn='完成'
	and storage_form_type_cn='业务采购单入库'
	and storage_supplier_name not like '%健客%'
	and storage_supplier_name not like '%广州云医%'
	and storage_actual_quantity>0
;



insert overwrite table sjcm.dwd_supplier_detail partition(dt)
select 
	dtl.supplier_type,
	dtl.name,
	dtl.id,
	dtl.company,
	dtl.creation_date,
	if(half_year_amount1>0,1,0), --半年有采购则是活跃，否则不活跃
	dtl.is_rebate,
	nvl(month_purchase_amount,0),
	nvl(case when nvl(month_purchase_amount,0)!=0 and nvl(last_year_month_purchase_amount,0)=0  then 1 when nvl(last_year_month_purchase_amount,0)=0 then 1  else (month_purchase_amount-last_year_month_purchase_amount)/last_year_month_purchase_amount end,0) as month_tb,
	nvl(case when nvl(month_purchase_amount,0)!=0 and nvl(last_month_purchase_amount,0)=0  then 1 when nvl(last_month_purchase_amount,0)=0 then 1 else (month_purchase_amount-last_month_purchase_amount)/last_month_purchase_amount end,0) as month_tb,
	'${var:yes_date}' updata_time,
	dtl.is_active is_used,
	if(before_this_year_amount1<=0 and this_year_amount1>0,1,0) is_new, --今年之前的采购金额大于0，则不是新增供应商	
	nvl(month_purchase_product,0),
	nvl(case when nvl(month_purchase_product,0)!=0 and nvl(last_year_month_purchase_product,0)=0  then 1 when nvl(last_year_month_purchase_product,0)=0 then 1 else (month_purchase_product-last_year_month_purchase_product)/last_year_month_purchase_product end,0) as product_month_tb,
	nvl(case when nvl(month_purchase_product,0)!=0 and nvl(last_month_purchase_product,0)=0  then 1 when nvl(last_month_purchase_product,0)=0 then 1 else (month_purchase_product-last_month_purchase_product)/last_month_purchase_product end,0) as product_month_hb,
	nvl(month_purchase_quantity,0),
	nvl(case when nvl(month_purchase_quantity,0)!=0 and nvl(last_year_month_purchase_quantity,0)=0  then 1 when nvl(last_year_month_purchase_quantity,0)=0 then 1 else (month_purchase_quantity-last_year_month_purchase_quantity)/last_year_month_purchase_quantity end,0) as product_month_tb,
	nvl(case when nvl(month_purchase_quantity,0)!=0 and nvl(last_month_purchase_quantity,0)=0  then 1 when nvl(last_month_purchase_quantity,0)=0 then 1 else (month_purchase_quantity-last_month_purchase_quantity)/last_month_purchase_quantity end,0) as quantity_month_hb,
	'${var:yes_date}' dt
from 	
--供应商信息
	(select  
		case when sp.supplier_classify=2 then '商业供应商'
			 when sp.supplier_classify=3 then '代理供应商'
			 when sp.supplier_classify=4 then '厂家供应商'
		else '其他' end as supplier_type,
		sp.name,
		sp.id,
		cif.name as company,
		sp.creation_date,
		if(sq.status=2,1,0) is_active,
		if(rs.rebate_source_name is null,0,1) is_rebate,
		cif.id company_id
	from 
		jkbd.supplier_base_parquet sp
	left join
		jkbd.supplier_qualification sq
	on 
		sp.id=sq.Supplier_baseId
	left join
		jkbd.company_info cif
	on
		sq.CompanyInfoId=cif.id
	left join
		jkbd.rebate_source_parquet rs
	on  trim(sp.name)=trim(rs.rebate_source_name)

	)dtl
--供应商所属企业(金额,商品数,商品件数):  当月采购，上个月同期采购，去年同期采购
left join
	(select 
		storage_supplier_id,
		company_info_id,
		sum(case when purchase_date>=month_first and purchase_date<='${var:yes_date}' then storage_actual_amount else 0 end) month_purchase_amount,
		sum(case when purchase_date>=last_month_first and purchase_date<=last_month_today then storage_actual_amount else 0 end) last_month_purchase_amount,
		sum(case when purchase_date>=last_year_first and purchase_date<=last_year_today then storage_actual_amount else 0 end) last_year_month_purchase_amount,
		
		sum(case when purchase_date>=month_first and purchase_date<='${var:yes_date}' then storage_actual_quantity else 0 end) month_purchase_quantity,
		sum(case when purchase_date>=last_month_first and purchase_date<=last_month_today then storage_actual_quantity else 0 end) last_month_purchase_quantity,
		sum(case when purchase_date>=last_year_first and purchase_date<=last_year_today then storage_actual_quantity else 0 end) last_year_month_purchase_quantity
	from			
		views.supplier_detail_view
	group by	
		storage_supplier_id,company_info_id
	)pcs	
on upper(dtl.id)=upper(pcs.storage_supplier_id) and dtl.company_id=pcs.company_info_id
--这个月的采购商品数量
left join
	(select
	storage_supplier_id,
	company_info_id,
	count(distinct product_code) month_purchase_product
	from			
		views.supplier_detail_view
	where 	purchase_date>=month_first and purchase_date<='${var:yes_date}'
	group by	
		storage_supplier_id,company_info_id	
	)this_mon
on 	upper(dtl.id)=upper(this_mon.storage_supplier_id) and dtl.company_id=this_mon.company_info_id

--上个月的采购商品数量
left join
	(select
		storage_supplier_id,
		company_info_id,
		count(distinct product_code) last_month_purchase_product
	from			
		views.supplier_detail_view
	where 	purchase_date>=last_month_first and purchase_date<=last_month_today
	group by	
		storage_supplier_id,company_info_id	
	)last_mon
on 	upper(dtl.id)=upper(last_mon.storage_supplier_id) and dtl.company_id=last_mon.company_info_id

--去年同比的采购商品数量
left join
	(select
		storage_supplier_id,
		company_info_id,
		count(distinct product_code) last_year_month_purchase_product
	from			
		views.supplier_detail_view
	where 	purchase_date>=last_year_first and purchase_date<=last_year_today
	group by	
		storage_supplier_id,company_info_id	
	)last_yr
on 	upper(dtl.id)=upper(last_yr.storage_supplier_id) and dtl.company_id=last_yr.company_info_id

--供应商:   今年之前的采购额，半年采购额。。。。。。。判断是否活跃供应商,是否新增供应商
left join
	(select 
		storage_supplier_id,
		sum(case when purchase_date<this_year  then storage_actual_amount else 0 end) before_this_year_amount1,
		sum(case when purchase_date>=this_year  then storage_actual_amount else 0 end) this_year_amount1,
		sum(case when purchase_date>half_year  then storage_actual_amount else 0 end) half_year_amount1
	from	
		(
		select 
		storage_supplier_id,
		to_date(reviewer_date_time) purchase_date,
		storage_actual_amount,
		trunc('${var:yes_date}','MM') month_first, --这个月初
		months_sub(to_date('${var:yes_date}'), 1) last_month_today, --上个月的今天
		months_sub(to_date('${var:yes_date}'), 12) last_year_today, --去年的今天
		trunc(months_sub('${var:yes_date}', 1),'MM') last_month_first, --上个月初
		trunc(months_sub('${var:yes_date}', 12),'MM') last_year_first, --去年月初
		trunc(now(),'yyyy') this_year,								--今年初2019-01-01
		months_sub('${var:yes_date}',6) half_year --六个月前日期
	from jkbd.fact_storage_detail_full 	
	where 
		storage_form_status_cn='完成'
		and storage_form_type_cn='业务采购单入库'
		and storage_actual_quantity>0
		)tmp
	group by	
		storage_supplier_id
	)pcs2	
on upper(dtl.id)=upper(pcs2.storage_supplier_id) 


