#!/bin/bash
export PYTHON_EGG_CACHE=./myeggs
export HADOOP_USER_NAME=hdfs
source /etc/profile


 #昨天
yes_date=`date -d "-1 day" +%Y-%m-%d`
 
#开发环境
#mysql账号密码
username=bigdata_admin
password=jianke@123
host=172.17.250.112



#地域分布表
python /opt/datax/bin/datax.py -p"-Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table area'"   area.json



#每日产品流量表	
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_flow_daily where  create_time='\''$yes_date 00:00:00'\'' '"   				product_flow_daily.json


#小时产品流量表
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_flow_hourly where  substr(create_time,1,10)='\''$yes_date'\'' '" 				product_flow_hourly.json

#地区分布表
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_user_distributed_daily where  substr(create_time,1,10)='\''$yes_date'\'' '"   	product_user_distributed_daily.json

#每日产品订单（确认）表 
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host -DpreSql='delete from product_order_daily where  substr(create_time,1,10)='\''$yes_date'\'' '" 					product_order_daily.json

#产品采购明细表 入库明细表
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_purchase_detail where  purchase_time='\''$yes_date 00:00:00'\'' '"  product_purchase_detail.json	
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_stock_in_detail where  stock_in_time='\''$yes_date 00:00:00'\'' '"  product_stock_in_detail.json

#产品日退单表 （汇总）
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host -DpreSql='delete from product_refund_daily where  substr(create_time,1,10)='\''$yes_date'\'' '" 				product_refund_daily.json

#产品（所有）退单明细表 
python /opt/datax/bin/datax.py -p"-Ddt=* -Dusername=$username -Dpassword=$password -Dhost=$host -DpreSql='truncate table product_refund_detail '" 					product_refund_detail.json


#产品关键词搜索转化表 
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host -DpreSql='delete from product_search_word_daily where  substr(create_time,1,10)='\''$yes_date'\'' '" 			product_search_word_daily.json

#关键词（日）搜索统计表 
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host -DpreSql='delete from search_word_daily where  substr(create_time,1,10)='\''$yes_date'\'' '" 			search_word_daily.json

#人群订单表
python /opt/datax/bin/datax.py -p"-Ddt='*' -Dusername=$username -Dpassword=$password -Dhost=$host -DpreSql='truncate table user_group  '" 			user_group.json

#人群每日平台订单表
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password  -Dhost=$host -DpreSql='delete from user_group_sls_origin_daily where  substr(create_time,1,10)='\''$yes_date'\'' '" 	user_group_sls_origin_daily.json


#人群每日产品类目订单表
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password  -Dhost=$host -DpreSql='delete from user_group_product_class_daily where  substr(create_time,1,10)='\''$yes_date'\'' '" 			user_group_product_class_daily.json

#商品概览 类目排行	
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_overview_product_class_daily where  date(create_time)='\''$yes_date'\'' '"   				product_overview_product_class_daily.json

#商品概览 单品排行	
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_overview_product_daily where  date(create_time)='\''$yes_date'\'' '"   				ads_product_overview_product_daily.json


#人群每日订单汇总表
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password  -Dhost=$host -DpreSql='delete from user_group_daily_summary where  substr(create_time,1,10)='\''$yes_date'\'' '" 	user_group_daily_summary.json


#人群贡献表
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password  -Dhost=$host -DpreSql='delete from user_group_hourly where  date(create_time)='\''$yes_date'\'' '" 	ads_user_group_hourly.json
                                                                                                                   
#人群下单地域表                                                                                                    
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password  -Dhost=$host -DpreSql='delete from user_group_order_quantity_distributed where  date(create_time)='\''$yes_date'\'' '" 	ads_user_group_order_quantity_distributed.json

#人群下单时刻表
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password  -Dhost=$host -DpreSql='delete from user_group_order_time_distributed where  date(create_time)='\''$yes_date'\'' '" 	ads_user_group_order_time_distributed.json

#商品概览（小时）表	
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_overview_hourly where  date(create_time)='\''$yes_date'\'' '"   				product_overview_hourly.json

#商品概览（每日）表 	
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_overview_daily where  date(create_time)='\''$yes_date'\'' '"   				product_overview_daily.json

#产品库存（缺货、上架、近效、合格）汇总表
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_in_warehouse_summary_daily where  date(create_time)='\''$yes_date'\'' '"   				product_in_warehouse_summary_daily.json



#供应商汇总（月同比环比）
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table supplier_detail '"   				supplier_detail.json

#供应商数量统计（总量、类型、活跃）
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from  supplier_summary_daily where  date(create_time)='\''$yes_date'\'' '"   				supplier_summary_daily.json

#供应链中心-商品调缺明细
python /opt/datax/bin/datax.py -p"-Ddt='*' -Dusername=$username -Dpassword=$password  -Dhost=$host -DpreSql='truncate table product_adjust_stockout_detail '" 	ads_product_adjust_stockout_detail.json

#供应链中心-商品入库价趋势
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from  product_storage_adjustment where  date(create_time)='\''$yes_date'\'' '"   				ads_product_storage_adjustment.json


#供应链中心-预估成本变化
python /opt/datax/bin/datax.py -p" -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table   cost_change  '"   				cost_change.json

#合作医疗企业
python /opt/datax/bin/datax.py -p" -Dusername=$username -Dpassword=$password -Dhost=$host -DpreSql='truncate table category_current_month_statis ' "   		ods_category_current_month_statis.json
python /opt/datax/bin/datax.py -p" -Dusername=$username -Dpassword=$password -Dhost=$host -DpreSql='truncate table category_day_statis ' "   				ods_category_day_statis.json
python /opt/datax/bin/datax.py -p" -Dusername=$username -Dpassword=$password -Dhost=$host -DpreSql='truncate table category_month_statis ' "   				ods_category_month_statis.json
python /opt/datax/bin/datax.py -p" -Dusername=$username -Dpassword=$password -Dhost=$host -DpreSql='truncate table category_month_target '"   				ods_category_month_target.json
python /opt/datax/bin/datax.py -p" -Dusername=$username -Dpassword=$password -Dhost=$host -DpreSql='truncate table medical_category '"   					ods_medical_category.json

#重点商品
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table important_product'"   important_product.json

##################################################流量表###############################################################
#用户留存对比
python /opt/datax/bin/datax.py -p"-Ddt=* -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table user_retention_compare '"   				user_retention_compare.json

#用户留存趋势
python /opt/datax/bin/datax.py -p"-Ddt=* -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table user_retention_trend '"   				user_retention_trend.json

#流量概览--类目流量表	
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from flow_class_daily where  create_time='\''$yes_date 00:00:00'\''  '"  flow_class_daily.json

#流量概览--来源流量表
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from flow_origin_daily where  create_time='\''$yes_date 00:00:00'\''  '"  flow_origin_daily.json


#流量概览--所有版本
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table flow_version '"   				dwd_flow_version.json

#流量概览--所有渠道
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table flow_channel '"   				dwd_flow_channel.json

#产品编号、产品名称、产品图片链接
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table product_detail '"   				product_detail.json

#产品编号、产品名称、产品图片链接
python /opt/datax/bin/datax.py -p"-Ddt=$yes_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table order_platform '"   				order_platform.json