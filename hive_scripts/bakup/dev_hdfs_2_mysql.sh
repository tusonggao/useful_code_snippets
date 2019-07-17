#!/bin/bash
export PYTHON_EGG_CACHE=./myeggs
export HADOOP_USER_NAME=hdfs
source /etc/profile

mon=`date -d "-1 day" +%Y-%m`
#今天
today_date=`date -d "-0 day" +%Y-%m-%d`


 #每日定时
 #昨天
yes_date=`date -d "-1 day" +%Y%m%d`
 #设定起始和终止日期 格式如 start_date="20190401" end_date="20190403",默认yes_date为昨日日期
start_date=$yes_date
end_date=$yes_date
#mysql账号密码
username=bigdata_admin
password=jianke@123
host=172.17.250.112

while [ "$start_date" -le "$end_date" ];
do
	stat_date=`date -d "$start_date" +%Y-%m-%d`
	start_date=$(date -d "$start_date+1days" +%Y%m%d)
	#执行更新操作
	#每小时销售、每日产品销售、每日汇总表
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from boss_sale_hourly where  substr(create_time,1,10)='\''$stat_date'\'' '"   		boss_sale_hourly.json
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from boss_sale_daily  where  substr(create_time,1,10)='\''$stat_date'\'' '" 				boss_sale_daily.json
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from boss_sale_daily_summary where  substr(create_time,1,10)='\''$stat_date'\'' '"  boss_sale_daily_summary.json

	#产品表  价格调整表 每日产品表 产品入库明细表
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host" 																									product.json	
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host" 																									boss_price_adjustment_details.json
	#product_daily字段增加
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_daily where  create_time='\''$stat_date 00:00:00'\'''" 				boss_product_daily.json
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_log_stock_out_daily where  create_time='\''$stat_date 00:00:00'\'''" 				product_log_stock_out_daily.json
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host" 																									product_stock_in_price_adjustment.json
	
	#流量表 每日平台汇总表 短信统计表 
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from boss_flow_details where flow_date='\''$stat_date'\'' ' "					boss_flow_details.json
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from sls_origin_daily where  substr(create_time,1,10)='\''$stat_date'\'' '"		sls_origin_daily.json
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from sms_statistics_daily where  substr(create_time,1,10)='\''$stat_date'\'' '" 	sms_statistics_daily.json
	#
	#每日事业部汇总表 
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from  business_unit_daily where  substr(create_time,1,10)='\''$stat_date'\'' '" 	boss_business_unit_daily.json
	#套装销售表
	python /opt/datax/bin/datax.py -p"-Ddt=* -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table product_in_group'"  																all_dwd_product_group.json
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_group_daily where  substr(create_time,1,10)='\''$stat_date'\'''"  	ads_product_group_daily.json
	
	#类目表
	python /opt/datax/bin/datax.py -p"-Ddt=* -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table product_class'"  																	all_dwd_product_class.json
	#类目新老用户统计表
	python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_class_daily where  substr(create_time,1,10)='\''$stat_date'\'' '"  	ads_product_class_daily.json
	
	
	#产品出库，入库统计表   产品库存状况表
    python /opt/datax/bin/datax.py -p"-Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='truncate table product_in_warehouse'"  ads_product_in_warehouse.json
    python /opt/datax/bin/datax.py -p"-Ddt=$stat_date -Dusername=$username -Dpassword=$password -Dhost=$host  -DpreSql='delete from product_in_warehouse_daily where  substr(create_time,1,10)='\''$stat_date'\'' '"  ads_product_in_warehouse_daily.json
	
	
	
	
done

	
	
	
	
	
	
	
#参数说明
#dt=时间  host=mysql主机ip  password=mysql密码 preSql=插入数据前执行删除的sql参数（删除全量或者删除当天）