#!/bin/bash

export PYTHON_EGG_CACHE=./myeggs
time1=$(date "+%Y-%m-%d %H:%M:%S")
echo $time1
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
