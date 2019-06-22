from impala.dbapi import connect

# conn = connect(host='impala.idc.jianke.com', port=21050)
# cur = conn.cursor()
#
# sql = "insert overwrite jkbd.temp_hz select 'hz1', null union  select 'hz2', null union select 'hz3', null "
# cur.execute(sql)




import os
import json
import time
import pandas as pd
from sqlalchemy import create_engine


start_t = time.time()
#conn_impala = create_engine('impala://172.21.57.127:21050', encoding="utf-8")
conn_impala = create_engine('impala://impala.idc.jianke.com:21050')
sql = "insert overwrite jkbd.temp_hz select 'hz1', null union select 'hz2', null union select 'hz3', null union select 'hz4', null;"

print('sql is ', sql)
df = pd.read_sql(sql, conn_impala)
end_t = time.time()
print('read data from impala cost time ', end_t-start_t)
print('df.shape is', df.shape)
print('df.head() is', df.head())

