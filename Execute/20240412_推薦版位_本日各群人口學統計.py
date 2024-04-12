#!/usr/bin/env python
# coding: utf-8

# In[20]:


import os,pandas as pd
os.chdir(r'C:\Users\018363\PythonCode')
import SenaoDB


# In[21]:


db=SenaoDB.DB()
conn=db._conn
engine=db._engine


# In[22]:


sql="""
--本日分群人口學
drop table if exists  group_demography;
create temporary table  group_demography as (
select A.member_id,A.age,A.sex,B."Group",B."CreateDate",
A.city,A.cht_pre_amt,A.cht_pre_name
from pdata.mbr_profile_mart A
left join (select member_id,"Group","CreateDate" from ptemp.anio_hdbscan_result) B on  B.member_id=A.member_id
);
"""
db.cursor.execute(sql)
db.connection.commit() 


# In[23]:


sql="""
select "CreateDate","Group",count(member_id) as "人數",
count(distinct case when age is not null then member_id else null end) "年齡有效的人數",
avg(age) "平均年齡",
count(distinct case when sex is not null then member_id else null end) "性別有效的人數",
count(distinct case when sex='F' then member_id else null end) "女性",
avg(case when sex='F' then age else null end) "女性平均年齡",
count(distinct case when sex='M' then member_id else null end) "男性",
avg(case when sex='M' then age else null end) "男性平均年齡",
count(distinct case when sex='Z' then member_id else null end) "未表明",
avg(case when sex='Z' then age else null end) "未表明性別平均年齡",
count(case when city in ('基隆市','新北市','台北市','桃園市','宜蘭縣','花蓮縣','台東縣','新竹市','新竹縣') then member_id else null end) as "北區業務",
count(case when city in ('南投縣','彰化縣','苗栗縣','台中市') then member_id else null end) as "中區業務",
count(case when city in ('雲林縣','嘉義縣','嘉義市','金門縣','台南市','屏東縣','澎湖縣','高雄市') then member_id else null end) as "南區業務",
count(distinct case when cht_pre_amt<800 then member_id else null end) "中低資費",
count(distinct case when cht_pre_amt>800 then member_id else null end) "高資費",
count(case when city in ('基隆市') then member_id else null end) as "基隆市",
count(case when city in ('新北市') then member_id else null end) as "新北市",
count(case when city in ('台北市') then member_id else null end) as "台北市",
count(case when city in ('桃園市') then member_id else null end) as "桃園市",
count(case when city in ('宜蘭縣') then member_id else null end) as "宜蘭縣",
count(case when city in ('花蓮縣') then member_id else null end) as "花蓮縣",
count(case when city in ('台東縣') then member_id else null end) as "台東縣",
count(case when city in ('新竹市') then member_id else null end) as "新竹市",
count(case when city in ('新竹縣') then member_id else null end) as "新竹縣",
count(case when city in ('南投縣') then member_id else null end) as "南投縣",
count(case when city in ('彰化縣') then member_id else null end) as "彰化縣",
count(case when city in ('苗栗縣') then member_id else null end) as "苗栗縣",
count(case when city in ('台中市') then member_id else null end) as "台中市",
count(case when city in ('雲林縣') then member_id else null end) as "雲林縣",
count(case when city in ('嘉義縣') then member_id else null end) as "嘉義縣",
count(case when city in ('嘉義市') then member_id else null end) as "嘉義市",
count(case when city in ('金門縣') then member_id else null end) as "金門縣",
count(case when city in ('台南市') then member_id else null end) as "台南市",
count(case when city in ('屏東縣') then member_id else null end) as "屏東縣",
count(case when city in ('澎湖縣') then member_id else null end) as "澎湖縣",
count(case when city in ('高雄市') then member_id else null end) as "高雄市"
from group_demography
where "Group" is not null
group by "CreateDate","Group";
"""
df=pd.read_sql(sql,conn)
print(df.shape)
os.chdir(r'C:\Users\018363\Project\20230616_GroupRecommandation\模型產出觀察\hdbscan分群推薦\hdbscan各分群人口學統計')
df.to_excel(pd.Timestamp.today().strftime('%Y%m%d')+'本日分群人口學統計.xlsx',index=False)


# In[24]:


sql="""
--本日分群人口學
drop table if exists  group_demography;
"""
db.cursor.execute(sql)
db.connection.commit() 


# In[ ]:




