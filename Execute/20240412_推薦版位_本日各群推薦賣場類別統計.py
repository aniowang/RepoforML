#!/usr/bin/env python
# coding: utf-8

# In[22]:


import os,pandas as pd
os.chdir(r'C:\Users\018363\PythonCode')
import SenaoDB


# In[23]:


db=SenaoDB.DB()
conn=db._conn


# In[24]:


sql="""
--本日推薦賣場、依照分群
select distinct A.*
--,B.item_no
,C.brand,C.level1_name,C.level2_name,C.level3_name 
from  pstage.senao_ec_ad_marts A 
left join ( 
select mart_code,item_no from edwadmin.SENAO_EC_MARTS_ITEMS group by mart_code,item_no) B on A.mart_code=B.mart_code --賣場對料號
left join (
select p_no,brand,level1_name,level2_name,level3_name from pdata.tb_producttree_ec --料號分類
where level1_name is not null
group by p_no,brand,level1_name,level2_name,level3_name
) C on C.p_no=B.item_no
where 
--A.seq <30 and
A.main_cat='recommendforyou' 
and A.second_cat <>'home'
"""
df_detail=pd.read_sql(sql,conn)
df_detail


# In[25]:


sql="""
--本日推薦賣場、依照分群
with t as (
select distinct A.*
--,B.item_no
,C.brand,C.level1_name,C.level2_name,C.level3_name 
from  pstage.senao_ec_ad_marts A 
left join ( 
select mart_code,item_no from edwadmin.SENAO_EC_MARTS_ITEMS group by mart_code,item_no) B on A.mart_code=B.mart_code --賣場對料號
left join (
select p_no,brand,level1_name,level2_name,level3_name from pdata.tb_producttree_ec --料號分類
where level1_name is not null
group by p_no,brand,level1_name,level2_name,level3_name
) C on C.p_no=B.item_no
where 
--A.seq <30 and
A.main_cat='recommendforyou' 
and A.second_cat <>'home'
order by 2 desc
)
select second_cat,level1_name,level2_name,level3_name,count(distinct mart_code) as 賣場數 from t
group by second_cat,level1_name,level2_name,level3_name order by 1,5 desc
"""
df=pd.read_sql(sql,conn)
df


# In[26]:


os.chdir(r'C:\Users\018363\Project\20230616_GroupRecommandation\模型產出觀察\hdbscan分群推薦\hdbscan各分群推薦賣場')
df.to_excel(pd.Timestamp.today().strftime('%Y%m%d')+'本日分群推薦賣場類別統計.xlsx',index=False)


# In[27]:


df_detail.to_excel(pd.Timestamp.today().strftime('%Y%m%d')+'本日分群推薦賣場類別明細.xlsx',index=False)


# In[ ]:




