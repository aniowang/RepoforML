#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd,SenaoDB
from tqdm import tqdm


# In[ ]:


db=SenaoDB.DB()
conn=db._conn
engine=db._engine


# In[ ]:


txn_date=(pd.Timestamp.today()-pd.DateOffset(days=1)).strftime('%Y%m%d')
txn_date


# In[ ]:


#先備份昨日結果
sql=f"""
--備份
drop table if exists ptest.anio_txn_tag_backup;
select * into ptest.anio_txn_tag_backup from  ptest.anio_txn_tag;
"""
db.cursor.execute(sql)
db.connection.commit()


# In[ ]:


#撈取昨日交易紀錄
sql=f"""
drop table if exists ptest.anio_txn_tag_updatetmp;
SELECT 
    member_id,
    off_p_level1, 
    off_p_level2,
    off_p_level1||'_'||off_p_level2 "product_cat",
    TO_CHAR(order_date,'yyyymmdd')::varchar(10) "Date"
into ptest.anio_txn_tag_updatetmp
FROM (
    SELECT 
        member_id,
        off_p_level1,
        off_p_level2,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY member_id, off_p_level1, off_p_level2 ORDER BY order_date DESC) AS rn
    FROM 
    (select member_id,off_p_level1,off_p_level2,order_date from pdata.txn_allchannel_detail 
     WHERE member_id IS NOT NULL and cancel_flag='N' and p_no_bz='N' and  off_p_level1 in (
        '二手回收類','日用‧設計‧戶外','包膜服務','平板商品類','保健‧保養','食品‧票券','家電商品類',
        '通訊商品類','資訊商品類','維修類','應用周邊','應用週邊商品類') 
        and order_date >'{txn_date}'
     )
) t
WHERE rn = 1 ;
"""
db.cursor.execute(sql)
db.connection.commit()

sql="""
select member_id,product_cat,"Date" from ptest.anio_txn_tag_updatetmp
"""
df=pd.read_sql(sql,conn)
print('交易筆數：',df.shape[0])

# 執行Pivot Table
pivoted = df.pivot_table(index='member_id', columns='product_cat', values='Date'
                         ,aggfunc=lambda x : str(int(x))[:10]
                        ).fillna('').reset_index()

pivoted['update_date']=pd.Timestamp.now().strftime('%Y%m%d')
print('預計更新最新交易日的會員數：',pivoted.shape[0])

#上傳預計更新的表單
pivoted.to_sql('anio_txn_tag_daily_update', engine, schema='ptest', if_exists='replace', index=False)


# In[ ]:


#新增會員
def Insert_by_Column():
    sql=f"""   
    INSERT INTO ptest.anio_txn_tag(member_id)
    SELECT B.member_id
    FROM ptest.anio_txn_tag_daily_update B
    left JOIN ptest.anio_txn_tag A ON A.member_id = B.member_id 
    WHERE A.member_id is null ;    
    """
    db.cursor.execute(sql)
    db.connection.commit()
    return()


# In[ ]:


#若是有新分類(欄位)，也要更新
sql="""
select * from  ptest.anio_txn_tag limit 1
"""
original_columns=pd.read_sql(sql,conn)

#需要新增的欄位
new_columns=[i  for i in pivoted.columns[1:-1].to_list()   if  i not in original_columns.columns[1:-1].to_list()]
print('新增的欄位：',new_columns)

#新增欄位
def Add_NewColumns(newcolumn):
    sql=f"""
    alter table ptest.anio_txn_tag
    ADD COLUMN   {newcolumn} text;
    """
    db.cursor.execute(sql)
    db.connection.commit()
    return()


# In[ ]:


#更新最新交易日by欄位
def Update_by_Column(column):
    sql=f"""
    UPDATE ptest.anio_txn_tag A
    SET A."{column}" = B."{column}",update_date='{pd.Timestamp.today().strftime('%Y%m%d')}'
    FROM ptest.anio_txn_tag_daily_update B
    WHERE A.member_id = B.member_id and B."{column}" <>''
    """
    db.cursor.execute(sql)
    db.connection.commit()
    return()


# In[ ]:


try:
    db=SenaoDB.DB()
except:
    pass

#執行新增欄位
for _ in tqdm(new_columns):
    Add_NewColumns(_)
    
#執行新增會員
Insert_by_Column()

#執行更新最新交易日by欄位
for _ in tqdm([i for i in pivoted.columns[1:-1]]):
    Update_by_Column(_)    


# In[ ]:


#移除暫存
sql=f"""
drop table if exists ptest.anio_txn_tag_updatetmp;
"""
db.cursor.execute(sql)
db.connection.commit()


# In[ ]:


#關閉連線
db.close()

