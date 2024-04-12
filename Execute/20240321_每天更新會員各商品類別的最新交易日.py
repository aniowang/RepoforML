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


txn_date=(pd.Timestamp.today()-pd.DateOffset(days=7)).strftime('%Y%m%d')
print(txn_date)


# In[ ]:


#先備份昨日結果
sql=f"""
--備份
drop table if exists ptest.anio_txn_tag_backup;
select * into ptest.anio_txn_tag_backup from  ptest.anio_txn_tag;
"""
db.cursor.execute(sql)
db.connection.commit()

print("備份昨日結果完畢")
# In[ ]:
try:
    db=SenaoDB.DB()
except:
    pass

#撈取前7日交易紀錄
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

print('前7日交易紀錄撈取完畢')

try:
    db=SenaoDB.DB()
except:
    pass
    
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
print('執行新增欄位')
for _ in tqdm(new_columns):
    Add_NewColumns(_)

print('執行新增會員')    
#執行新增會員
Insert_by_Column()

print('執行更新最新交易日by欄位')
#執行更新最新交易日by欄位

for _ in tqdm([i for i in pivoted.columns[1:-1]]):
    Update_by_Column(_)    

db.close()

# In[ ]:

#try:
#    db=SenaoDB.DB()
#    #移除暫存
#    sql=f"""
#    truncate ptest.anio_txn_tag_updatetmp;
#    """
#    db.cursor.execute(sql)
#    db.connection.commit()
#    db.close()
#    print('暫存表ptest.anio_txn_tag_updatetmp移除')
#except:
#    pass


try:
    db=SenaoDB.DB()
except:
    pass

print('開始更新ptest.insider_txn_tag')

# 20240322 追加更新
# 20240401 空值改Null
sql=f"""
drop table if exists ptest.insider_txn_tag;
select B.uuid ::varchar(20),A.member_id ::varchar(10),
coalesce(A."二手回收類_二手類",NULL) ::varchar(8) "TXN_USED_GOODS_RECYCLE_USED_CATEGORY",
coalesce(A."保健‧保養_保健食品",NULL) ::varchar(8) "TXN_HEALTH_NUTRITION_NUTRITION_PRODUCTS",
coalesce(A."保健‧保養_健康工具",NULL) ::varchar(8) "TXN_HEALTH_NUTRITION_WELLNESS_TOOLS",
coalesce(A."保健‧保養_女性保養",NULL) ::varchar(8) "TXN_HEALTH_NUTRITION_FEMALE_CARE",
coalesce(A."保健‧保養_寵物健康",NULL) ::varchar(8) "TXN_HEALTH_NUTRITION_PET_HEALTH",
coalesce(A."保健‧保養_清潔保養",NULL) ::varchar(8) "TXN_HEALTH_NUTRITION_CLEANING_CARE",
coalesce(A."包膜服務_包膜服務",NULL) ::varchar(8) "TXN_WRAP_SERVICE_WRAP_SERVICE",
coalesce(A."家電商品類_冰洗空調/冰箱",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_REFRI_WASHER",
coalesce(A."家電商品類_冰洗空調/大家電周邊",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_AIRCON_ACCESSORIES",
coalesce(A."家電商品類_冰洗空調/洗衣機",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_WASHING_MACHINE",
coalesce(A."家電商品類_冰洗空調/空調",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_AIRCON",
coalesce(A."家電商品類_家電/其他",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_OTHERS",
coalesce(A."家電商品類_小家電/健康美容",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_HEALTH_BEAUTY",
coalesce(A."家電商品類_小家電/其他家電",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_OTHER_APPLIANCES",
coalesce(A."家電商品類_小家電/季節家電",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_SEASONAL_APPLIANCES",
coalesce(A."家電商品類_小家電/廚房家電",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_KITCHEN_APPLIANCES",
coalesce(A."家電商品類_小家電/清淨除塵",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_AIR_PURIFIERS",
coalesce(A."家電商品類_小家電/照明電工",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_LIGHTING",
coalesce(A."家電商品類_廚具衛浴",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_KITCHEN_BATH",
coalesce(A."家電商品類_影音家電/多媒體撥放器",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_MEDIA_PLAYERS",
coalesce(A."家電商品類_影音家電/家庭音響",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_HOME_THEATER",
coalesce(A."家電商品類_影音家電/投影機",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_PROJECTORS",
coalesce(A."家電商品類_影音家電/電視",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_TVS",
coalesce(A."家電商品類_影音家電/電視周邊",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_TV_ACCESSORIES",
coalesce(A."家電商品類_影音家電/音響配件",NULL) ::varchar(8) "TXN_HOME_APPLIANCES_SPEAKER_ACCESSORIES",
coalesce(A."平板商品類_平板/平板電腦",NULL) ::varchar(8) "TXN_TABLETS_TABLETS",
coalesce(A."應用周邊_精選配件",NULL) ::varchar(8) "TXN_MOBILE_ACCESSORIES_SELECTED",
coalesce(A."應用週邊商品類_APPLE 配件",NULL) ::varchar(8) "TXN_MOBILE_ACCESSORIES_APPLE",
coalesce(A."應用週邊商品類_充電用配件商品",NULL) ::varchar(8) "TXN_MOBILE_ACCESSORIES_CHARGING",
coalesce(A."應用週邊商品類_其它商品",NULL) ::varchar(8) "TXN_MOBILE_ACCESSORIES_OTHER",
coalesce(A."應用週邊商品類_專用保護類商品",NULL) ::varchar(8) "TXN_MOBILE_ACCESSORIES_PROTECTION_CASES",
coalesce(A."應用週邊商品類_智慧穿戴式裝置",NULL) ::varchar(8) "TXN_MOBILE_ACCESSORIES_WEARABLES",
coalesce(A."應用週邊商品類_有線耳機類商品",NULL) ::varchar(8) "TXN_MOBILE_ACCESSORIES_WIRED_HEADPHONES",
coalesce(A."應用週邊商品類_藍芽及無線類商品",NULL) ::varchar(8) "TXN_MOBILE_ACCESSORIES_WIRELESS",
coalesce(A."應用週邊商品類_行動電源",NULL) ::varchar(8) "TXN_MOBILE_ACCESSORIES_POWERBANKS",
coalesce(A."應用週邊商品類_記憶卡",NULL) ::varchar(8) "TXN_MOBILE_ACCESSORIES_MEMORY_CARDS",
coalesce(A."日用‧設計‧戶外_休閒旅行",NULL) ::varchar(8) "TXN_LIVING_OUTDOOR_TRAVEL",
coalesce(A."日用‧設計‧戶外_戶外露營",NULL) ::varchar(8) "TXN_LIVING_OUTDOOR_CAMPING",
coalesce(A."日用‧設計‧戶外_日用百貨",NULL) ::varchar(8) "TXN_LIVING_OUTDOOR_HOUSEHOLD",
coalesce(A."日用‧設計‧戶外_生活設計",NULL) ::varchar(8) "TXN_LIVING_OUTDOOR_DESIGN",
coalesce(A."維修類_0022 維修手機及料件",NULL) ::varchar(8) "TXN_REPAIR_MOBILE_REPAIR_PARTS",
coalesce(A."資訊商品類_筆電-非Apple",NULL) ::varchar(8) "TXN_IT_LAPTOPS_NON_APPLE",
coalesce(A."資訊商品類_網通/其他",NULL) ::varchar(8) "TXN_IT_NETWORKING_OTHERS",
coalesce(A."資訊商品類_網通監控/無線網卡",NULL) ::varchar(8) "TXN_IT_WIRELESS_CARDS",
coalesce(A."資訊商品類_網通監控/網通商品",NULL) ::varchar(8) "TXN_IT_NETWORKING_PRODUCTS",
coalesce(A."資訊商品類_車用商品",NULL) ::varchar(8) "TXN_IT_CAR_ACCESSORIES",
coalesce(A."資訊商品類_遊戲/遊戲周邊",NULL) ::varchar(8) "TXN_IT_GAMING_ACCESSORIES",
coalesce(A."資訊商品類_遊戲電玩",NULL) ::varchar(8) "TXN_IT_GAMING_CONSOLES",
coalesce(A."資訊商品類_電腦/APPLE CPU",NULL) ::varchar(8) "TXN_IT_APPLE_CPU",
coalesce(A."資訊商品類_電腦/印表機",NULL) ::varchar(8) "TXN_IT_PRINTERS",
coalesce(A."資訊商品類_電腦/筆記型電腦",NULL) ::varchar(8) "TXN_IT_LAPTOPS",
coalesce(A."資訊商品類_電腦/電腦週邊",NULL) ::varchar(8) "TXN_IT_COMPUTER_ACCESSORIES",
coalesce(A."通訊商品類_行動電話",NULL) ::varchar(8) "TXN_MOBILE_PHONES",
coalesce(A."食品‧票券_人氣票券",NULL) ::varchar(8) "TXN_FOOD_POPULAR_TICKETS",
coalesce(A."食品‧票券_休閒零食點心",NULL) ::varchar(8) "TXN_FOOD_SNACKS",
coalesce(A."食品‧票券_冷凍．冷藏．生鮮",NULL) ::varchar(8) "TXN_FOOD_FROZEN_FRESH",
coalesce(A."食品‧票券_民生食材‧南北貨",NULL) ::varchar(8) "TXN_FOOD_GROCERIES",
coalesce(A."食品‧票券_酒類商品",NULL) ::varchar(8) "TXN_FOOD_ALCOHOL",
coalesce(A."食品‧票券_電子票券",NULL) ::varchar(8) "TXN_FOOD_EVOUCHERS",
coalesce(A."食品‧票券_飲品‧沖調",NULL) ::varchar(8) "TXN_FOOD_BEVERAGES",
A.update_date ::timestamp "txn_tag_update_date"
into ptest.insider_txn_tag
from ptest.anio_txn_tag A
left join (select * from ptest.insider_member_map) B on B.member_id=A.member_id;

grant select on ptest.insider_txn_tag to edwadmin ;
"""
db.cursor.execute(sql)
db.connection.commit()

print('ptest.insider_txn_tag更新完畢')

#關閉連線
db.close()

#追加更新資料至正式區20240410
try:
    db=SenaoDB.DB()
except:
    pass

print('開始更新ptemp.insider_txn_tag')

sql=f"""

truncate ptemp.insider_txn_tag;

insert into	ptemp.insider_txn_tag(
uuid ,
member_id ,
"txn_used_goods_recycle_used_category" ,
"txn_health_nutrition_nutrition_products" ,
"txn_health_nutrition_wellness_tools" ,
"txn_health_nutrition_female_care" ,
"txn_health_nutrition_pet_health" ,
"txn_health_nutrition_cleaning_care" ,
"txn_wrap_service_wrap_service" ,
"txn_home_appliances_refri_washer" ,
"txn_home_appliances_aircon_accessories" ,
"txn_home_appliances_washing_machine" ,
"txn_home_appliances_aircon" ,
"txn_home_appliances_others" ,
"txn_home_appliances_health_beauty" ,
"txn_home_appliances_other_appliances" ,
"txn_home_appliances_seasonal_appliances" ,
"txn_home_appliances_kitchen_appliances" ,
"txn_home_appliances_air_purifiers" ,
"txn_home_appliances_lighting" ,
"txn_home_appliances_kitchen_bath" ,
"txn_home_appliances_media_players" ,
"txn_home_appliances_home_theater" ,
"txn_home_appliances_projectors" ,
"txn_home_appliances_tvs" ,
"txn_home_appliances_tv_accessories" ,
"txn_home_appliances_speaker_accessories" ,
"txn_tablets_tablets" ,
"txn_mobile_accessories_selected" ,
"txn_mobile_accessories_apple" ,
"txn_mobile_accessories_charging" ,
"txn_mobile_accessories_other" ,
"txn_mobile_accessories_protection_cases" ,
"txn_mobile_accessories_wearables" ,
"txn_mobile_accessories_wired_headphones" ,
"txn_mobile_accessories_wireless" ,
"txn_mobile_accessories_powerbanks" ,
"txn_mobile_accessories_memory_cards" ,
"txn_living_outdoor_travel" ,
"txn_living_outdoor_camping" ,
"txn_living_outdoor_household" ,
"txn_living_outdoor_design" ,
"txn_repair_mobile_repair_parts" ,
"txn_it_laptops_non_apple" ,
"txn_it_networking_others" ,
"txn_it_wireless_cards" ,
"txn_it_networking_products" ,
"txn_it_car_accessories" ,
"txn_it_gaming_accessories" ,
"txn_it_gaming_consoles" ,
"txn_it_apple_cpu" ,
"txn_it_printers" ,
"txn_it_laptops" ,
"txn_it_computer_accessories" ,
"txn_mobile_phones" ,
"txn_food_popular_tickets" ,
"txn_food_snacks" ,
"txn_food_frozen_fresh" ,
"txn_food_groceries" ,
"txn_food_alcohol" ,
"txn_food_evouchers" ,
"txn_food_beverages" ,
txn_tag_update_date
)
select uuid ,
member_id ,
"TXN_USED_GOODS_RECYCLE_USED_CATEGORY" ,
"TXN_HEALTH_NUTRITION_NUTRITION_PRODUCTS" ,
"TXN_HEALTH_NUTRITION_WELLNESS_TOOLS" ,
"TXN_HEALTH_NUTRITION_FEMALE_CARE" ,
"TXN_HEALTH_NUTRITION_PET_HEALTH" ,
"TXN_HEALTH_NUTRITION_CLEANING_CARE" ,
"TXN_WRAP_SERVICE_WRAP_SERVICE" ,
"TXN_HOME_APPLIANCES_REFRI_WASHER" ,
"TXN_HOME_APPLIANCES_AIRCON_ACCESSORIES" ,
"TXN_HOME_APPLIANCES_WASHING_MACHINE" ,
"TXN_HOME_APPLIANCES_AIRCON" ,
"TXN_HOME_APPLIANCES_OTHERS" ,
"TXN_HOME_APPLIANCES_HEALTH_BEAUTY" ,
"TXN_HOME_APPLIANCES_OTHER_APPLIANCES" ,
"TXN_HOME_APPLIANCES_SEASONAL_APPLIANCES" ,
"TXN_HOME_APPLIANCES_KITCHEN_APPLIANCES" ,
"TXN_HOME_APPLIANCES_AIR_PURIFIERS" ,
"TXN_HOME_APPLIANCES_LIGHTING" ,
"TXN_HOME_APPLIANCES_KITCHEN_BATH" ,
"TXN_HOME_APPLIANCES_MEDIA_PLAYERS" ,
"TXN_HOME_APPLIANCES_HOME_THEATER" ,
"TXN_HOME_APPLIANCES_PROJECTORS" ,
"TXN_HOME_APPLIANCES_TVS" ,
"TXN_HOME_APPLIANCES_TV_ACCESSORIES" ,
"TXN_HOME_APPLIANCES_SPEAKER_ACCESSORIES" ,
"TXN_TABLETS_TABLETS" ,
"TXN_MOBILE_ACCESSORIES_SELECTED" ,
"TXN_MOBILE_ACCESSORIES_APPLE" ,
"TXN_MOBILE_ACCESSORIES_CHARGING" ,
"TXN_MOBILE_ACCESSORIES_OTHER" ,
"TXN_MOBILE_ACCESSORIES_PROTECTION_CASES" ,
"TXN_MOBILE_ACCESSORIES_WEARABLES" ,
"TXN_MOBILE_ACCESSORIES_WIRED_HEADPHONES" ,
"TXN_MOBILE_ACCESSORIES_WIRELESS" ,
"TXN_MOBILE_ACCESSORIES_POWERBANKS" ,
"TXN_MOBILE_ACCESSORIES_MEMORY_CARDS" ,
"TXN_LIVING_OUTDOOR_TRAVEL" ,
"TXN_LIVING_OUTDOOR_CAMPING" ,
"TXN_LIVING_OUTDOOR_HOUSEHOLD" ,
"TXN_LIVING_OUTDOOR_DESIGN" ,
"TXN_REPAIR_MOBILE_REPAIR_PARTS" ,
"TXN_IT_LAPTOPS_NON_APPLE" ,
"TXN_IT_NETWORKING_OTHERS" ,
"TXN_IT_WIRELESS_CARDS" ,
"TXN_IT_NETWORKING_PRODUCTS" ,
"TXN_IT_CAR_ACCESSORIES" ,
"TXN_IT_GAMING_ACCESSORIES" ,
"TXN_IT_GAMING_CONSOLES" ,
"TXN_IT_APPLE_CPU" ,
"TXN_IT_PRINTERS" ,
"TXN_IT_LAPTOPS" ,
"TXN_IT_COMPUTER_ACCESSORIES" ,
"TXN_MOBILE_PHONES" ,
"TXN_FOOD_POPULAR_TICKETS" ,
"TXN_FOOD_SNACKS" ,
"TXN_FOOD_FROZEN_FRESH" ,
"TXN_FOOD_GROCERIES" ,
"TXN_FOOD_ALCOHOL" ,
"TXN_FOOD_EVOUCHERS" ,
"TXN_FOOD_BEVERAGES" ,
txn_tag_update_date from ptest.insider_txn_tag
;
"""
db.cursor.execute(sql)
db.connection.commit()
#關閉連線
db.close()
print('更新ptemp.insider_txn_tag完畢')