#!/usr/bin/env python
# coding: utf-8

# In[1]:


import zipfile
with zipfile.ZipFile("task_data.json_(1).zip", 'r') as zip_ref:
    zip_ref.extractall()


# In[18]:


import pandas as pd
import json
import datetime
import psycopg2
import io

import os

pd.set_option('display.max_columns', None)


#DB Credentials
con = psycopg2.connect(
    host = "localhost",
    database ="Test",
    user = "user1",
    password = "password"
)


# In[3]:


from sqlalchemy import create_engine

# connection string: driver://username:password@server/database
conn = create_engine('postgresql://user1:password@localhost/Test')


# In[4]:


with open ("task_data.json") as f:
    data = json.load(f)
#data


# In[6]:


df = pd.json_normalize(data, max_level=4)
#df


# In[7]:


#https://www.youtube.com/watch?v=KZbU-edZ8_w
bnd_json = df.explode('bundes')
bnd = pd.json_normalize(json.loads(bnd_json.to_json(orient="records")))
#bnd


# In[8]:


bnd_json2 = bnd.explode('bundes.itemTypes.items')
bnd2 = pd.json_normalize(json.loads(bnd_json2.to_json(orient="records")))
#bnd2


# In[9]:


bnd2['createdAt'] = pd.to_datetime(bnd2['createdAt'])
bnd2['updatedAt'] = pd.to_datetime(bnd2['updatedAt'])


# In[10]:


##create custom dataframes

orders = bnd2[['createdAt','extStoreUUID','extTenantUUID','requestedFrom','status','takeaway','timezone','updatedAt','uuid','payment.deliveryFee','payment.discount','payment.price','payment.serviceCharge','payment.subtotalAmount','payment.totalAmount','payment.vatAmount','user.extUserUUID']
             ].drop_duplicates()

categories = bnd2[['bundes.basketUUID','bundes.category.name','bundes.category.uuid','bundes.description']
                 ].drop_duplicates()

category_items = bnd2[['bundes.basketUUID','bundes.itemTypes.items.itemUUID']
                 ].drop_duplicates()

items = bnd2[['bundes.itemTypes.name','bundes.itemTypes.uuid','bundes.itemTypes.items.discount','bundes.itemTypes.items.itemUUID','bundes.itemTypes.items.name','bundes.itemTypes.items.subtotalAmount','bundes.itemTypes.items.totalAmount','bundes.itemTypes.items.type','bundes.itemTypes.items.uuid','bundes.itemTypes.items.vatAmount','bundes.itemTypes.items.vatRateEatIn','bundes.itemTypes.items.vatRateTakeaway']
            ].drop_duplicates()

order_items = bnd2[['uuid','bundes.itemTypes.items.itemUUID','bundes.discount','bundes.kitchenStation.extTenantUUID','bundes.kitchenStation.name','bundes.kitchenStation.uuid','bundes.menuUUID','bundes.name','bundes.status','bundes.subtotalAmount','bundes.totalAmount','bundes.uuid','bundes.vatAmount','bundes.vatRateEatIn','bundes.vatRateTakeaway','bundes.kitchenStation','bundes.promotion.uuid','bundes.promotion.value']
            ].drop_duplicates()


# In[20]:


cur = con.cursor()


# In[12]:


##Create Tables
#Orders table
cur.execute("CREATE TABLE IF NOT EXISTS orders (  id SERIAL,  createdat TIMESTAMP,  extstoreuuid VARCHAR(255),  exttenantuuid VARCHAR(255),  requestedfrom VARCHAR(20),  status VARCHAR(20),  takeaway BOOLEAN,  timezone VARCHAR,  updatedat TIMESTAMP,  uuid VARCHAR(255),  payment_deliveryfee NUMERIC,  payment_discount NUMERIC,  payment_price NUMERIC,  payment_servicecharge NUMERIC,  payment_subtotalamount NUMERIC,  payment_totalamount NUMERIC,  payment_vatamount NUMERIC,  user_extuseruuid VARCHAR(255),  PRIMARY KEY (uuid));")

con.commit()


# In[13]:


#Categories table
cur.execute("CREATE TABLE IF NOT EXISTS categories (  id SERIAL,  basketuuid VARCHAR,  categoryname VARCHAR,  categoryuuid VARCHAR,  description VARCHAR,  PRIMARY KEY (basketuuid));")

con.commit()


# In[14]:


#items table
cur.execute("CREATE TABLE IF NOT EXISTS items (  id SERIAL,  typename VARCHAR(255),  typeuuid VARCHAR(100),  discount NUMERIC,  item_uuid VARCHAR(100),  name VARCHAR(255),  subtotal NUMERIC,  totalamount NUMERIC,  type VARCHAR(100),  uuid VARCHAR(100),  vatamount NUMERIC,  vatrateeatin NUMERIC,  vatratetakeaway NUMERIC,  PRIMARY KEY (id));")

con.commit()


# In[15]:


#order_items table
cur.execute("CREATE TABLE IF NOT EXISTS order_items (  id SERIAL,  order_uuid VARCHAR,  item_uuid VARCHAR,  discount NUMERIC,  exttenantuuid VARCHAR,  kitchenstationname VARCHAR,  kitchenstationuuid VARCHAR,  menuuuid VARCHAR,  name VARCHAR,  status VARCHAR,  subtotalamount NUMERIC,  totalamount NUMERIC,  uuid VARCHAR,  vatamount NUMERIC,  vatrateeatin NUMERIC,  vatratetakeaway NUMERIC,  kitchenstation VARCHAR,  promotionuuid VARCHAR,  promotionvalue NUMERIC,  PRIMARY KEY (id)   );")

con.commit()


# In[21]:


#category_items table
cur.execute("CREATE TABLE IF NOT EXISTS category_items (  id SERIAL,  category_uuid VARCHAR(100),  item_uuid VARCHAR(100),  PRIMARY KEY (id)           );")

con.commit()


# In[22]:


#db field names
def orders_col():
    return ['createdat','extstoreuuid','exttenantuuid','requestedfrom','status','takeaway','timezone','updatedat'            ,'uuid','payment_deliveryfee','payment_discount','payment_price','payment_servicecharge','payment_subtotalamount'            ,'payment_totalamount','payment_vatamount','user_extuseruuid']

def categories_col():
    return ['basketuuid','categoryname','categoryuuid','description']

def items_col ():
    return ['typename','typeuuid','discount','item_uuid','name','subtotal','totalamount','type','uuid','vatamount','vatrateeatin','vatratetakeaway']

def order_i_col ():
    return ['order_uuid','item_uuid','discount','exttenantuuid','kitchenstationname','kitchenstationuuid','menuuuid','name'            ,'status','subtotalamount','totalamount','uuid','vatamount','vatrateeatin','vatratetakeaway','kitchenstation','promotionuuid','promotionvalue']

def cat_i_col ():
    return ['category_uuid','item_uuid']


# In[409]:


#cur = con.cursor()
#output = io.StringIO()
#orders.to_csv(output, sep='\t', header=False, index=False)
#output.seek(0)
#contents = output.getvalue()
#cur.copy_from(output, 'orders',null="", columns=orders_col())
#c.commit()


# In[23]:


def write_data(dframe, table, cols):
    cur = con.cursor()
    output = io.StringIO()
    dframe.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, table, null="", columns=cols)
    con.commit()


# In[484]:


#write_data(orders,'orders',orders_col())


# In[25]:


try:
    write_data(orders,'orders',orders_col())
except:
    print("err writing orders table")
    con.rollback()
    
try:
    write_data(categories,'categories',categories_col())
except:
    print("err writing categories table")
    con.rollback()
    
try:
    write_data(category_items,'category_items',cat_i_col())
except:
    print("err writing category_items table")
    con.rollback()
    
try:
    write_data(items,'items',items_col())
except:
    print("err writing items table")
    con.rollback()
    
try:
    write_data(order_items,'order_items',order_i_col())
except:
    print("err writing order_items table")
    con.rollback()


# In[26]:


#categories rename and write
categories.rename({
    'bundes.basketUUID':'basketuuid',
    'bundes.category.name':'categoryname',
    'bundes.category.uuid':'categoryuuid',
    'bundes.description':'description'
}, axis='columns',inplace =True)


categories.to_sql('categories', conn, if_exists='append', index = False)


# In[27]:


#items rename and write
items.rename({
    'bundes.itemTypes.name':'typename',
    'bundes.itemTypes.uuid':'typeuuid',
    'bundes.itemTypes.items.discount':'discount',
    'bundes.itemTypes.items.itemUUID':'item_uuid',
    'bundes.itemTypes.items.name':'name',
    'bundes.itemTypes.items.subtotalAmount':'subtotal',
    'bundes.itemTypes.items.totalAmount':'totalamount',
    'bundes.itemTypes.items.type':'type',
    'bundes.itemTypes.items.uuid':'uuid',
    'bundes.itemTypes.items.vatAmount':'vatamount',
    'bundes.itemTypes.items.vatRateEatIn':'vatrateeatin',
    'bundes.itemTypes.items.vatRateTakeaway':'vatratetakeaway'  
}, axis='columns',inplace =True)

items.to_sql('items', conn, if_exists='append', index = False)


# In[28]:


#order_items rename and write
order_items.rename({
    'uuid':'order_uuid',
    'bundes.itemTypes.items.itemUUID':'item_uuid',  
    'bundes.discount':'discount',
    'bundes.kitchenStation.extTenantUUID':'exttenantuuid',
    'bundes.kitchenStation.name':'kitchenstationname',
    'bundes.kitchenStation.uuid':'kitchenstationuuid',
    'bundes.menuUUID':'menuuuid',
    'bundes.name':'name',
    'bundes.status':'status',
    'bundes.subtotalAmount':'subtotalamount',
    'bundes.totalAmount':'totalamount',
    'bundes.uuid':'uuid',
    'bundes.vatAmount':'vatamount',
    'bundes.vatRateEatIn':'vatrateeatin',
    'bundes.vatRateTakeaway':'vatratetakeaway',
    'bundes.kitchenStation':'kitchenstation',
    'bundes.promotion.uuid':'promotionuuid',
    'bundes.promotion.value':'promotionvalue'    
}, axis='columns',inplace =True)

order_items.to_sql('order_items', conn, if_exists='append', index = False)



# In[29]:


#close connection
cur.close()


# In[ ]:


#JOINS

#SELECT 
#*
#
#FROM 
#orders o
#join order_items oi on oi.order_uuid=o.uuid
#join items i on i.item_uuid = oi.item_uuid
#join category_items ci on ci.item_uuid = i.item_uuid
#join categories c on c.basketuuid=ci.category_uuid

