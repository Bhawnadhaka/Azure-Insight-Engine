# Databricks notebook source
spark

# COMMAND ----------

storage_account = "olistdatastorage2"
application_id = "2db0ce8a-458a-4c4e-9388-740adc5cec05"
directory_id = "c4276580-96af-40d7-9b13-3f59b507b966"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", 
               "<secret-client>")  ## i removed it
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/{directory_id}/oauth2/token")


# COMMAND ----------

# MAGIC %md
# MAGIC # Reading the Data

# COMMAND ----------

customer_df = spark.read.format("csv").option("header", "true").option("inferSchema",True).load(f"abfss://olistcontainer@olistdatastorage2.dfs.core.windows.net/bronze/olist_customers_dataset.csv")
display(customer_df)

# COMMAND ----------

geolocation_df = spark.read.format("csv").option("header", "true").option("inferSchema",True).load(f"abfss://olistcontainer@olistdatastorage2.dfs.core.windows.net/bronze/olist_geolocation_dataset.csv")
display(geolocation_df)

# COMMAND ----------

order_item_df = spark.read.format("csv").option("header", "true").option("inferSchema",True).load(f"abfss://olistcontainer@olistdatastorage2.dfs.core.windows.net/bronze/olist_order_items_dataset.csv")
display(order_item_df)

# COMMAND ----------

review_df = spark.read.format("csv").option("header", "true").option("inferSchema", True).load(f"abfss://olistcontainer@olistdatastorage2.dfs.core.windows.net/bronze/olist_order_reviews_dataset.csv")
display(review_df)

# COMMAND ----------

seller_df = spark.read.format("csv").option("header", "true").option("inferSchema",True).load(f"abfss://olistcontainer@olistdatastorage2.dfs.core.windows.net/bronze/olist_sellers_dataset.csv")
display(seller_df)

# COMMAND ----------

product_df = spark.read.format("csv").option("header", "true").option("inferSchema",True).load(f"abfss://olistcontainer@olistdatastorage2.dfs.core.windows.net/bronze/olist_products_dataset.csv")
display(product_df)

# COMMAND ----------

payment_df = spark.read.format("csv").option("header", "true").option("inferSchema",True).load(f"abfss://olistcontainer@olistdatastorage2.dfs.core.windows.net/bronze/olist_order_payments_dataset.csv")
display(payment_df)

# COMMAND ----------

orders_df = spark.read.format("csv").option("header", "true").option("inferSchema",True).load(f"abfss://olistcontainer@olistdatastorage2.dfs.core.windows.net/bronze/olist_orders_dataset.csv")
display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading data from pymongo

# COMMAND ----------

from pymongo import MongoClient

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "urdl3.h.filess.io"
database = "olistdata_drawntable"
port = "27018"
username = "olistdata_drawntable"
password = "580cfe3f4c42863fa9d72f4061c31ed7712303b3"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase

# COMMAND ----------

collection=mydatabase["product_categories"]

# COMMAND ----------

import pandas as pd
mongo_data=pd.DataFrame(list(collection.find()))
mongo_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning the data 

# COMMAND ----------

from pyspark.sql.functions import col,datediff,current_date,to_date,when

# COMMAND ----------

def clean_dataframe(df,name):
    print("cleaning"+name)
    return df.dropDuplicates().na.drop('all')
orders_df=clean_dataframe(orders_df,"orders")
display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## convert data columns

# COMMAND ----------

orders_df =orders_df.withColumn("orderpurchase_timestamp",to_date(col("order_purchase_timestamp")))\
  .withColumn("order_delivered_customer_date",to_date(col("order_delivered_customer_date")))\
    .withColumn("order_estimated_delivery_date",to_date(col("order_estimated_delivery_date")))
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## calculate delivery and timestamp delays

# COMMAND ----------

orders_df=orders_df.withColumn("actual_delivery_time",datediff("order_delivered_customer_date","orderpurchase_timestamp"))
orders_df=orders_df.withColumn("estimated_delivery_time",datediff("order_estimated_delivery_date","orderpurchase_timestamp"))
orders_df=orders_df.withColumn("delay Time",col("actual_delivery_time")-col("estimated_delivery_time"))
display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining

# COMMAND ----------

# Ensure all DataFrames are defined
# Assuming items_df, product_df, and sellers_df are defined somewhere above

order_customers_df = orders_df.join(
    customer_df,
    orders_df.customer_id == customer_df.customer_id,
    "left"
)

order_payments_df = order_customers_df.join(
    payment_df,
    order_customers_df.order_id == payment_df.order_id,
    "left"
)

orders_items_df = order_payments_df.join(
    order_item_df,
    "order_id",
    "left"
)

orders_items_product_df = orders_items_df.join(
    product_df,
    orders_items_df.product_id == product_df.product_id,
    "left"
)

final_df = orders_items_product_df.join(
    seller_df,
    orders_items_product_df.seller_id == seller_df.seller_id,
    "left"
)

# COMMAND ----------

display(final_df)

# COMMAND ----------

mongo_data.drop('_id',axis=1,inplace=True)
mongo_spark_df=spark.createDataFrame(mongo_data)
display(mongo_spark_df)


# COMMAND ----------

 final_df=final_df.join(mongo_spark_df,"product_category_name","left")

# COMMAND ----------

display(final_df)

# COMMAND ----------

def remove_duplicate_columns(df):
    columns=df.columns
    seen_columns=set()
    columns_to_drop=[]
    for column in columns:
        if column in seen_columns:
            columns_to_drop.append(column)
        else:
            seen_columns.add(column)
    df_cleaned=df.drop(*columns_to_drop)
    return df_cleaned
final_df=remove_duplicate_columns(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://olistcontainer@olistdatastorage2.dfs.core.windows.net/silver")

# COMMAND ----------

