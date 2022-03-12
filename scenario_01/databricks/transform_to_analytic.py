# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from datetime import date

# COMMAND ----------

# Setting configuration to optimize the delta tables
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")


spark = (
    SparkSession
    .builder
    .appName('OrderTest2')
    .getOrCreate()
)

# COMMAND ----------

# ETL process get data from the current date
today = date.today()

s3_uri_scheme = 's3a'

bucket_target = 'gn-datalake-development-analytic'
key_target    = 'files/coxa_test/report'

path_target = f'{s3_uri_scheme}://{bucket_target}/{key_target}'

# COMMAND ----------

# Report SQL
sq_report = f"select \
                dfOrderHead.customer_id, \
                dfOrderHead.order_id, \
                dfOrderHead.merchant_id, \
                dfOrderHead.status, \
                dfOrders.order_mount, \
                dfOrders.description, \
                dfOrders.tax_amount, \
                dfOrders.timestamp, \
                dfOrderItems.quantity, \
                dfItems.name as item_name, \
                dfShopper.email, \
                dfBillingAddress.phoneNumber, \
                dfOrderHead.date as date \
              from dfOrderHead \
                left join dfOrders on dfOrderHead.order_id = dfOrders.id and dfOrders.date = '{today}' \
                left join dfOrderItems on dfOrderItems.id = dfOrders.items_id and dfOrderItems.date = '{today}' \
                left join dfItems on dfItems.id = dfOrderItems.item_id  and dfItems.date = '{today}' \
                left join dfShopper on dfShopper.id = dfOrderHead.shopper_id and dfShopper.date = '{today}' \
                left join dfBillingAddress on dfBillingAddress.id = dfShopper.billingAddress_id and dfBillingAddress.date = '{today}'\
              where dfOrderHead.date = '{today}'"

print(f'sq_report: {sq_report}')

dfReport = spark.sql(sq_report)

# COMMAND ----------

# If is nothing to process, exit the process.
count = dfReport.count()

if count == 0:
    dbutils.notebook.exit(f"Count={count}. Nothing to process!")
else:
    print(f'Go ahead, processing {count} lines')


spark.sql(f"CREATE TABLE if not exists report USING delta LOCATION '{path_target}'")

# COMMAND ----------

# SQL used in merge
sq_join_merge = "delta_table.customer_id = df_table.customer_id \
                 and delta_table.order_id = df_table.order_id \
                 and delta_table.merchant_id = df_table.merchant_id \
                 and delta_table.status = df_table.status"

# COMMAND ----------

# Write / merge
deltaTable = DeltaTable.forPath(spark, path_target)
deltaTable \
    .alias("delta_table") \
    .merge(dfReport.alias("df_table"),
           sq_join_merge) \
    .whenNotMatchedInsertAll() \
    .execute()
