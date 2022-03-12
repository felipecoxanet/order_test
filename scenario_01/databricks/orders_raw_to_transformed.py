# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from datetime import date

# COMMAND ----------

# Setting configuration to optimize the delta tables
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")

spark = (
    SparkSession
    .builder
    .appName('OrderTest')
    .getOrCreate()
)
# COMMAND ----------

# ETL process get data from the current date
today = date.today()

s3_uri_scheme = 's3a'

bucket_source           = 'gn-datalake-development-raw'
key_source              = f'files/coxa_test/{today}/*'
path_source_table_delta = f'{s3_uri_scheme}://{bucket_source}/{key_source}'
print(path_source_table_delta)

bucket_target              = 'gn-datalake-development-transformed'
key_target_order_head      = 'files/coxa_test/order_head'
key_target_orders          = 'files/coxa_test/orders'
key_target_order_items     = 'files/coxa_test/order_items'
key_target_items           = 'files/coxa_test/items'
key_target_shopper         = 'files/coxa_test/shopper'
key_target_billing_address = 'files/coxa_test/billing_address'

path_target_table_order_head      = f'{s3_uri_scheme}://{bucket_target}/{key_target_order_head}'
path_target_table_orders          = f'{s3_uri_scheme}://{bucket_target}/{key_target_orders}'
path_target_table_order_items     = f'{s3_uri_scheme}://{bucket_target}/{key_target_order_items}'
path_target_table_items           = f'{s3_uri_scheme}://{bucket_target}/{key_target_items}'
path_target_table_shopper         = f'{s3_uri_scheme}://{bucket_target}/{key_target_shopper}'
path_target_table_billing_address = f'{s3_uri_scheme}://{bucket_target}/{key_target_billing_address}'

# COMMAND ----------

df = (
    spark
    .read
    .format("json")
    .option("multiline","true")
    .load(path_source_table_delta)
)

# COMMAND ----------

# If is nothing to process, exit the process.
count = df.count()

if count == 0:
    dbutils.notebook.exit(f"Count={count}. Nothing to process!")
else:
    print(f'Go ahead, processing {count} lines')

# COMMAND ----------

# Creating Order Head df
dfOrderHead = (
    df.selectExpr(
        "customerId.s   as customer_id", 
        "merchantId.s   as merchant_id", 
        "Order.M.Id.s   as order_id",
        "shopper.m.id.s as shopper_id",
        "Status.s       as status"
    )
)

dfOrderHead = (
    dfOrderHead
    .withColumn("date", current_date().cast("string"))
)

dfOrderHead.show()

# COMMAND ----------

# Creating Orders df
dfOrders = (
    df.selectExpr(
        "Order.m.id.s              as id", 
        "Order.m.reference.s       as reference", 
        "Order.M.orderAmount.n     as order_mount",
        "Order.m.description.s     as description",
        "Order.m.taxAmount.n       as tax_amount",
        "Order.m.items.l.m.id.s[0] as items_id",
        "Order.m.timestamp.s       as timestamp"        
    )
)

dfOrders = (
    dfOrders
    .withColumn("date", current_date().cast("string"))
)

dfOrders.show()

# COMMAND ----------

# Creating Order Items df
dfCount = df.select("Order.m.items.l.m.id.s").rdd
list = dfCount.map(lambda x: [i for i in x[0]]).reduce(lambda x: x)
print(list)

dfOrderItems = (
    df.selectExpr(
        "Order.m.items.l.m.id.s[0]        as id", 
        "Order.m.items.l.m.reference.s[0] as item_id",
        "Order.m.items.l.m.quantity.n[0]  as quantity"
    )
)

if len(list) > 0:
    count_in = 0
    for x in list:
        print(x)
        print(count_in)
        dfOrderItems1 = (
            df.selectExpr(
                f"Order.m.items.l.m.id.s[{count_in}]        as id", 
                f"Order.m.items.l.m.reference.s[{count_in}] as item_id",
                f"Order.m.items.l.m.quantity.n[{count_in}]  as quantity"
            )
        )
        dfOrderItems = (
            dfOrderItems.join(
                dfOrderItems1, 
                on=['id', 'item_id', 'quantity'], 
                how='outer'
            )
        )
        count_in = count_in + 1

dfOrderItems = (
    dfOrderItems
    .withColumn("date", current_date().cast("string"))
)

dfOrderItems.show()

# COMMAND ----------

# Creating Items df
dfItems = (
    df.selectExpr(
        "Order.m.items.l.m.reference.s[0] as id", 
        "Order.m.items.l.m.image.s[0]     as image",
        "Order.m.items.l.m.price.n[0]     as price",
        "Order.m.items.l.m.name.s[0]      as name",
        "Order.m.items.l.m.sku.s[0]       as sku",
        "Order.m.items.l.m.url.s[0]       as url"
    )
)

if len(list) > 0:
    count_in = 0
    for x in list:
        print(x)
        print(count_in)
        dfItems1 = (
            df.selectExpr(
                f"Order.m.items.l.m.reference.s[{count_in}] as id", 
                f"Order.m.items.l.m.image.s[{count_in}]     as image",
                f"Order.m.items.l.m.price.n[{count_in}]     as price",
                f"Order.m.items.l.m.name.s[{count_in}]      as name",
                f"Order.m.items.l.m.sku.s[{count_in}]       as sku",
                f"Order.m.items.l.m.url.s[{count_in}]       as url"
            )
        )
        dfItems = (
            dfItems.join(
                dfItems1, 
                on=['id', 'image', 'price', 'name', 'sku', 'url'], 
                how='outer'
            )
        )
        count_in = count_in + 1

dfItems = (
    dfItems
    .withColumn("date", current_date().cast("string"))
)

dfItems.show()

# COMMAND ----------

# Creating Shopper df
dfShopper = (
    df.selectExpr(
        "shopper.m.id.s                  as id", 
        "shopper.m.firstName.s           as firstName", 
        "shopper.M.lastName.s            as lastName",
        "shopper.m.phone.s               as phone",
        "shopper.m.billingAddress.m.id.s as billingAddress_id",
        "shopper.m.birthDate.s           as birthDate",
        "shopper.m.email.s               as email",
        "shopper.m.timestamp.s           as timestamp"  
    )
)

dfShopper = (
    dfShopper
    .withColumn("date", current_date().cast("string"))
)

dfShopper.show()

# COMMAND ----------

# Creating Billing Address df
dfBillingAddress = (
    df.selectExpr(
        "shopper.m.billingAddress.m.id.s as id", 
        "shopper.m.billingAddress.m.number.s as number", 
        "shopper.m.billingAddress.m.zipCode.s as zipCode", 
        "shopper.m.billingAddress.m.phoneNumber.s as phoneNumber", 
        "shopper.m.billingAddress.m.city.s as city", 
        "shopper.m.billingAddress.m.street.s as street", 
        "shopper.m.billingAddress.m.state.s as state", 
        "shopper.m.billingAddress.m.timestamp.s as timestamp", 
    )
)

dfBillingAddress = (
    dfBillingAddress
    .withColumn("date", current_date().cast("string"))
)

dfBillingAddress.show()

# COMMAND ----------

# Sqs used to merge process
sq_join_merge_order_head = "delta_table.customer_id     = df_table.customer_id \
                            and delta_table.merchant_id = df_table.merchant_id \
                            and delta_table.order_id    = df_table.order_id \
                            and delta_table.shopper_id  = df_table.shopper_id \
                            and delta_table.status      = df_table.status"

sq_join_merge_orders = "delta_table.id            = df_table.id \
                        and delta_table.reference = df_table.reference"

sq_join_merge_order_items = "delta_table.id         = df_table.id \
                            and delta_table.item_id = df_table.item_id"

sq_join_merge_items = "delta_table.id     = df_table.id \
                       and delta_table.sku = df_table.sku"

sq_join_merge_shopper = "delta_table.id                    = df_table.id \
                         and delta_table.billingAddress_id = df_table.billingAddress_id \
                         and delta_table.email             = df_table.email"

sq_join_merge_billing_address = "delta_table.id     = df_table.id \
                                 and delta_table.phoneNumber = df_table.phoneNumber"

# COMMAND ----------

# Write / merge
def write_merge_delta(path, df, sq_join_merge):
    deltaTable = DeltaTable.forPath(spark, path)
    deltaTable \
        .alias("delta_table") \
        .merge(df.alias("df_table"),
               sq_join_merge) \
        .whenNotMatchedInsertAll() \
        .execute()

# COMMAND ----------

#table_order_head 
write_merge_delta(path_target_table_order_head, dfOrderHead, sq_join_merge_order_head)
spark.sql(f"CREATE TABLE if not exists dfOrderHead USING delta LOCATION '{path_target_table_order_head}'")

#table_orders
write_merge_delta(path_target_table_orders, dfOrders, sq_join_merge_orders)
spark.sql(f"CREATE TABLE if not exists dfOrders USING delta LOCATION '{path_target_table_orders}'")

#table_order_items
write_merge_delta(path_target_table_order_items, dfOrderItems, sq_join_merge_order_items)
spark.sql(f"CREATE TABLE if not exists dfOrderItems USING delta LOCATION '{path_target_table_order_items}'")

#table_items 
write_merge_delta(path_target_table_items, dfItems, sq_join_merge_items)
spark.sql(f"CREATE TABLE if not exists dfItems USING delta LOCATION '{path_target_table_items}'")

#table_shopper
write_merge_delta(path_target_table_shopper, dfShopper, sq_join_merge_shopper)
spark.sql(f"CREATE TABLE if not exists dfShopper USING delta LOCATION '{path_target_table_shopper}'")

#table_billing_address 
write_merge_delta(path_target_table_billing_address, dfBillingAddress, sq_join_merge_billing_address)
spark.sql(f"CREATE TABLE if not exists dfBillingAddress USING delta LOCATION '{path_target_table_billing_address}'")
