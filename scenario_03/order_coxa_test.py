# This job is triggered by a lambda function to load json order files
import logging
import uuid
import uuid
import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from datetime import date

# Variables
aws_iam_role = "arn:aws:iam::123456:role/hopeHope"
db_url = 'jdbc:redshift://<host>:<port>/<database_name>?user=felipe_coxa&password=123456'

# Logger
MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables - Paths
today = date.today()

redshit_role = "arn:aws:iam::123456:role/hopeHope"

s3_uri = 's3a'

test_version = 'coxa_test3'
bucket_source           = 'gn-datalake-development-transformed'
key_source              = f'files/coxa_test/{today}/*'
path_source_table_delta = f'{s3_uri}://{bucket_source}/{key_source}'
logger.info(path_source_table_delta)

bucket_target              = 'gn-datalake-development-transformed'
key_target_order_head      = f'files/{test_version}/order_head'
key_target_orders          = f'files/{test_version}/orders'
key_target_order_items     = f'files/{test_version}/order_items'
key_target_items           = f'files/{test_version}/items'
key_target_shopper         = f'files/{test_version}/shopper'
key_target_billing_address = f'files/{test_version}/billing_address'

path_target_table_order_head      = f'{s3_uri}://{bucket_target}/{key_target_order_head}'
path_target_table_orders          = f'{s3_uri}://{bucket_target}/{key_target_orders}'
path_target_table_order_items     = f'{s3_uri}://{bucket_target}/{key_target_order_items}'
path_target_table_items           = f'{s3_uri}://{bucket_target}/{key_target_items}'
path_target_table_shopper         = f'{s3_uri}://{bucket_target}/{key_target_shopper}'
path_target_table_billing_address = f'{s3_uri}://{bucket_target}/{key_target_billing_address}'

bucket_target_analytic = 'gn-datalake-development-analytic'
key_target_analytic    = f'files/{test_version}/report'
path_target_analytic   = f'{s3_uri}://{bucket_target_analytic}/{key_target_analytic}'

bucket_databricks = 'gn-datalake-databricks-development'
tmp_dir           = f'{s3_uri}://{bucket_databricks}/tmp_jobs/'

# Spark
sc = SparkContext()
spark = SparkSession \
    .builder \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()
    
# Add the Jar file in Hadoop session
sc.addPyFile("dbfs:/usr/lib/spark/jars/spark-redshift_2.11-2.0.1.jar")
sc.addPyFile("dbfs:/usr/lib/spark/jars/aws_java_sdk_1_12_134.jar")

session = boto3.session.Session(aws_access_key_id='<aws_access_key_id>', aws_secret_access_key='<aws_secret_access_key>', region_name='us-east-1')
sts_connection = session.client('sts')
response = sts_connection.assume_role(RoleArn='arn:aws:iam::123456:role/AWSGlueServiceRoleDefault', RoleSessionName='AWSGlueServiceRoleDefault',DurationSeconds=3600)
credentials = response['Credentials']

spark = SparkSession \
    .builder \
    .config("org.apache.hadoop:hadoop-aws:3.3.1", "com.amazonaws:aws-java-sdk:1.12.134") \
    .config("spark.jars.packages") \
    .config("com.databricks.spark.redshift") \
    .config("spark.hadoop.fs.s3a.endpoint","s3.us-east-1.amazonaws.com") \
    .config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider') \
    .config('fs.s3a.access.key', credentials['AccessKeyId']) \
    .config('fs.s3a.secret.key', credentials['SecretAccessKey']) \
    .config('fs.s3a.session.token', credentials['SessionToken']) \
    .config('log4j.logger.org.apache.hadoop.fs.s3a=DEBUG') \
    .config('log4j.logger.com.amazonaws.request=DEBUG') \
    .config('log4j.logger.com.amazonaws.thirdparty.apache.http=DEBUG') \
    .getOrCreate()

df = (
    spark
    .read
    .format("json")
    .option("multiline","true")
    .load(path_source_table_delta)
)

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

# Write / merge
#table_order_head 
dfOrderHead \
    .write \
    .format("parquet") \
    .partitionBy("date")\
    .mode("append") \
    .save(path_target_table_order_head)
    
#table_orders
dfOrders \
    .write \
    .format("parquet") \
    .partitionBy("date")\
    .mode("append") \
    .save(path_target_table_orders)

#table_order_items
dfOrderItems \
    .write \
    .format("parquet") \
    .partitionBy("date")\
    .mode("append") \
    .save(path_target_table_order_items)

#table_items 
dfItems \
    .write \
    .format("parquet") \
    .partitionBy("date")\
    .mode("append") \
    .save(path_target_table_items)

#table_shopper
dfShopper \
    .write \
    .format("parquet") \
    .partitionBy("date")\
    .mode("append") \
    .save(path_target_table_shopper)

#table_billing_address 
dfBillingAddress \
    .write \
    .format("parquet") \
    .partitionBy("date")\
    .mode("append") \
    .save(path_target_table_billing_address)

logger.info("end of raw to transformed process")

#-------------------------------------------------------
# transform_to_analytic

logger.info("Starting transformed to analytic process")

dfOrderHead.createOrReplaceTempView('dfOrderHead')
dfOrders.createOrReplaceTempView('dfOrders')
dfOrderItems.createOrReplaceTempView('dfOrderItems')
dfItems.createOrReplaceTempView('dfItems')
dfShopper.createOrReplaceTempView('dfShopper')
dfBillingAddress.createOrReplaceTempView('dfBillingAddress')

# Report SQL
sq_report = f"select distinct\
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

logger.info(f'sq_report: {sq_report}')

dfReport = spark.sql(sq_report)
dfReport.show()

dfReport \
    .write \
    .format("parquet") \
    .partitionBy("date")\
    .mode("append") \
    .save(path_target_analytic)

logger.info("Ended transformed to analytic process")

#-------------------------------------------------------
# analytic_to_redshift

logger.info("Starting analytic to redshift process")

dfRedshift = dfReport.drop(dfReport.date)
dfRedshift.show()

# Generate a random string to create an unique name to tmp table
random_part = uuid.uuid4().hex
print(f'Random part: {random_part}')

table_name    = "final_report3"
target_schema = "temp"
stage_schema  = "stage"
target_table  = target_schema + "." + table_name
stage_table   = stage_schema + "." + table_name + "_" + random_part
print(f'Tmp table name: {stage_table}')
print(f'Final table name: {target_table}')

table_layout = '\
    customer_id VARCHAR(256)   ENCODE RAW \
	,order_id VARCHAR(256)   ENCODE RAW \
	,merchant_id VARCHAR(256)   ENCODE RAW \
	,status VARCHAR(256)   ENCODE lzo \
	,order_mount VARCHAR(256)   ENCODE lzo \
	,description VARCHAR(256)   ENCODE lzo \
	,tax_amount VARCHAR(256)   ENCODE lzo \
	,"timestamp" VARCHAR(256)   ENCODE lzo \
	,quantity VARCHAR(256)   ENCODE lzo \
	,item_name VARCHAR(256)   ENCODE lzo \
	,email VARCHAR(256)   ENCODE lzo \
	,phonenumber VARCHAR(256)   ENCODE lzo'

post_query = """
    begin;
        --creating schema and table if not exist
        create schema if not exists {target_schema};
        create table if not exists {target_table}
        ({table_layout})
        diststyle key
        distkey (customer_id)
        sortkey (
            customer_id
            , order_id
            , merchant_id
        );
        alter table {target_table} owner to felipe_coxa;
        
        --merge sql
        lock {target_table};
        delete from {target_table} using {stage_table}
        where {stage_table}.customer_id = {target_table}.customer_id
          and {stage_table}.order_id    = {target_table}.order_id
          and {stage_table}.merchant_id = {target_table}.merchant_id
          and {stage_table}.status      = {target_table}.status;
        insert into {target_table} (select * from {stage_table});
        drop table {stage_table};

    end;""".format(
    stage_table        = stage_table, 
    target_table       = target_table, 
    table_layout       = table_layout,
    target_schema      = target_schema
)
print(f'Post query: {post_query}')

# Using Spark with Redshift connector to save dara in DB
# The write option creates the stage DB
dfRedshift \
    .write \
    .format("com.databricks.spark.redshift") \
    .options(
        url=db_url,
        dbtable=stage_table,
        tempdir=tmp_dir,
        postactions=post_query,
        diststyle="KEY",
        distkey="customer_id",
        sortkeyspec="SORTKEY(customer_id, order_id, merchant_id)",
        aws_iam_role=redshit_role,
    ) \
    .mode("overwrite") \
    .save()

logger.info("Ended analytic to redshift process")
