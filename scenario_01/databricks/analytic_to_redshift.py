# Databricks notebook source
import boto3
import json
import uuid

from datetime import date
from pyspark.sql.functions import col

# COMMAND ----------

# Variables - Paths
s3_uri = 's3a'

bucket_databricks = f'gn-datalake-databricks-development'
bucket_analytic   = 'gn-datalake-development-analytic'
key_target        = 'files/coxa_test/report'

path_from_delta_table = f'{s3_uri}://{bucket_analytic}/{key_target}/'

tmp_dir = f'{s3_uri}://{bucket_databricks}/tmp_jobs/'

today = date.today()

# COMMAND ----------

# Secret use in db conection
secretarn = 'arn:aws:secretsmanager:us-east-1:590284024382:secret:prod/snowplow/coxa-1W1z7E'

secmgr = boto3.client('secretsmanager', region_name='us-east-1')
secret = secmgr.get_secret_value(SecretId=secretarn)
secretString = json.loads(secret["SecretString"])
user = secretString["username"]
password = secretString["password"]
host = secretString["host"]
port = secretString["port"]
database = f'getdevelopment'
dbClusterIdentifier = secretString["dbClusterIdentifier"]

connection_string = f'jdbc:postgresql://{host}:{port}/{database}?user={user}&password={password}'
redshit_role = "arn:aws:iam::590284024382:role/dwproducaoSpectrumRole"

# COMMAND ----------

df = (
    spark
    .read
    .format("delta")
    .load(path_from_delta_table)
    .where(col('date') == today)
)
print(f'Loanding from: {path_from_delta_table}date={today}')

# COMMAND ----------

# If is nothing to process, exit the process.
count = df.count()

if count == 0:
    dbutils.notebook.exit(f"Count={count}. Nothing to process!")
else:
    print(f'Go ahead, processing {count} lines')

# COMMAND ----------

# Removing the date partition column
df = df.drop(df.date)

# COMMAND ----------

# Generate a random string to create an unique name to tmp table
random_part = uuid.uuid4().hex
print(f'Random part: {random_part}')

table_name    = "final_report"
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

# COMMAND ----------

# Using Spark with Redshift connector to save dara in DB
# The write option creates the stage DB
df \
    .write \
    .format("com.databricks.spark.redshift") \
    .options(
        url=connection_string,
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
