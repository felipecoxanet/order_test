import json
import logging
import os
from urllib.parse import unquote_plus
import boto3
logger = logging.getLogger()
logger.setLevel(logging.INFO)
timeout = 40

# Json order
# The lambda is triggered by S3 events and then starts a Glue job
def lambda_handler(event, context):
    logger.info("Order Json handler (env={}, event={})".format(os.environ, event))
    
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    
    # Treatment to remove + (plus) sign sent from S3 from files with space in name
    key = unquote_plus(key)
    _, tail = os.path.split(key)
    logger.info("S3 parameters (bucket={}, key={})".format(bucket_name, key))
    
    # Create a Secrets Manager client
    client = boto3.client("secretsmanager", region_name="us-east-1")
    get_secret_value_response = client.get_secret_value(SecretId="dev/getdevelop/coxa")
    secret = get_secret_value_response["SecretString"]
    secret = json.loads(secret)
    db_username = secret.get("username")
    db_password = secret.get("password")
    db_host = secret.get("host")
    db_port = secret.get("port")
    database = secret.get("database")
    url = (
        "jdbc:redshift://{db_host}:{db_port}"
        "/{database}?user={db_username}&password={db_password}".format(
            db_host=db_host,
            db_port=str(db_port),
            database=database,
            db_username=db_username,
            db_password=db_password,
        )
    )
    
    # Glue Job name.
    glue_job_name = "coxa_order_test2"

    glue_arguments = {
        "--key": key,
        "--bucket_name": bucket_name,
        "--file_name": tail,
        "--url": url,
    }
    
    # Start Glue Job
    logger.info(f"Starting glue job {glue_job_name}, {glue_arguments})")
    boto_client = boto3.client("glue", region_name="us-east-1")
    response = boto_client.start_job_run(
        JobName=glue_job_name,
        Arguments=glue_arguments,
        Timeout=timeout,
    )
    logger.info(f"Glue job started {response['JobRunId']}, {response})")
    
    return response