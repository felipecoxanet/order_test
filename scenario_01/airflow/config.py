import os
from datetime import datetime
from dags.lib.slack_notification import slack_notification

environment = os.environ['ENVIRONMENT']
instance_pool_id = os.environ['POOLID']

default_args = {
    'owner': 'Coxa',
    'depends_on_past': False,
    'retries': 0,
    'start_date': datetime(2022, 3, 13),
    'on_failure_callback': slack_notification,
}

new_cluster = {
    'spark_version': '10.3.x-scala2.12',
    "instance_pool_id": instance_pool_id,
    'num_workers': 1,
    'aws_attributes': {
        'instance_profile_arn': f'arn:aws:iam::123456:instance-profile/{environment}-databricks-job-role'
    },
}

notebook_task_params_orders_raw_to_transformed = {
    'new_cluster': new_cluster,
    'notebook_task': {
        'notebook_path': '/Repos/databricks_repo/databricks/pipeline/orders_raw_to_transformed',
        'base_parameters': {
            'environment': environment
        }
     },
    'databricks_retry_limit': 0,
    'databricks_conn_id': 'databricks_default',
    'timeout_seconds': 600,
    "libraries": [
        {
            "pypi": {
                "package": "s3fs"
            }
        }
    ],
}

notebook_task_params_transform_to_analytic = {
    'new_cluster': new_cluster,
    'notebook_task': {
        'notebook_path': '/Repos/databricks_repo/databricks/pipeline/transform_to_analytic',
        'base_parameters': {
            'environment': environment
        }
    },
    'databricks_retry_limit': 0,
    'databricks_conn_id': 'databricks_default',
    'timeout_seconds': 600
}

notebook_task_params_analytic_to_redshift = {
    'new_cluster': new_cluster,
    'notebook_task': {
        'notebook_path': '/Repos/databricks_repo/databricks/pipeline/analytic_to_redshift',
        'base_parameters': {
            'environment': environment
        }
     },
    'databricks_retry_limit': 0,
    'databricks_conn_id': 'databricks_default',
    'timeout_seconds': 600,
    "libraries": [
        {
            "jar": f"s3://{environment}/libraries/spark-redshift_2.11-2.0.1.jar"
        }
    ],
}