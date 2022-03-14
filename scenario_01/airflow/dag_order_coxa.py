from airflow import DAG
from airflow.config import default_args, notebook_task_params_orders_raw_to_transformed, notebook_task_params_transform_to_analytic, notebook_task_params_analytic_to_redshift
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

dag = DAG('order_coxa_test', default_args=default_args, schedule_interval="0 11,23 * * *", tags=['databrikcs', 'pipeline', 'coxa_order'])

raw_to_transformed = DatabricksSubmitRunOperator(
    task_id='raw_to_transformed',
    json = notebook_task_params_orders_raw_to_transformed,
    dag=dag
)

transform_to_analytic = DatabricksSubmitRunOperator(
    task_id='transform_to_analytic',
    json = notebook_task_params_transform_to_analytic,
    dag=dag
)

analytic_to_redshift = DatabricksSubmitRunOperator(
    task_id='analytic_to_redshift',
    json = notebook_task_params_analytic_to_redshift,
    dag=dag
)

raw_to_transformed >> transform_to_analytic >> analytic_to_redshift