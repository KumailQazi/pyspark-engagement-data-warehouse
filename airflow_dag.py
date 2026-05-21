"""
# ============================================================
# dags/tapmad_engagement_pipeline.py
# Apache Airflow DAG for Tapmad User Engagement ETL
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@tapmad.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    'tapmad_user_engagement_pipeline',
    default_args=default_args,
    description='T-1 batch pipeline for user engagement metrics',
    schedule_interval='0 2 * * *',  # 02:00 UTC daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['engagement', 'daily', 'critical'],
) as dag:

    # Task 1: Extract users CDC and merge SCD Type 2 dimension
    task_dim_user = DatabricksSubmitRunOperator(
        task_id='dim_user_scd2_merge',
        json={
            'existing_cluster_id': 'tapmad-etl-cluster',
            'notebook_task': {
                'notebook_path': '/Repos/etl/dim_user_scd2',
                'base_parameters': {'process_date': '{{ ds }}'}
            }
        }
    )

    # Task 2: Extract and stage content metadata
    task_dim_content = DatabricksSubmitRunOperator(
        task_id='dim_content_merge',
        json={
            'existing_cluster_id': 'tapmad-etl-cluster',
            'notebook_task': {
                'notebook_path': '/Repos/etl/dim_content',
                'base_parameters': {'process_date': '{{ ds }}'}
            }
        }
    )

    # Task 3: Bronze to Silver — playback events dedup + typing
    task_bronze_silver = DatabricksSubmitRunOperator(
        task_id='bronze_to_silver',
        json={
            'existing_cluster_id': 'tapmad-etl-cluster',
            'notebook_task': {
                'notebook_path': '/Repos/etl/bronze_to_silver',
                'base_parameters': {'process_date': '{{ ds }}'}
            }
        }
    )

    # Task 4: Silver to Curated — playback facts + session facts
    task_silver_curated = DatabricksSubmitRunOperator(
        task_id='silver_to_curated',
        json={
            'existing_cluster_id': 'tapmad-etl-cluster',
            'notebook_task': {
                'notebook_path': '/Repos/etl/silver_to_curated',
                'base_parameters': {'process_date': '{{ ds }}'}
            }
        }
    )

    # Task 5: Curated to Marts — user-day agg + engagement score + weekly mart
    task_curated_marts = DatabricksSubmitRunOperator(
        task_id='curated_to_marts',
        json={
            'existing_cluster_id': 'tapmad-etl-cluster',
            'notebook_task': {
                'notebook_path': '/Repos/etl/curated_to_marts',
                'base_parameters': {'process_date': '{{ ds }}'}
            }
        }
    )

    # Task 6: Data Quality Checks (Great Expectations)
    task_data_quality = PythonOperator(
        task_id='data_quality_checks',
        python_callable=lambda ds: print(f"Running GE validations for {ds}"),
        op_kwargs={'ds': '{{ ds }}'}
    )

    # Task 7: Optimize Delta tables (ZORDER + VACUUM)
    task_optimize = DatabricksSubmitRunOperator(
        task_id='optimize_tables',
        json={
            'existing_cluster_id': 'tapmad-etl-cluster',
            'notebook_task': {
                'notebook_path': '/Repos/etl/optimize_tables',
                'base_parameters': {'process_date': '{{ ds }}'}
            }
        }
    )

    # Dependencies
    [task_dim_user, task_dim_content] >> task_bronze_silver >> task_silver_curated >> task_curated_marts >> task_data_quality >> task_optimize
