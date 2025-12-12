from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

PROJECT_ID = "grand-jigsaw-476820-t1"
REGION = "us-central1"
ZONE = "us-central1-b"
GCS_BUCKET = "gs://delta-lake-payment-gateway-476820"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

def generate_cluster_name(**context):
    cluster_name = f"bronze-ephemeral-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    context['ti'].xcom_push(key='cluster_name', value=cluster_name)
    return cluster_name

def read_watermark(**context):
    from google.cloud import bigquery
    
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
        SELECT last_processed_timestamp
        FROM `{PROJECT_ID}.bronze_dataset.job_control`
        WHERE job_name = 'bronze_incremental_load'
          AND status = 'SUCCESS'
        ORDER BY completed_at DESC
        LIMIT 1
    """
    
    try:
        result = client.query(query).result()
        row = next(result, None)
        watermark = row.last_processed_timestamp.strftime("%Y-%m-%d %H:%M:%S") if row and row.last_processed_timestamp else None
    except Exception as e:
        print(f"No previous watermark found: {e}")
        watermark = None
    
    print(f"Watermark: {watermark}")
    context['ti'].xcom_push(key='watermark', value=watermark or "NULL")
    context['ti'].xcom_push(key='batch_id', value=f"batch-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
    return watermark

with models.DAG(
    dag_id="bronze_incremental_load-14",
    description="Bronze layer incremental load with ephemeral Dataproc",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["bronze", "delta-lake", "incremental"]
) as dag:
    
    generate_cluster_name_task = PythonOperator(
        task_id='generate_cluster_name',
        python_callable=generate_cluster_name,
        provide_context=True
    )
    
    read_watermark_task = PythonOperator(
        task_id='read_watermark',
        python_callable=read_watermark,
        provide_context=True
    )
    
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name="{{ ti.xcom_pull(task_ids='generate_cluster_name', key='cluster_name') }}",
        cluster_config={
            "gce_cluster_config": {
                "zone_uri": ZONE,
                "internal_ip_only": False,
                "metadata": {
                    "hive-metastore-instance": f"{PROJECT_ID}:{REGION}:hive-metastore-mysql",
                    "kms-key-uri": f"projects/{PROJECT_ID}/locations/{REGION}/keyRings/dataproc-keys/cryptoKeys/hive-password-key",
                    "db-hive-password-uri": f"{GCS_BUCKET}/secrets/hive-password.encrypted"
                },
                "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"]
            },
            "master_config": {
                    "num_instances": 1,
                    "machine_type_uri": "n1-standard-2",
                    "disk_config": {"boot_disk_size_gb": 60}
                },
                "worker_config": {
                    "num_instances": 2,
                    "machine_type_uri": "n1-standard-2",
                    "disk_config": {"boot_disk_size_gb": 60}
                },
            "software_config": {
                "image_version": "2.2-debian12",
                "optional_components": ["JUPYTER", "DELTA"],
                "properties": {
                    "hive:hive.metastore.warehouse.dir": f"{GCS_BUCKET}/warehouse",
                    "dataproc:dataproc.allow.zero.workers": "true"
                }
            },
            "initialization_actions": [
                {
                    "executable_file": f"{GCS_BUCKET}/debug_init_scripts/cloud-sql-proxy-debug.sh"
                }
            ],
            "lifecycle_config": {
                "idle_delete_ttl": {"seconds": 600}
            }
        }
    )
    
    validate_bronze = DataprocSubmitJobOperator(
        task_id="validate_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": "{{ ti.xcom_pull(task_ids='generate_cluster_name', key='cluster_name') }}"
            },
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_BUCKET}/airflow/jobs/validate_bronze.py",
                "args": [
                    f"{GCS_BUCKET}/raw/20241202/day*.csv",
                    "{{ ti.xcom_pull(task_ids='read_watermark', key='watermark') }}",
                    "{{ ti.xcom_pull(task_ids='read_watermark', key='batch_id') }}"
                ]
            }
        }
    )
    
    load_bronze = DataprocSubmitJobOperator(
        task_id="load_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": "{{ ti.xcom_pull(task_ids='generate_cluster_name', key='cluster_name') }}"
            },
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_BUCKET}/airflow/jobs/load_bronze.py",
                "args": [
                    "{{ ti.xcom_pull(task_ids='read_watermark', key='batch_id') }}",
                    "bronze_incremental_load",
                    "incremental",
                    "0",
                    "0",
                    "{{ ts }}"
                ]
            }
        }
    )
    
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name="{{ ti.xcom_pull(task_ids='generate_cluster_name', key='cluster_name') }}",
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    generate_cluster_name_task >> read_watermark_task
    generate_cluster_name_task >> create_cluster
    [read_watermark_task, create_cluster] >> validate_bronze >> load_bronze >> delete_cluster