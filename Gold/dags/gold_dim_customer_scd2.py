from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
    cluster_name = f"gold-dim-customer-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    context['ti'].xcom_push(key='cluster_name', value=cluster_name)
    return cluster_name

with models.DAG(
    dag_id="gold_dim_customer_scd2",
    description="Gold dim_customer SCD Type 2 (tracks customer tier changes)",
    default_args=default_args,
    schedule_interval="0 4 * * *",  # 4 AM daily (after Silver at 3 AM)
    start_date=days_ago(1),
    catchup=False,
    tags=["gold", "delta-lake", "dimensions", "scd2", "customer"]
) as dag:
    
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver_completion",
        external_dag_id="silver_incremental_load",
        external_task_id="load_silver",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        poke_interval=60,
        timeout=3600,
        execution_delta=timedelta(hours=1)  # Silver runs at 3 AM, Gold at 4 AM
    )
    
    generate_cluster_name_task = PythonOperator(
        task_id='generate_cluster_name',
        python_callable=generate_cluster_name,
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
    
    load_dim_customer = DataprocSubmitJobOperator(
        task_id="load_dim_customer_scd2",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": "{{ ti.xcom_pull(task_ids='generate_cluster_name', key='cluster_name') }}"
            },
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_BUCKET}/airflow/jobs/gold_dim_customer_scd2.py"
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
    
    wait_for_silver >> generate_cluster_name_task >> create_cluster
    create_cluster >> load_dim_customer >> delete_cluster