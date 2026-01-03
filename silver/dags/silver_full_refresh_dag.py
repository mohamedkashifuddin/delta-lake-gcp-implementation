from airflow import models
from airflow.models.param import Param
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def validate_confirmation(**context):
    """
    Validates user confirmed the full refresh operation.
    Full refresh DELETES all silver data - requires explicit confirmation.
    """
    dag_params = context['params']
    
    confirm = dag_params.get('confirm_full_refresh', '').upper()
    
    if confirm != 'YES':
        raise ValueError(
            "❌ Full refresh NOT confirmed.\n\n"
            "⚠️  WARNING: This operation will DELETE all silver data and reload from Bronze.\n\n"
            "To proceed, trigger DAG with: {\"confirm_full_refresh\": \"YES\"}\n"
            "(Type YES in all caps)"
        )
    
    batch_id = f"full-refresh-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    cluster_name = f"silver-full-refresh-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    context['ti'].xcom_push(key='batch_id', value=batch_id)
    context['ti'].xcom_push(key='cluster_name', value=cluster_name)
    
    print("✅ Full refresh confirmed")
    print(f"   Batch ID: {batch_id}")
    print(f"   Cluster: {cluster_name}")
    print("⚠️  All silver data will be OVERWRITTEN")
    
    return {
        "batch_id": batch_id,
        "cluster_name": cluster_name,
        "confirmed": True
    }

with models.DAG(
    dag_id="silver_full_refresh",
    description="⚠️ DESTRUCTIVE: Reloads ALL silver data from Bronze (manual trigger, requires confirmation)",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["silver", "delta-lake", "full-refresh", "manual", "destructive"],
    params={
        "confirm_full_refresh": Param(
            default="NO",
            type="string",
            title="⚠️ Confirm Full Refresh",
            description='Type "YES" (all caps) to confirm you want to DELETE all silver data and reload from Bronze.',
            enum=["NO", "YES"]
        )
    }
) as dag:
    
    validate_confirmation_task = PythonOperator(
        task_id='validate_confirmation',
        python_callable=validate_confirmation,
        provide_context=True
    )
    
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name="{{ ti.xcom_pull(task_ids='validate_confirmation', key='cluster_name') }}",
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
                    "hive:hive.metastore.warehouse.dir": f"{GCS_BUCKET}/warehouse"
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
    
    run_full_refresh = DataprocSubmitJobOperator(
        task_id="run_full_refresh",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": "{{ ti.xcom_pull(task_ids='validate_confirmation', key='cluster_name') }}"
            },
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_BUCKET}/airflow/jobs/silver_full_refresh.py"
            }
        }
    )
    
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name="{{ ti.xcom_pull(task_ids='validate_confirmation', key='cluster_name') }}",
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    validate_confirmation_task >> create_cluster >> run_full_refresh >> delete_cluster