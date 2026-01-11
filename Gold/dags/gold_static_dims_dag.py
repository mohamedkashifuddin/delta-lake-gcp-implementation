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
    Validates user confirmed the static dimensions load.
    This is a ONE-TIME operation that should only run once.
    """
    dag_params = context['params']
    
    confirm = dag_params.get('confirm_load', '').upper()
    
    if confirm != 'YES':
        raise ValueError(
            "❌ Static dimensions load NOT confirmed.\n\n"
            "ℹ️  This DAG loads 3 static dimensions (date, payment_method, status).\n"
            "   Run this ONCE before starting daily incremental loads.\n\n"
            "To proceed, trigger DAG with: {\"confirm_load\": \"YES\"}\n"
            "(Type YES in all caps)"
        )
    
    cluster_name = f"gold-static-dims-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    context['ti'].xcom_push(key='cluster_name', value=cluster_name)
    
    print("✅ Static dimensions load confirmed")
    print(f"   Cluster: {cluster_name}")
    print("   Loading: dim_date, dim_payment_method, dim_status")
    
    return {
        "cluster_name": cluster_name,
        "confirmed": True
    }

with models.DAG(
    dag_id="gold_static_dimensions_load",
    description="⚠️ ONE-TIME: Loads static dimensions (date, payment_method, status) - manual trigger, requires confirmation",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["gold", "delta-lake", "dimensions", "static", "manual", "one-time"],
    params={
        "confirm_load": Param(
            default="NO",
            type="string",
            title="Confirm Static Dimensions Load",
            description='Type "YES" (all caps) to confirm loading static dimensions. Run this ONCE before starting incremental DAGs.',
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
    
    load_dim_date = DataprocSubmitJobOperator(
        task_id="load_dim_date",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": "{{ ti.xcom_pull(task_ids='validate_confirmation', key='cluster_name') }}"
            },
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_BUCKET}/airflow/jobs/gold_dim_date.py"
            }
        }
    )
    
    load_dim_payment_methods = DataprocSubmitJobOperator(
        task_id="load_dim_payment_methods",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": "{{ ti.xcom_pull(task_ids='validate_confirmation', key='cluster_name') }}"
            },
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_BUCKET}/airflow/jobs/gold_dim_payment_methods.py"
            }
        }
    )
    
    load_dim_status = DataprocSubmitJobOperator(
        task_id="load_dim_status",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": "{{ ti.xcom_pull(task_ids='validate_confirmation', key='cluster_name') }}"
            },
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_BUCKET}/airflow/jobs/gold_dim_status.py"
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
    
    # Static dimensions can run in parallel (independent)
    validate_confirmation_task >> create_cluster
    create_cluster >> [load_dim_date, load_dim_payment_methods, load_dim_status]
    [load_dim_date, load_dim_payment_methods, load_dim_status] >> delete_cluster