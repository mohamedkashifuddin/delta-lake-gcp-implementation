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
    "owner": "compliance-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def validate_deletion_request(**context):
    """
    Validates GDPR deletion request parameters.
    Requires explicit confirmation to prevent accidental deletions.
    
    NOTE: Airflow UI doesn't redirect after trigger. This is Airflow's default behavior.
    User must manually navigate back to DAG list after confirming.
    """
    dag_params = context['params']
    
    customer_id = dag_params.get('customer_id', '').strip()
    confirm = dag_params.get('confirm_deletion', '').upper()
    
    if not customer_id:
        raise ValueError(
            "❌ customer_id is required.\n\n"
            "To delete a customer's data, provide:\n"
            '{"customer_id": "USER_XXXX", "confirm_deletion": "YES"}\n\n'
            "Click 'Cancel' to abort."
        )
    
    if confirm != 'YES':
        raise ValueError(
            f"❌ GDPR deletion NOT confirmed.\n\n"
            f"Customer ID provided: {customer_id}\n"
            f"Confirmation status: {confirm}\n\n"
            "⚠️  WARNING: This will mark ALL customer transactions as deleted in Bronze.\n"
            "This action is logged for compliance audit.\n\n"
            'To proceed, change "confirm_deletion" to "YES" (all caps) and trigger again.\n'
            'Or click "Cancel" to abort this request.'
        )
    
    batch_id = f"gdpr-{customer_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    cluster_name = f"compliance-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    context['ti'].xcom_push(key='customer_id', value=customer_id)
    context['ti'].xcom_push(key='batch_id', value=batch_id)
    context['ti'].xcom_push(key='cluster_name', value=cluster_name)
    
    print("✅ GDPR deletion request validated")
    print(f"   Customer ID: {customer_id}")
    print(f"   Batch ID: {batch_id}")
    print(f"   Cluster: {cluster_name}")
    print("⚠️  All customer transactions will be marked as deleted")
    
    return {
        "customer_id": customer_id,
        "batch_id": batch_id,
        "cluster_name": cluster_name,
        "confirmed": True
    }

with models.DAG(
    dag_id="bronze_compliance_deletion-4",
    description="⚠️ GDPR Compliance: Mark customer data as deleted (manual trigger, requires confirmation)",
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=["bronze", "compliance", "gdpr", "manual", "deletion"],
    params={
        "customer_id": Param(
            default="",
            type="string",
            title="Customer ID",
            description='Customer ID to delete (e.g., "USER_0123")',
            minLength=1
        ),
        "confirm_deletion": Param(
            default="NO",
            type="string",
            title="⚠️ Confirm Deletion",
            description='Type "YES" (all caps) to confirm GDPR deletion request.',
            enum=["NO", "YES"]
        )
    }
) as dag:
    
    validate_request = PythonOperator(
        task_id='validate_deletion_request',
        python_callable=validate_deletion_request,
        provide_context=True
    )
    
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name="{{ ti.xcom_pull(task_ids='validate_deletion_request', key='cluster_name') }}",
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
    
    run_deletion = DataprocSubmitJobOperator(
        task_id="mark_customer_deleted_bronze",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": "{{ ti.xcom_pull(task_ids='validate_deletion_request', key='cluster_name') }}"
            },
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_BUCKET}/airflow/jobs/bronze_mark_deleted_by_customer.py",
                "args": [
                    "--customer_id={{ ti.xcom_pull(task_ids='validate_deletion_request', key='customer_id') }}"
                ]
            }
        }
    )
    
    propagate_to_silver = DataprocSubmitJobOperator(
        task_id="propagate_deletion_to_silver",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": "{{ ti.xcom_pull(task_ids='validate_deletion_request', key='cluster_name') }}"
            },
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_BUCKET}/airflow/jobs/silver_propagate_deletes.py",
                "args": [
                    "--customer_id={{ ti.xcom_pull(task_ids='validate_deletion_request', key='customer_id') }}"
                ]
            }
        }
    )
    
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name="{{ ti.xcom_pull(task_ids='validate_deletion_request', key='cluster_name') }}",
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    validate_request >> create_cluster >> run_deletion >> propagate_to_silver >> delete_cluster