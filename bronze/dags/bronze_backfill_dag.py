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
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

def validate_and_prepare_params(**context):
    """
    Validate user-provided date parameters and generate batch ID.
    Fails DAG early if parameters are invalid (before cluster creation).
    """
    # dag_run_conf = context.get('dag_run').conf or {}
    
    # start_date = dag_run_conf.get('start_date')
    # end_date = dag_run_conf.get('end_date')
    
    dag_params = context['params']
    
    start_date = dag_params['start_date']
    end_date = dag_params['end_date']
    
    # Validation 1: Required parameters
    if not start_date or not end_date:
        raise ValueError(
            "Missing required parameters. Please trigger with config:\n"
            '{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}'
        )
    
    # Validation 2: Date format
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError(
            f"Invalid date format. Use YYYY-MM-DD.\n"
            f"Received: start_date={start_date}, end_date={end_date}"
        )
    
    # Validation 3: Logical date range
    if start_dt > end_dt:
        raise ValueError(
            f"start_date ({start_date}) must be <= end_date ({end_date})"
        )
    
    # Validation 4: Reasonable range (prevent accidents)
    date_diff = (end_dt - start_dt).days
    if date_diff > 90:
        raise ValueError(
            f"Date range too large: {date_diff} days. Maximum allowed: 90 days.\n"
            "Split into smaller backfill jobs if needed."
        )
    
    # Generate batch ID
    batch_id = f"backfill-{start_date}-to-{end_date}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Generate cluster name
    cluster_name = f"bronze-backfill-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='start_date', value=start_date)
    context['ti'].xcom_push(key='end_date', value=end_date)
    context['ti'].xcom_push(key='batch_id', value=batch_id)
    context['ti'].xcom_push(key='cluster_name', value=cluster_name)
    context['ti'].xcom_push(key='date_range_days', value=date_diff + 1)
    
    print(f"✅ Validation passed:")
    print(f"   Date range: {start_date} to {end_date} ({date_diff + 1} days)")
    print(f"   Batch ID: {batch_id}")
    print(f"   Cluster: {cluster_name}")
    
    return {
        "start_date": start_date,
        "end_date": end_date,
        "batch_id": batch_id,
        "cluster_name": cluster_name
    }

with models.DAG(
    dag_id="bronze_backfill-3",
    description="Bronze layer backfill for specific date ranges (manual trigger, native operators)",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["bronze", "delta-lake", "backfill", "manual"],
    params={
        "start_date": Param(
            default=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            type="string",
            title="Backfill Start Date",
            description="Inclusive start date (YYYY-MM-DD) for data to process.",
            format="date"
        ),
        "end_date": Param(
            default=datetime.now().strftime('%Y-%m-%d'),
            type="string",
            title="Backfill End Date",
            description="Inclusive end date (YYYY-MM-DD) for data to process.",
            format="date"
        ),
    }

) as dag:
    
    validate_params = PythonOperator(
        task_id='validate_params',
        python_callable=validate_and_prepare_params,
        provide_context=True
    )
    
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name="{{ ti.xcom_pull(task_ids='validate_params', key='cluster_name') }}",
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
    
    run_backfill = DataprocSubmitJobOperator(
        task_id="run_backfill",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {
                "cluster_name": "{{ ti.xcom_pull(task_ids='validate_params', key='cluster_name') }}"
            },
            "pyspark_job": {
                "main_python_file_uri": f"{GCS_BUCKET}/airflow/jobs/bronze_backfill.py",
                "args": [
                    f"{GCS_BUCKET}/raw/20241202/day*.csv",
                    "{{ ti.xcom_pull(task_ids='validate_params', key='start_date') }}",
                    "{{ ti.xcom_pull(task_ids='validate_params', key='end_date') }}",
                    "{{ ti.xcom_pull(task_ids='validate_params', key='batch_id') }}"
                ]
            }
        }
    )
    
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name="{{ ti.xcom_pull(task_ids='validate_params', key='cluster_name') }}",
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    validate_params >> create_cluster >> run_backfill >> delete_cluster