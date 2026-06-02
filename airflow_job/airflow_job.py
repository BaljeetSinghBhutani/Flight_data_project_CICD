from datetime import datetime, timedelta
import uuid
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date':  datetime(2026, 6, 2),
}


with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args = default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    env  = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow-projects-gsd")
    bq_project = Variable.get("bq_project", default_var="project-24871fad-51e3-458f-a9d")    
    bq_dataset = Variable.get("bq_dataset", default_var = f"flight_data_{env}")
    tables = Variable.get("tables", deserialize_json=True)
    
    # Extract the table names from 'tables' variable
    
    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]
    
    batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"
    
    # Task-1 File sensor for GCS
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket=gcs_bucket,
        object=f"airflow-project-1/source-{env}/flight_booking.csv",
        google_cloud_conn_id="google_cloud_default",
        timeout=300,
        poke_interval=30,
        mode="poke",
    )
    
    # Task-2 Submit Pyspark Job to Dataproc Serverless
    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{gcs_bucket}/airflow-project-1/spark-job/spark_transformation_job.py",
            "python_file_uris": [],
            "jar_file_uris": [],
            "args": [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
                
            ]
        },
        "runtime_config": {
            "version": "2.2",
        },
        "environment_config": {
            "execution_config": {
                "service_account": "1060624277041-compute@developer.gserviceaccount.com",
                "network_uri":"projects/project-24871fad-51e3-458f-a9d/global/networks/default" ,
                "subnetwork_uri" : "projects/project-24871fad-51e3-458f-a9d/regions/us-east1/subnetworks/default",
            }
        }  
        
        
    }
    
    
    
    
    
    pyspark_task=DataprocCreateBatchOperator(
        task_id = "run_spark_job_on_dataproc_serverless",
        batch=batch_details,
        batch_id=batch_id,
        project_id="project-24871fad-51e3-458f-a9d",
        region="us-east1",
        gcp_conn_id="google_cloud_default"
    )
    
    
    file_sensor >> pyspark_task