import os
import functions_framework
from typing import Optional
from pydantic import BaseModel, ValidationError, Field
from google.cloud import dataproc_v1

# 1. Pydantic Schema for Strict File-Level Structural Validation
class CarSaleRecord(BaseModel):
    sr_no: str
    car_name: str
    brand: str
    model: str
    vehicle_age: int
    km_driven: int
    seller_type: str
    fuel_type: str
    transmission_type: str
    mileage: float
    engine: int
    max_power: float
    seats: int
    selling_price: int

@functions_framework.cloud_event
def process_gcs_file(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    
    # Ignore files already processed in archive/quarantine folders
    if file_name.startswith("archive/") or file_name.startswith("quarantine/"):
        return

    print(f"Processing triggered for file: gs://{bucket_name}/{file_name}")

    # Set GCP Configs via Environment Variables
    project_id = os.environ.get("GCP_PROJECT")
    region = os.environ.get("GCP_REGION", "us-central1")
    spark_script_path = os.environ.get("SPARK_SCRIPT_GCS_PATH") # e.g. gs://my-code-bucket/pyspark_etl.py
    sa_email = os.environ.get("SA_EMAIL")
    
    # Trigger Dataproc Serverless Batch Job
    dataproc_client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    batch_job = dataproc_v1.Batch(
        pyspark_batch=dataproc_v1.PySparkBatch(
            main_python_file_uri=spark_script_path,
            args=[
                f"--bucket={bucket_name}",
                f"--file_name={file_name}",
                f"--project_id={project_id}"
            ],
            # Includes PySpark-BigQuery Connector
            # jar_file_uris=["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"] 
        ),
        environment_config=dataproc_v1.EnvironmentConfig(
            execution_config=dataproc_v1.ExecutionConfig(
                service_account=sa_email
            )
        )
    )

    operation = dataproc_client.create_batch(
        parent=f"projects/{project_id}/locations/{region}",
        batch=batch_job,
        batch_id=f"car-sales-etl-{os.urandom(4).hex()}"
    )
    print(f"Dataproc Serverless PySpark Batch Job submitted successfully with SA: {sa_email}")