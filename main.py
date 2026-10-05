import os
import json
import logging
import functions_framework
import pandas as pd
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, ValidationError
from google.cloud import storage, bigquery

logging.basicConfig(level=logging.INFO)

# Environment Variables
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("BQ_DATASET_ID", "analytics_lakehouse")
TABLE_ID = os.getenv("BQ_TABLE_ID", "events_lakehouse")
DEAD_LETTER_BUCKET = os.getenv("DEAD_LETTER_BUCKET")

storage_client = storage.Client()
bq_client = bigquery.Client()

# Define Expected Data Model Schema for Validation
class EventRecord(BaseModel):
    event_id: str
    user_id: str
    event_type: str
    timestamp: str
    value: Optional[float] = Field(default=0.0)

def validate_dataframe(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Enforces data quality using Pydantic. Throws error if validation fails."""
    validated_records = []
    records = df.to_dict(orient="records")
    for row in records:
        # Fails hard if required fields are missing or typed incorrectly
        record = EventRecord(**row)
        validated_records.append(record.model_dump())
    return validated_records

def quarantine_file(bucket_name: str, file_name: str, reason: str):
    """Moves corrupted or invalid files to the Dead-Letter Storage Bucket."""
    logging.error(f"Quarantining file {file_name} from {bucket_name}. Reason: {reason}")
    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(file_name)
    
    dl_bucket = storage_client.bucket(DEAD_LETTER_BUCKET)
    destination_blob_name = f"quarantine_{file_name}"
    
    # Copy to Dead Letter
    source_bucket.copy_blob(source_blob, dl_bucket, destination_blob_name)
    # Delete original
    source_blob.delete()
    logging.info(f"File quarantined to gs://{DEAD_LETTER_BUCKET}/{destination_blob_name}")

def sync_bigquery_schema(df: pd.DataFrame, table_ref: bigquery.TableReference):
    """
    Handles Schema Evolution:
    Automatically adds new incoming DataFrame columns to the existing BigQuery table.
    """
    try:
        table = bq_client.get_table(table_ref)
        existing_fields = {field.name: field.field_type for field in table.schema}
        
        # Pandas to BigQuery Type Mapping
        type_mapping = {
            "object": "STRING",
            "int64": "INTEGER",
            "float64": "FLOAT",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP"
        }
        
        new_schema = list(table.schema)
        schema_changed = False

        for col_name, dtype in df.dtypes.items():
            if col_name not in existing_fields:
                bq_type = type_mapping.get(str(dtype), "STRING")
                new_field = bigquery.SchemaField(col_name, bq_type, mode="NULLABLE")
                new_schema.append(new_field)
                schema_changed = True
                logging.info(f"Schema Evolution Triggered: Adding new column '{col_name}' ({bq_type}) to BigQuery.")

        if schema_changed:
            table.schema = new_schema
            bq_client.update_table(table, ["schema"])
            logging.info("BigQuery schema successfully updated.")
            
    except Exception as e:
        # Table might not exist yet; auto-create strategy handles this
        logging.info(f"Table verification/creation needed: {str(e)}")

@functions_framework.cloud_event
def process_gcs_csv(cloud_event):
    """Main Eventarc Entry Point."""
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    if not file_name.endswith(".csv"):
        logging.info(f"Ignoring non-CSV file: {file_name}")
        return

    logging.info(f"Processing event for file: gs://{bucket_name}/{file_name}")

    try:
        # 1. Read CSV from GCS
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        csv_content = blob.download_as_bytes()
        df = pd.read_csv(pd.io.common.BytesIO(csv_content))

        if df.empty:
            raise ValueError("CSV file is empty")

        # 2. Data Quality Enforcement
        validated_data = validate_dataframe(df)
        clean_df = pd.DataFrame(validated_data)

        # 3. Handle Schema Evolution in BigQuery
        dataset_ref = bq_client.dataset(DATASET_ID)
        table_ref = dataset_ref.table(TABLE_ID)

        sync_bigquery_schema(clean_df, table_ref)

        # 4. Load Data to BigQuery
        job_config = bigquery.LoadJobConfig(
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True
        )

        load_job = bq_client.load_table_from_dataframe(
            clean_df, table_ref, job_config=job_config
        )
        load_job.result()  # Wait for completion

        logging.info(f"Successfully loaded {len(clean_df)} records to {DATASET_ID}.{TABLE_ID}")

    except ValidationError as ve:
        quarantine_file(bucket_name, file_name, f"Data Quality Check Failed: {ve}")
    except Exception as e:
        quarantine_file(bucket_name, file_name, f"Processing Error: {str(e)}")