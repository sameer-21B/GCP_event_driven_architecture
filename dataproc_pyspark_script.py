import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, current_date, date_format, concat_ws
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--file_name", required=True)
    parser.add_argument("--project_id", required=True)
    args = parser.parse_args()

    bucket = args.bucket
    file_name = args.file_name
    project_id = args.project_id
    today_str = datetime.utcnow().strftime('%Y-%m-%d')

    spark = SparkSession.builder \
        .appName("CarSalesMedallionIngestion") \
        .getOrCreate()

    # 1. Enforce Exact Source Schema Definition
    schema = StructType([
        StructField("sr_no", StringType(), True),
        StructField("car_name", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("model", StringType(), True),
        StructField("vehicle_age", IntegerType(), True),
        StructField("km_driven", IntegerType(), True),
        StructField("seller_type", StringType(), True),
        StructField("fuel_type", StringType(), True),
        StructField("transmission_type", StringType(), True),
        StructField("mileage", DoubleType(), True),
        StructField("engine", IntegerType(), True),
        StructField("max_power", DoubleType(), True),
        StructField("seats", IntegerType(), True),
        StructField("selling_price", IntegerType(), True)
    ])

    input_path = f"gs://{bucket}/{file_name}"
    
    # Read raw CSV file
    df_raw = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(input_path)

    # 2. Add Row-Level Data Quality (DQ) Flags
    # Business Rules:
    # - sr_no, car_name, brand must not be NULL
    # - vehicle_age, km_driven, engine, seats, selling_price must be > 0
    # - seller_type in ('Individual', 'Dealer', 'Trustmark Dealer')
    # - fuel_type in ('Petrol', 'Diesel', 'CNG', 'LPG', 'Electric')
    # - transmission_type in ('Manual', 'Automatic')
    
    valid_sellers = ["Individual", "Dealer", "Trustmark Dealer"]
    valid_fuels = ["Petrol", "Diesel", "CNG", "LPG", "Electric"]
    valid_transmissions = ["Manual", "Automatic"]

    df_dq = df_raw.withColumn("quarantine_reasons", lit("")) \
        .withColumn("quarantine_reasons", 
            when(col("sr_no").isNull() | col("car_name").isNull() | col("brand").isNull(), 
                 concat_ws("; ", col("quarantine_reasons"), lit("Missing required identifier fields")))
            .otherwise(col("quarantine_reasons"))
        ) \
        .withColumn("quarantine_reasons", 
            when((col("vehicle_age") < 0) | (col("km_driven") < 0) | (col("engine") <= 0) | 
                 (col("seats") <= 0) | (col("selling_price") <= 0), 
                 concat_ws("; ", col("quarantine_reasons"), lit("Numeric fields out of domain bounds")))
            .otherwise(col("quarantine_reasons"))
        ) \
        .withColumn("quarantine_reasons", 
            when(~col("seller_type").isin(valid_sellers), 
                 concat_ws("; ", col("quarantine_reasons"), lit("Invalid seller_type")))
            .otherwise(col("quarantine_reasons"))
        ) \
        .withColumn("quarantine_reasons", 
            when(~col("fuel_type").isin(valid_fuels), 
                 concat_ws("; ", col("quarantine_reasons"), lit("Invalid fuel_type")))
            .otherwise(col("quarantine_reasons"))
        ) \
        .withColumn("quarantine_reasons", 
            when(~col("transmission_type").isin(valid_transmissions), 
                 concat_ws("; ", col("quarantine_reasons"), lit("Invalid transmission_type")))
            .otherwise(col("quarantine_reasons"))
        )

    # Filter Valid vs Quarantined Rows
    df_valid = df_dq.filter(col("quarantine_reasons") == "") \
        .drop("quarantine_reasons") \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source_file", lit(file_name))

    df_quarantined = df_dq.filter(col("quarantine_reasons") != "") \
        .withColumn("file_name", lit(file_name)) \
        .withColumn("_quarantined_at", current_timestamp())

    # 3. Write Valid Rows to BigQuery Bronze Table
    if df_valid.count() > 0:
        df_valid.write \
            .format("bigquery") \
            .option("table", f"{project_id}.analytics_bronze.car_sales_raw") \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()

    # 4. Write Quarantined Rows to Day-Wise GCS Folder
    # Storage Path: gs://<bucket>/quarantine/date=YYYY-MM-DD/
    if df_quarantined.count() > 0:
        quarantine_output_path = f"gs://{bucket}/quarantine/date={today_str}/"
        df_quarantined.write \
            .mode("append") \
            .option("header", "true") \
            .csv(quarantine_output_path)
            
        print(f"Quarantined {df_quarantined.count()} rows to {quarantine_output_path}")

    # 5. Archive Validated Source File into Day-Wise GCS Folder
    # Storage Path: gs://<bucket>/archive/date=YYYY-MM-DD/
    archive_output_path = f"gs://{bucket}/archive/date={today_str}/"
    df_raw.withColumn("file_name", lit(file_name)) \
        .write \
        .mode("append") \
        .option("header", "true") \
        .csv(archive_output_path)

    print("Pipeline Execution Completed.")

if __name__ == "__main__":
    main()