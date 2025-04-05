import os
import requests
from ftplib import FTP
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# --- Configuration (Consider using environment variables or arguments) ---
API_URL = "https://jsonplaceholder.typicode.com/users"
LOCAL_CSV_PATH = "/opt/spark/work-dir/data/employee_data.csv" # Path *inside* the container
FTP_HOST = os.getenv("FTP_HOST", "ftp-server") # Use Docker service name
FTP_USER = os.getenv("FTP_USER", "user")
FTP_PASS = os.getenv("FTP_PASS", "password")
FTP_DIR = "/upload" # Directory inside the FTP server
OUTPUT_FILENAME = "processed_employee_data.parquet" # Using Parquet for efficiency
TEMP_OUTPUT_PATH = f"/tmp/{OUTPUT_FILENAME}" # Temp path *inside* the Spark container

def upload_to_ftp(host, user, password, source_local_path, target_remote_dir, filename):
    """Uploads a file to the FTP server."""
    try:
        with FTP(host) as ftp:
            ftp.login(user=user, passwd=password)
            print(f"FTP Logged in to {host}")

            # Create directory if it doesn't exist (optional)
            try:
                ftp.mkd(target_remote_dir)
                print(f"Created remote directory: {target_remote_dir}")
            except Exception as e:
                print(f"Remote directory {target_remote_dir} likely already exists or error: {e}")

            ftp.cwd(target_remote_dir) # Change to target directory
            print(f"Changed FTP directory to: {ftp.pwd()}")

            with open(source_local_path, 'rb') as f:
                print(f"Uploading {filename} to FTP...")
                ftp.storbinary(f'STOR {filename}', f)
            print(f"Successfully uploaded {filename} to {host}:{target_remote_dir}/")
    except Exception as e:
        print(f"Error connecting or uploading to FTP: {e}")
        raise

def main():
    print("Starting PySpark ETL Job...")

    # Initialize Spark Session
    # Running in local mode within the container
    spark = SparkSession.builder \
        .appName("APILocalFileToFTP") \
        .master("local[*]") \
        .getOrCreate()

    print("Spark Session created.")

    # --- Extract ---
    # 1. Read from API
    try:
        response = requests.get(API_URL)
        response.raise_for_status() # Raise exception for bad status codes
        users_data = response.json()
        # Convert JSON to Spark DataFrame
        # Define schema explicitly for robustness
        user_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("username", StringType(), True),
            StructField("email", StringType(), True),
            # Add other fields if needed, accessing nested address/company requires more complex schema/flattening
            StructField("phone", StringType(), True),
            StructField("website", StringType(), True),
        ])
        # Need to filter users_data to only include fields in schema if API returns more
        filtered_users_data = [{k: u.get(k) for k in [f.name for f in user_schema.fields]} for u in users_data]
        users_df = spark.createDataFrame(filtered_users_data, schema=user_schema)
        print(f"Successfully read {users_df.count()} records from API.")
        users_df.show(5, truncate=False)
    except Exception as e:
        print(f"Error fetching or processing API data: {e}")
        spark.stop()
        return

    # 2. Read from Local CSV
    try:
        employee_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("department", StringType(), True),
            StructField("salary", DoubleType(), True), # Use DoubleType for salary
        ])
        employee_df = spark.read.csv(LOCAL_CSV_PATH, header=True, schema=employee_schema)
        print(f"Successfully read {employee_df.count()} records from CSV.")
        employee_df.show(5)
    except Exception as e:
        print(f"Error reading CSV file at {LOCAL_CSV_PATH}: {e}")
        spark.stop()
        return

    # --- Transform ---
    print("Transforming data...")
    # Join datasets
    # Assuming API 'id' maps to CSV 'user_id'
    joined_df = users_df.join(employee_df, users_df["id"] == employee_df["user_id"], "inner")

    # Select relevant columns and add a processing timestamp
    final_df = joined_df.select(
        col("id").alias("employee_id"),
        col("name"),
        col("email"),
        col("department"),
        col("salary"),
        lit("processed").alias("status"), # Add a status column
        current_timestamp().alias("processing_ts") # Add timestamp
    )
    print(f"Transformation complete. Resulting schema:")
    final_df.printSchema()
    final_df.show(10, truncate=False)

    # --- Load ---
    print(f"Writing transformed data to temporary path: {TEMP_OUTPUT_PATH}")
    try:
        # Write DataFrame locally (inside the container) first - Parquet is often preferred
        final_df.write.mode("overwrite").parquet(TEMP_OUTPUT_PATH)
        print(f"Successfully wrote data locally to {TEMP_OUTPUT_PATH}")

        # Now upload the Parquet file(s) to FTP
        # Note: Parquet often writes a directory. We might need to zip it or upload individual files.
        # For simplicity here, let's assume it writes a single file or we upload the dir structure.
        # A robust solution would handle the directory structure (_SUCCESS file, part-*.parquet files).
        # Simplification: Let's target writing a single CSV instead for easier FTP upload in this example.

        TEMP_OUTPUT_PATH_CSV = "/tmp/processed_employee_data.csv"
        print(f"Writing as single CSV for easier FTP upload: {TEMP_OUTPUT_PATH_CSV}")
        final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(TEMP_OUTPUT_PATH_CSV + "_dir")

        # Find the actual CSV file created by Spark inside the directory
        import glob
        csv_files = glob.glob(os.path.join(TEMP_OUTPUT_PATH_CSV + "_dir", "*.csv"))
        if not csv_files:
            raise Exception("No CSV file found after Spark write.")
        source_csv_file = csv_files[0]
        target_csv_filename = "processed_employee_data.csv"

        print(f"Uploading {source_csv_file} to FTP as {target_csv_filename}...")
        upload_to_ftp(FTP_HOST, FTP_USER, FTP_PASS, source_csv_file, FTP_DIR, target_csv_filename)

    except Exception as e:
        print(f"Error during Load phase: {e}")
    finally:
        # Stop Spark Session
        print("Stopping Spark Session.")
        spark.stop()

if __name__ == "__main__":
    main()