from __future__ import annotations

import pendulum
import os
from docker.types import Mount

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator

# Get ENV variables defined in docker-compose
FTP_HOST = os.getenv("FTP_HOST", "ftp-server")
FTP_USER = os.getenv("FTP_USER", "user")
FTP_PASS = os.getenv("FTP_PASS", "password")

# Define the Docker image we built
SPARK_IMAGE = "pyspark-etl-app:latest"

# Define mounts needed by the Spark container
# Needs access to the input data directory
# Note: Source paths are from the *host* machine where docker-compose runs
#       Target paths are inside the *Spark container* launched by DockerOperator
# These might need adjustment based on your docker-compose volume mounts for the worker
data_mount = Mount(
    source="/path/on/host/to/etl-pyspark-airflow-project/data", # !! MUST match your host path !!
    target="/opt/spark/work-dir/data",
    type="bind",
    read_only=True
)
# Alternative if worker already mounts `data` volume: reference the volume name
# data_mount = Mount( source="etl-pyspark-airflow-project_data", # Assumes volume name convention
#                     target="/opt/spark/work-dir/data",
#                     type="volume", read_only=True)
# Check your volume names with `docker volume ls`


with DAG(
    dag_id="pyspark_api_local_ftp_etl",
    schedule=None, # Trigger manually for testing
    # schedule_interval="0 5 * * *", # Example: Run daily at 5 AM UTC
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pyspark", "etl", "docker", "ftp", "example"],
) as dag:
    start = DummyOperator(task_id="start")

    run_pyspark_etl_job = DockerOperator(
        task_id="run_pyspark_etl_job",
        image=SPARK_IMAGE,
        api_version="auto", # Use auto-detected Docker API version
        auto_remove=True,   # Remove container after execution
        # Command to execute inside the Spark container
        command="python /opt/spark/work-dir/etl_job.py",
        docker_url="unix://var/run/docker.sock", # Connect to Docker daemon on the host
        network_mode="etl-pyspark-airflow-project_default", # Connect to the docker-compose network (check name w/ `docker network ls`)
        # Pass environment variables needed by the script
        environment={
            "FTP_HOST": FTP_HOST,
            "FTP_USER": FTP_USER,
            "FTP_PASS": FTP_PASS,
            "SPARK_MASTER_URL": "local[*]", # Ensure Spark runs in local mode inside container
        },
        # Mounts required by the Spark job container (e.g., input data)
        # !! Adjust the 'source' path below to the ABSOLUTE path on your HOST machine !!
        mounts=[
             Mount(source="/Users/youruser/path/to/etl-pyspark-airflow-project/data", # Example macOS/Linux path
                   target="/opt/spark/work-dir/data", type="bind", read_only=True),
            # Mount(source="c:/Users/youruser/path/to/etl-pyspark-airflow-project/data", # Example Windows path
            #       target="/opt/spark/work-dir/data", type="bind", read_only=True)
        ],
        mount_tmp_dir=False, # Avoids Airflow mounting its own temp dir
        # Optional: Add resource limits if needed
        # mem_limit='2g',
        # cpu_period=100000,
        # cpu_quota=150000, # Equivalent to 1.5 CPUs
        tty=True, # Allocate pseudo-TTY useful for logging
    )

    end = DummyOperator(task_id="end")

    start >> run_pyspark_etl_job >> end