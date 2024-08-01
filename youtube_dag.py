from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import youtube_comments as ytc
from bigquery_automation import infer_schema_from_csv, create_dataset_and_table
from google.auth import default

@dag(
    schedule="@daily",  # The DAG runs daily
    start_date=datetime(2024, 7, 27),  # Start date for the DAG execution
    catchup=False,  # Do not backfill missed DAG runs
    default_args={
        'owner': 'Martin Riveros',
        'depends_on_past': False,  
        'email_on_failure': False,  
        'email_on_retry': False,  
        'retries': 1,  # Number of retry attempts on failure
    },
    description='Fetch YouTube comments and upload to GCS'  
)
def youtube_comments_to_gcs():
    
    # Get default credentials and project ID for Google Cloud
    _, project_id = default()

    # GCS bucket and file name where data will be stored
    bucket_name = 'youtube_fetch_data'
    destination_blob_name = 'comments.csv'

    # BigQuery destination settings
    dataset_id = "created_by_dag"
    location = "us-central1"
    table_id = f"comments_{{ds}}"  # Dynamic table ID with date substitution

    @task
    def fetch_and_upload_comments(bucket_name: str, destination_blob_name: str):
        """
        Fetches YouTube comments and uploads them to Google Cloud Storage (GCS).
        
        Parameters:
        bucket_name (str): The name of the GCS bucket where the file will be stored.
        destination_blob_name (str): The name of the file (blob) to be created in the GCS bucket.
        
        Returns:
        None
        """
        ytc.fetch_youtube_comments(project_id, bucket_name, destination_blob_name)

    # Define the task to wait for the file in GCS
    wait_for_file = GCSObjectExistenceSensor(
        task_id='wait_for_file',
        bucket=bucket_name,
        object=destination_blob_name,
        google_cloud_conn_id='google_cloud_default',
        timeout=600,  # Timeout in seconds for waiting
        poke_interval=30,  # Interval (in seconds) to check for the file
        mode='poke',  # The task will "poke" for the file at regular intervals
    )

    @task
    def process_file():
        """
        Processes the CSV file once it is detected in GCS, infers the schema, 
        and creates a BigQuery dataset and table based on the inferred schema.
        
        Returns:
        None
        """
        try:
            print("File detected in GCS. Starting processing.")      
            # Infer schema from the CSV file
            infered_schema = infer_schema_from_csv(bucket_name, destination_blob_name)
            # Create BigQuery dataset and table with the inferred schema
            create_dataset_and_table(project_id, dataset_id, table_id, infered_schema, location)
            
            print("File processing completed successfully.")
        except Exception as e:
            print(f"Error occurred while processing the file: {e}")
            raise
    
    # Define the BigQuery insert job
    # This could be done more specifically by using instance class GCSToBigQueryOperator
    upload_to_bq = BigQueryInsertJobOperator(
        task_id='upload_to_bq',
        configuration={
            "load": {
                "sourceUris": [f"gs://{bucket_name}/{destination_blob_name}"],  # The URI of the source file in GCS
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset_id,  # BigQuery dataset ID
                    "tableId": table_id  # BigQuery table ID
                },
                "sourceFormat": "CSV",  # The format of the source file
                "autodetect":"True",  # Automatically detect the schema
                "writeDisposition": "WRITE_TRUNCATE",  # Overwrite the table if it exists
                "max_bad_records":"25",  # Maximum number of bad records allowed before failing
            }
        },
        location='US',  # Location of the BigQuery job
    )

    # Define the task execution order (>>)
    fetch_and_upload_comments(
        bucket_name=bucket_name,
        destination_blob_name=destination_blob_name
    ) >> wait_for_file >> process_file() >> upload_to_bq

# Instantiate the DAG
dag_instance = youtube_comments_to_gcs()

