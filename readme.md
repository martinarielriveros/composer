# YouTube Comments to BigQuery Pipeline with Cloud Composer and Secret Manager

This project automates the process of fetching YouTube comments, storing them in Google Cloud Storage (GCS) as a CSV file, and uploading the data to BigQuery using Apache Airflow.

## Steps to Set Up and Run the Pipeline

### 1 - Set Up Google Cloud Environment
1. **Create a Google Cloud Project.**
2. **Enable the necessary APIs:**
   - YouTube Data API
   - Secret Manager API
   - Cloud Storage API
   - BigQuery API
   - Composer API
3. **Set up authentication with a service account that has appropriate permissions:**
   - BigQuery Data Editor
   - BigQuery Job User
   - Composer Administrator
   - Secret Manager Secret Accessor
   - Storage Admin

### 2 - Store YouTube API Key in Secret Manager
1. **Create a secret in Secret Manager** to store the YouTube API key.

### 3 - Set Up Google Cloud Storage
1. **Create a GCS bucket** where the fetched YouTube comments will be stored as a CSV file.

### 4 - Create a Python Script for Data Processing
Write a Python script that:
   - Fetches the YouTube API key from Secret Manager.
   - Uses the YouTube Data API to fetch comments from a specified video.
   - Stores the comments in a Pandas DataFrame.
   - Uploads the DataFrame as a CSV file to the GCS bucket.

### 5 - Set Up Airflow DAG
Define an Airflow DAG with the following tasks:
   - Fetch YouTube comments and upload them to GCS.
   - Use a GCS sensor to detect when the file is uploaded.
   - Process the CSV file, infer schema, and create a BigQuery table.
   - Upload the CSV data to the BigQuery table.

### 6 - Deploy and Run the Airflow DAG
1. **Deploy the DAG** to an Airflow environment in Google Cloud Composer.
2. **Schedule and monitor the DAG** to ensure it runs daily as configured.

### 7 - Test and Validate
1. **Test the entire pipeline end-to-end** to ensure data is correctly fetched, processed, and loaded into BigQuery.

---

