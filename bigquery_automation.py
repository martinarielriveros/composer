from google.cloud.bigquery import Client, Table, SchemaField
from google.cloud import storage
import pandas as pd


def infer_schema_from_csv(bucket_name, file_name):
  """Infers a BigQuery schema from the first row of a CSV file.

  Args:
    bucket_name: The name of the Google Cloud Storage bucket.
    file_name: The name of the CSV file in the bucket.

  Returns:
    A list of bigquery.SchemaField objects representing the inferred schema.
  """

  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(file_name)

  # Download the first line of the CSV
  with blob.open('rt') as f:
    header = f.readline().strip().split(',')

  # Read the first row of data
  with blob.open('rt') as f:
    next(f)  # Skip header
    first_row = f.readline().strip().split(',')

  # Infer data types using pandas
  df = pd.DataFrame([first_row], columns=header)
  data_types = df.dtypes.to_dict()

  bigquery_types = {
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'object': 'STRING',  # Default to string for object types
        # Add more mappings as needed
    }

    # Create the BigQuery schema
  schema = [SchemaField(col, bigquery_types[str(dtype)])
              for col, dtype in data_types.items()]
  print(schema)
  return schema

def create_dataset_and_table(project_id, dataset_name, table_id, schema, location='US'):
  """Creates a dataset and a table in the specified project and location.

  Args:
    project_id: The project ID.
    dataset_name: The dataset name.
    table_id: The table name.
    schema: The table schema.
    location: The location of the dataset. Defaults to 'US'.
  """

  client = Client(project=project_id)

  # Create the dataset (API request)
  dataset_ref = client.dataset(dataset_name)
  dataset_ref.location = location
  
  try:
      client.create_dataset(dataset_ref)
      print(f"Dataset {dataset_name} created.")
  except Exception as e:  # Handle existing dataset
      print("Error creating Dataset: ", e)

  # Create the table within the dataset (API request)
  table_ref = dataset_ref.table(table_id)
  table = Table(table_ref, schema=schema)
  try:
      client.create_table(table)
      print(f"Table {table_id} created.")
  except Exception as e:  # Handle existing table (optional)
      print("Error creating table: ", e)





