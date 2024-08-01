import googleapiclient.discovery
import pandas as pd
from google.cloud import storage, secretmanager

def get_secret(project_id: str, secret_id: str, client=None) -> str:
    """Fetches a secret from Google Secret Manager.

    Args:
        project_id (str): The Google Cloud project ID.
        secret_id (str): The ID of the secret to retrieve.
        client (Optional[secretmanager.SecretManagerServiceClient]): 
            An optional Secret Manager client object. If not provided, a new client is created.

    Returns:
        str: The decoded secret payload.
    """
    if client is None:
        # Initialize Secret Manager client if not provided
        client = secretmanager.SecretManagerServiceClient()

    # Construct the resource name of the secret
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    # Access the secret version and fetch the payload
    response = client.access_secret_version(name=name)

    # Decode the secret from bytes to string
    return response.payload.data.decode('UTF-8') 

def fetch_youtube_comments(project_id: str, bucket_name: str, destination_blob_name: str, **kwargs) -> None:
    """Fetches comments from a YouTube video and uploads them to Google Cloud Storage as a CSV file.

    Args:
        project_id (str): The Google Cloud project ID.
        bucket_name (str): The name of the GCS bucket where the file will be stored.
        destination_blob_name (str): The name of the file (blob) to be created in the GCS bucket.

    Returns:
        None
    """
    # Initialize an empty DataFrame with specified columns
    comments = pd.DataFrame(columns=['comments', 'likes'])

    # Retrieve the YouTube API key from Secret Manager
    youtube_api_key = get_secret(project_id, 'YOUTUBE_API_KEY')

    # Set up the YouTube API client
    api_service_name = "youtube"
    api_version = "v3"
    video_id = "4CLu-twnSkc"  # Hardcoded video ID (change it as you wish)
    
    youtube = googleapiclient.discovery.build(
        api_service_name,
        api_version,
        developerKey=youtube_api_key  # Use the API key to authenticate
    )

    def execute_request(page_token=None) -> dict:
        """Executes the API request to fetch YouTube comments.

        Args:
            page_token (Optional[str]): Token for the next page of results.

        Returns:
            dict: The API response containing comments and other metadata.
        """
        # Build the API request
        request = youtube.commentThreads().list(
            part="snippet",  # Retrieve the 'snippet' part of the comments
            videoId=video_id,
            textFormat="plainText",
            pageToken=page_token,  # Fetch the next page if a token is provided
            maxResults=100  # Maximum number of comments per request
        )
        # Execute the request and return the response
        response = request.execute()
        return response
    
    next_page_token = None  # Initialize page token
    
    # Loop through the pages of comments until there are no more pages
    while True:
        response = execute_request(next_page_token)

        # Iterate over each comment in the response
        for item in response["items"]:
            # Extract the comment text and like count
            comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            like_count = item["snippet"]["topLevelComment"]["snippet"]["likeCount"]

            # Create a new row with the extracted data
            new_row = pd.DataFrame({'comments': [comment_text], 'likes': [like_count]})
            # Concatenate the new row to the comments DataFrame
            comments = pd.concat([comments, new_row], ignore_index=True)
        
        # Get the next page token, if it exists
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break  # Exit the loop if there are no more pages

    # Initialize the Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)  # Access the specified GCS bucket
    blob = bucket.blob(destination_blob_name)  # Create a blob (file) object

    # Upload the DataFrame as a CSV file to GCS
    blob.upload_from_string(comments.to_csv(index=False), 'text/csv')

    print(f"File uploaded to {destination_blob_name}.")
