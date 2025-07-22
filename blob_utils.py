from azure.storage.blob import BlobServiceClient
from config import STORAGE_CONNECTION_STRING, BLOB_CONTAINER

# def upload_file_to_blob(file):
#     blob_service = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
#     blob_name = f"{uuid.uuid4()}_{file.name}"
#     blob_client = blob_service.get_blob_client(container='data-pipeline', blob='silver/final.csv')
#     blob_client.upload_blob(file, overwrite=True)
#     return blob_client.url

def upload_file_to_blob(file, destination_blob_path="bronze/"):
    blob_service = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    blob_client = blob_service.get_blob_client(container=BLOB_CONTAINER, blob=destination_blob_path)
    blob_client.upload_blob(file, overwrite=True)
    return blob_client.url



