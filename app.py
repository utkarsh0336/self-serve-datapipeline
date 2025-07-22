import streamlit as st
import uuid
from io import BytesIO, StringIO
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient
from blob_utils import upload_file_to_blob
from adf_dynamic import create_pipeline, trigger_pipeline
import pyarrow.parquet as pq
import pyarrow as pa
import tempfile
import os
from config import STORAGE_CONNECTION_STRING, BLOB_CONTAINER
from azure.storage.blob import ContainerClient

st.set_page_config("Dynamic ADF Pipeline", layout="centered")
st.title("🛠️ Create Your Own ADF Pipeline")

user_id = st.text_input("Your email or name")

# Pipeline Name
pipeline_name = st.text_input("Pipeline name (optional)", value=f"user_pipeline_{uuid.uuid4().hex[:6]}")

# Step 1: Source
st.subheader("📥 Source")
source_type = st.selectbox("Source Type", ["Upload", "External URL"])

# Step 2: Destination
st.subheader("📤 Destination")
destination_path = st.text_input("Output blob path (e.g., bronze/final.csv)")


# Extract final directory name from destination_path (e.g., bronze/final.csv → final)
import os

final_dest = None
if destination_path:
    final_dest = os.path.splitext(os.path.basename(destination_path))[0]  # "final" from "final.csv"
    print(f"Final destination: {final_dest}")

    dest_base = f'silver/{final_dest}/'  # Properly interpolated path


file_format = st.selectbox("Format", ["csv", "json"])

# Step 3: Storage Choice
st.markdown("### 💾 Storage Options")
storage_choice = st.radio("Where to store output?", ["Default (silver/)", "Custom Azure Blob"])

if storage_choice == "Custom Azure Blob":
    custom_conn_str = st.text_input("Enter Azure Storage Connection String", type="password")
    custom_container = st.text_input("Enter Container Name")
    use_custom_storage = True
else:
    use_custom_storage = False

# Step 4: Transformations
st.subheader("🔧 Transformations")
drop_cols = st.text_input("Drop columns (comma-separated)")
filter_condition = st.text_input("Filter (e.g., age > 25)")
rename_map = st.text_area("Rename columns (format: old:new, one per line)")

rename_dict = {}
if rename_map:
    for line in rename_map.splitlines():
        if ":" in line:
            old, new = line.split(":")
            rename_dict[old.strip()] = new.strip()

# Step 5: Upload Logic
source_path = None
if source_type == "Upload":
    file = st.file_uploader("Upload your file")
    if file and destination_path:
        try:
            if use_custom_storage:
                blob_service = BlobServiceClient.from_connection_string(custom_conn_str)
                blob_client = blob_service.get_blob_client(container=custom_container, blob=destination_path)
                blob_client.upload_blob(file, overwrite=True)
                source_path = blob_client.url
            else:
                source_path = upload_file_to_blob(file, destination_blob_path=destination_path)
            st.success("✅ File uploaded successfully.")
        except Exception as e:
            st.error(f"❌ Upload failed: {e}")
else:
    source_path = st.text_input("Enter external file URL")

# Step 6: Trigger
if st.button("🚀 Create and Run Pipeline"):
    if not source_path or not destination_path:
        st.error("Source and destination are required.")
    else:
        params = {
            "sourcePath": source_path,
            "destinationPath": destination_path,
            "fileFormat": file_format,
            "dropColumns": drop_cols,
            "filterCondition": filter_condition,
            "renameColumns": str(rename_dict)
        }

        created = create_pipeline(pipeline_name, params)
        if created:
            run_id = trigger_pipeline(pipeline_name, params)
            st.success(f"✅ Pipeline created and triggered.")

            # Preview output if in default storage & CSV
            # if not use_custom_storage and file_format == "csv":
            #     try:
            #         blob_client = BlobClient.from_connection_string(
            #             conn_str=STORAGE_CONNECTION_STRING,
            #             container_name=BLOB_CONTAINER,
            #             blob_name=dest_base
            #         )
            #         blob_data = blob_client.download_blob().readall()
            #         df = pd.read_csv(StringIO(blob_data.decode()))
            #         st.subheader("🔍 Output Preview")
            #         st.dataframe(df.head())
            #     except Exception as e:
            #         st.warning(f"Could not preview output: {e}")
        else:
            st.error("❌ Failed to create pipeline.")

# if not use_custom_storage and file_format == "csv":
#     st.subheader("🔍 Output Preview")
#     if st.button("🔄 Refresh Preview"):
#         try:
#             blob_client = BlobClient.from_connection_string(
#                 conn_str=STORAGE_CONNECTION_STRING,
#                 container_name=BLOB_CONTAINER,
#                 blob_name=dest_base
#             )
            
#             print("Blob URL:", blob_client.url)
#             blob_data = blob_client.download_blob().readall()
#             print("Blob data:", blob_data)

#             df = pd.read_parquet(BytesIO(blob_client))
#             st.dataframe(df.head())
#         except Exception as e:
#             st.warning(f"Could not preview output: {e}")


if not use_custom_storage and file_format == "csv":
    st.subheader("🔍 Output Preview")
    if st.button("🔄 Refresh Preview"):
        try:
            # Extract final folder from destination path
            final_dest = os.path.splitext(os.path.basename(destination_path))[0] # (e.g., final.csv → final)
            dest_prefix = f"silver/{final_dest}/"

            container_client = ContainerClient.from_connection_string(
                conn_str=STORAGE_CONNECTION_STRING,
                container_name=BLOB_CONTAINER
            )

            # List blobs under the prefix (excluding _delta_log/)
            blob_list = container_client.list_blobs(name_starts_with=dest_prefix)
            dfs = []
            for blob in blob_list:
                if "_delta_log/" in blob.name:
                    continue
                if blob.name.endswith(".parquet"):
                    blob_client = container_client.get_blob_client(blob.name)
                    data = blob_client.download_blob().readall()
                    df_part = pd.read_parquet(BytesIO(data))
                    dfs.append(df_part)

            if dfs:
                final_df = pd.concat(dfs, ignore_index=True)
                st.dataframe(final_df.head(10))
            else:
                st.info("No Parquet part files found.")
        except Exception as e:
            st.warning(f"❌ Could not preview output: {e}")



