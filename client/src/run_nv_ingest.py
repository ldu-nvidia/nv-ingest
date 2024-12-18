import os
from PIL import Image
from io import BytesIO
import base64

# client config
HTTP_HOST = os.environ.get('HTTP_HOST', "localhost")
HTTP_PORT = os.environ.get('HTTP_PORT', "7670")
TASK_QUEUE = os.environ.get('TASK_QUEUE', "morpheus_task_queue")

# minio config
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', "minioadmin")
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', "minioadmin")

# time to wait for job to complete
DEFAULT_JOB_TIMEOUT = 90

# sample input file and output directory
SAMPLE_PDF = "/home/ldu/Documents/Repos/nv-ingest/data/ASML/snapdragon_600_apq_8064_data_sheet.pdf"

from base64 import b64decode
import time

from nv_ingest_client.client import NvIngestClient
from nv_ingest_client.message_clients.rest.rest_client import RestClient
from nv_ingest_client.primitives import JobSpec
from nv_ingest_client.primitives.tasks import DedupTask
from nv_ingest_client.primitives.tasks import EmbedTask
from nv_ingest_client.primitives.tasks import ExtractTask
from nv_ingest_client.primitives.tasks import FilterTask
from nv_ingest_client.primitives.tasks import SplitTask
from nv_ingest_client.primitives.tasks import StoreTask, StoreEmbedTask
from nv_ingest_client.primitives.tasks import VdbUploadTask
from nv_ingest_client.util.file_processing.extract import extract_file_content
from IPython import display
import logging
logger = logging.getLogger(__name__)


logging.basicConfig(filename='myapp.log', level=logging.INFO)
logger.info("extract pdf file contents: ")
file_content, file_type = extract_file_content(SAMPLE_PDF)
logger.info("extracted file content")

client = NvIngestClient(
    message_client_allocator=RestClient,
    message_client_hostname=HTTP_HOST,
    message_client_port=HTTP_PORT,
    message_client_kwargs=None,
    msg_counter_id="nv-ingest-message-id",
    worker_pool_size=1,
)

job_spec = JobSpec(
    document_type=file_type,
    payload=file_content,
    source_id=SAMPLE_PDF,
    source_name=SAMPLE_PDF,
    extended_options={
        "tracing_options": {
            "trace": True,
            "ts_send": time.time_ns(),
        }
    },
)

extract_task = ExtractTask(
    document_type=file_type,
    extract_text=True,
    extract_images=True,
    extract_tables=True,
    text_depth="document",
    extract_tables_method="yolox",
)

dedup_task = DedupTask(
    content_type="image",
    filter=False,
)

job_spec.add_task(extract_task)
job_spec.add_task(dedup_task)

job_id = client.add_job(job_spec)
client.submit_job(job_id, TASK_QUEUE)
generated_metadata = client.fetch_job_result(
    job_id, timeout=DEFAULT_JOB_TIMEOUT
)[0]

print(type(generated_metadata))
print(len(generated_metadata))


def redact_metadata_helper(metadata: dict) -> dict:
    """A simple helper function to redact `metadata["content"]` so improve readability."""
    text_metadata_redact = metadata.copy()
    text_metadata_redact["content"] = "<---Redacted for readability--->"
    return text_metadata_redact



def decode_base64_to_bitmap_and_save(base64_string, output_file_path):
    try:
        # Decode the base64 string to bytes
        decoded_bytes = base64.b64decode(base64_string)

        # Create a BytesIO stream from the decoded bytes
        byte_stream = BytesIO(decoded_bytes)

        # Open the image using Pillow (PIL)
        decoded_bitmap = Image.open(byte_stream)

        # Save the decoded bitmap as a PNG file
        decoded_bitmap.save(output_file_path, "PNG")

        return True  # Successful save
    except Exception as e:
        print(f"Error: {e}")
        return False  # Saving failed


for i in range(len(generated_metadata)):
    #if generated_metadata[i]['document_type'] == "image":
    img_metadata = generated_metadata[i]["metadata"]
    redact_metadata_helper(img_metadata)
    output_file_path = "image_" + str(i+1) + ".png"
    if decode_base64_to_bitmap_and_save(img_metadata["content"], output_file_path):
        print(f"Image saved as {output_file_path}")
    else:
        print("Failed to decode and save the image.")