from datetime import datetime, timedelta
import json
import logging
import mimetypes
import os
import shutil
from typing import Any

from google.cloud import bigquery, storage
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
import requests
from requests.auth import HTTPBasicAuth
from tenacity import retry, stop_after_attempt, wait_random_exponential

jira_api_url          = os.getenv("BASE_URL")
project_name          = os.getenv("PROJECT_NAME")
auth_token       = HTTPBasicAuth(os.getenv("EMAIL"), os.getenv("API_TOKEN"))
dataset_name          = os.getenv("DATASET")
gcs_bucket_name       = os.getenv("GCP_STORAGE_BUCKET")
bq_client             = bigquery.Client(project=project_name)
gcs_storage_client    = storage.Client(project=project_name)
gcs_bucket            = gcs_storage_client.bucket(gcs_bucket_name)

downloads_folder = os.getenv("TMP_DOWNLOADS_FOLDER")
os.makedirs(downloads_folder, exist_ok=True)
tmp_outpath = f"{downloads_folder}/{datetime.now().strftime('%Y%m%d')}"
os.makedirs(tmp_outpath, exist_ok=True)

RESOURCE_TYPES = {
    "spaces": "{url}spaces",
    "pages": "{url}pages",
    "tasks": "{url}tasks",
    "blogposts": "{url}blogposts",
    "footer-comments": "{url}footer-comments",
    "inline-comments": "{url}inline-comments",
    "attachments": "{url}attachments",
}

spaces_table_schema = [
    bigquery.SchemaField("spaceOwnerId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("createdAt", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("authorId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("homepageId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("key", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("currentActiveAlias", "STRING", mode="NULLABLE"),
]

comments_table_schema = [
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pageId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("blogPostId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("attachmentId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("resolutionStatus", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("properties", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("parentCommentId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("resolutionLastModifierId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("resolutionLastModifiedAt", "STRING", mode="NULLABLE"),
]

attachments_table_schema = [
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pageId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("createdAt", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("blogPostId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("comment", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("downloadLink", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("customContentId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("mediaType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("mediaTypeDescription", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("fileId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("fileSize", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("webuiLink", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gcsLink", "STRING", mode="NULLABLE"),
]

blogposts_table_schema = [
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("spaceId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("createdAt", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("authorId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("body", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
]

tasks_table_schema = [
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("localId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("spaceId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pageId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("blogPostId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("body", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("createdBy", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("assignedTo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("completedBy", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("createdAt", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("updatedAt", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("dueAt", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("completedAt", "STRING", mode="NULLABLE"),
]

pages_table_schema = [
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("spaceId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("parentId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("parentType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("position", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("authorId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ownerId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("lastOwnerId", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("subtype", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("createdAt", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("version", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("body", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sourceTemplateEntityId", "STRING", mode="NULLABLE"),
]

def configure_cloud_logging() -> logging.Logger:
    """Create a cloud logging handler"""
    client = google.cloud.logging.Client.from_service_account_json(
        os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), project=os.getenv("PROJECT_NAME"))
    client.setup_logging(log_level=logging.INFO)
    cloud_handler = CloudLoggingHandler(client, name=os.getenv("GCP_LOGGING_SERVICE_NAME"))
    file_handler = logging.FileHandler(f"{tmp_outpath}/debug_{datetime.now().strftime("%Y%m%d")}.log", mode="w", encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s")
    file_handler.setFormatter(formatter)

    logger = logging.getLogger(os.getenv("GCP_LOGGING_SERVICE_NAME"))
    logger.addHandler(cloud_handler)
    logger.addHandler(file_handler)

    return logger
LOGGER = configure_cloud_logging()

def directory_exists(directory_name: str) -> bool:
    """
    Check if directory_name is in the bucket.

    Args:
        bucket: the Google Cloud Storage to check in.
        directory_name: the name of the directory to search for.
    Returns:
        True if the directory name is in the bucket, otherwise False
    """
    if directory_name[-1] != "/":
        directory_name += "/"
    blobs = list(gcs_bucket.list_blobs(prefix=directory_name, max_results=1))

    return len(blobs) > 0

def gcs_add_directory(directory_name: str) -> bool:
    """
    Add an empty directory to the cloud storage bucket.

    Args:
        directory_name: the name of the directory to add.
    Returns:
        True to signal success.
    """
    if not directory_exists(directory_name):
        if directory_name[-1] != "/":
            directory_name = directory_name + "/"

        blob = gcs_bucket.blob(directory_name)
        blob.upload_from_string("", content_type="application/x-www-form-urlencoded:charset=UTF-8")
    return True

def gcs_check_and_create_bucket(bucket: storage.Bucket, bucket_name: str) -> bool:
    """
    Create a bucket if it does not exist in GCS.
    Args:
        bucket: a bucket instance
        bucket_name: name of the bucket to create if it does not exist
    Returns:
        True if the bucket exists or was created successfully, False otherwise.
    """
    if bucket.exists():
        return True
    else:
        try:
            bucket = gcs_storage_client.create_bucket(bucket_name)
        except Exception:
            return False
    return True

def gcs_add_file(file_path: str, directory_name: str = "confluence_attachments/") -> str:
    """
    Add a file to the cloud storage bucket.
    Args:
        file_path: local file path to the file to add to GCS
        directory_name: the name of the GCS directory to upload the file to.
    Returns:
        the public url of the image
    """
    gcs_check_and_create_bucket(gcs_bucket, gcs_bucket_name)

    if directory_name[-1] != "/":
        directory_name = directory_name + "/"

    gcs_add_directory(directory_name)

    blob = gcs_bucket.blob(directory_name + os.path.basename(file_path))
    blob.upload_from_filename(file_path)
    return blob.self_link

def download_and_verify_confluence_file(file_url: str, storage_location: str = tmp_outpath) -> str | None:
    """
    Download file attachments in confluence
    Args:
        file_url: the download url of the file.
        storage_location: the location where to store the file.
    Returns:
        The path to where the file was stored.
    """
    file_path = download_confluence_file(file_url, storage_location)

    if file_path:
        if verify_file_content(file_path):
            LOGGER.info("[download_and_verify_jira_file] ✓ File downloaded and verified successfully")
        else:
            LOGGER.warning("[download_and_verify_jira_file] ⚠ File may be corrupted or in unexpected format")
        return file_path
    else:
        LOGGER.error("[download_and_verify_jira_file] ✗ File download failed")
        return None

def download_confluence_file(file_url: str, save_dir: str) -> str:
    """
    Download a file from COnfluence
    Args:
        file_url: the url of the file to download
        save_dir: directory where the file should be saved
    Returns:
        path to where the file has been saved
    """
    save_path = ""
    try:
        # file_url format: /download/attachments/818577942/image2021-4-12_13-0-42.png?version=1&modificationDate=1618225243798&cacheVersion=1&api=v2
        download_url = os.getenv("BASE_URL").removesuffix("api/v2/") + file_url.removeprefix("/")
        response = requests.get(download_url, auth=auth_token, stream=True, timeout=15)
        response.raise_for_status()

        filename = file_url.split("/")[-1].split("?")[0]
        if not filename:
            content_type = response.headers.get("content-type", "").split(";")[0]
            ext = mimetypes.guess_extension(content_type) or ""
            filename = f"confluence_file{ext}"

        save_path = get_next_filename(os.path.join(save_dir, filename))

        with open(save_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    except requests.exceptions.RequestException as e:
        LOGGER.error("[_download_confluence_file] Error: %s", e)
    finally:
        return save_path

def verify_file_content(file_path: str) -> bool:
    """
    Verify if the file downloaded seems to be valid based on its content.
    Args:
        file_path: the path to the file to be verified.
    Returns:
        True if the file seems valid, otherwise False.
    """
    try:
        with open(file_path, "rb") as f:
            header = f.read(8)

        # Common file signatures
        file_signatures = {
            b"%PDF": "PDF file",
            b"\xFF\xD8\xFF": "JPEG image",
            b"\x89PNG\r\n\x1A\n": "PNG image",
            b"PK\x03\x04": "ZIP archive",
            b"GIF87a": "GIF image",
            b"GIF89a": "GIF image",
        }

        for signature, _ in file_signatures.items():
            if header.startswith(signature):
                return True

        # No signature match but file has content
        if len(header) > 0:
            return True

        return False
    except IOError:
        return False

def get_next_filename(file_path: str) -> str:
    """
    Add incremental number to a file name it the name already exists.
    Example: file.txt, file(1).txt, file(2).txt, etc.
    Args:
        file_path: the name of the file.
    Returns:
        The new name of the file given.
    """
    if not os.path.exists(file_path):
        return file_path

    name, ext = os.path.splitext(file_path)
    counter = 1
    while os.path.exists(file_path):
        file_path = f"{name}({counter}){ext}"
        counter += 1

    return file_path

def write_jsonl_file(data_list: list[dict[str, Any]], filename_prefix: str) -> str | None:
    file_path = None
    if data_list:
        file_path = f"{tmp_outpath}/{filename_prefix}_{datetime.now().strftime("%Y%m%d")}.jsonl"
        with open(file_path, "w", encoding="utf-8") as fp:
            for item in data_list:
                json.dump(item, fp=fp)
                fp.write("\n")
        LOGGER.info("[write_jsonl_file] Written %d records to %s", len(data_list), file_path)
    else:
        LOGGER.info("[write_jsonl_file] No data for %s to write", filename_prefix)
    return file_path

@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
def write_table_to_bq(table_id: str, schema: list[bigquery.SchemaField], file: str) -> bool:
    """
    Writes the data into a new table. If the table exists it is appended to.
    Args:
        table_id: the id of the table in Bigquery
        schema: a list of the table schema in Bigquery
        file: path to the jsonl file containing the data to insert
    Returns:
        True if data is successfully inserted, otherwise False
    """
    if os.stat(file).st_size == 0:
        LOGGER.info("[write_table_to_bq] File %s is empty, no data to write", file)
        return True

    today = datetime.today().strftime("%Y%m%d")
    table_id += f"_{today}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        max_bad_records=10
    )

    with open(file, "rb") as fp:
        try:
            load_job = bq_client.load_table_from_file(
                file_obj=fp,
                destination=table_id,
                job_config=job_config
            )
        except (ValueError, TypeError) as e:
            LOGGER.error("[write_table_to_bq] Error while loading to BigQuery: %s", e)
            return False

        try:
            load_job.result()
        except Exception as e:
            LOGGER.error("[write_table_to_bq] Load job failed/did not complete: %s", e)
            return False

    LOGGER.info("[write_table_to_bq] Successfully written table: %s", table_id)
    return True

@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(6))
def make_api_request(resource_type: str, /, **kwargs: Any) -> list[dict[str, Any]] | None:
    """
    Make an API request to Confluence API
    Args:
        resource_type: the type of resource to get
        **kwargs: appropriate parameters to pass to the query
    """
    yesterday = datetime.today() - timedelta(days=1)
    finished = False
    next_cursor = ""
    headers = {
        "Accept": "application/json"
    }
    params = {
        "limit": 250,
        "cql": f"lastmodified >= {yesterday.strftime("%Y/%m/%d")}"
    }
    raw_url = RESOURCE_TYPES.get(resource_type)
    if not raw_url:
        LOGGER.error("unknown recource type passed")
        return None

    url = raw_url.format(url=os.getenv("BASE_URL"))

    data: list[dict[str, Any]] = []
    while not finished:
        if next_cursor:
            params["cursor"] = next_cursor

        response = requests.get(url, params=params, headers=headers, auth=auth_token, timeout=15).json()
        data.extend(response.get("results", []))
        
        if response.get("_links") and response["_links"].get("next"):
            # next page cursor is stored in _links["next"] attribute and has the following format:
            # wiki/api/v2/{resource_type}?cursor=eyJpZCI6IjYxOTc0MTM0MCIsImNvbnRlbnRPcmRlciI6ImlkIiwiY29udGVudE9yZGVyVmFsdWUiOjYxOTc0MTM0MH0=&limit=25
            link_params = response["_links"]["next"].split("?")[1].split("&")
            for item in link_params:
                if item.startswith("cursor="):
                    next_cursor = item.removeprefix("cursor=")
                    break
        else:
            finished = True

    return data

def process_data(raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Process the data downloaded, removing unnecessary fields or stringifying nested fields
    Args:
        raw_data: unprocessed extracted data
    Returns:
        processed data
    """
    data = []
    for item in raw_data:
        if "_links" in item.keys():
            del item["_links"]

        if "icon" in item.keys():
            del item["icon"]

        if "version" in item.keys():
            item["version"] = str(item["version"])

        if "properties" in item.keys():
            item["properties"] = str(item["properties"])

        if "body" in item.keys():
            item["body"] = str(item["body"])

        data.append(item)
    return data

def process_attachments(raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Process attachment data downloaded, removing unnecessary fields or stringifying nested fields.
    Also downloads and saves attachments to GCS and includes link to the GCS file in the processed data
    Args:
        raw_data: unprocessed extracted data
    Returns:
        processed data
    """
    data = []
    save_dir = tmp_outpath + "/attachments"
    os.makedirs(save_dir, exist_ok=True)

    for item in raw_data:
        if "_links" in item.keys():
            del item["_links"]

        if "icon" in item.keys():
            del item["icon"]

        if "version" in item.keys():
            item["version"] = str(item["version"])

        if "properties" in item.keys():
            item["properties"] = str(item["properties"])

        if "body" in item.keys():
            item["body"] = str(item["body"])
        
        if "downloadLink" in item.keys():
            file_path = download_and_verify_confluence_file(item["downloadLink"], save_dir)
            item["gcsLink"] = file_path

        data.append(item)
    return data

def get_data() -> None:
    spaces = make_api_request("spaces")
    LOGGER.info("Fetched %d spaces", len(spaces))
    if spaces:
        spaces = process_data(spaces)
        spaces_path = write_jsonl_file(spaces, "spaces")
        write_table_to_bq(f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.spaces", spaces_table_schema, spaces_path)

    attachments = make_api_request("attachments")
    LOGGER.info("Fetched %d attachments", len(attachments))
    if attachments:
        attachments = process_attachments(attachments)
        attachments_path = write_jsonl_file(attachments, "attachments")
        write_table_to_bq(f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.attachments", attachments_table_schema, attachments_path)

    blogposts = make_api_request("blogposts")
    LOGGER.info("Fetched %d blogposts", len(blogposts))
    if blogposts:
        blogposts = process_data(blogposts)
        blogposts_path = write_jsonl_file(blogposts, "blogposts")
        write_table_to_bq(f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.blogposts", blogposts_table_schema, blogposts_path)

    tasks = make_api_request("tasks")
    LOGGER.info("Fetched %d tasks", len(tasks))
    if tasks:
        tasks = process_data(tasks)
        tasks_path = write_jsonl_file(tasks, "tasks")
        write_table_to_bq(f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.tasks", tasks_table_schema, tasks_path)

    pages = make_api_request("pages")
    LOGGER.info("Fetched %d pages", len(pages))
    if pages:
        pages = process_data(pages)
        pages_path = write_jsonl_file(pages, "pages")
        write_table_to_bq(f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.pages", pages_table_schema, pages_path)

    footer_comments = make_api_request("footer-comments")
    inline_comments = make_api_request("inline-comments")
    all_comments = footer_comments + inline_comments
    LOGGER.info("Fetched %d comments", len(all_comments))
    if all_comments:
        all_comments = process_data(all_comments)
        all_commnents_path = write_jsonl_file(all_comments, "comments")
        write_table_to_bq(f"{os.getenv("PROJECT_NAME")}.{os.getenv("DATASET")}.comments", comments_table_schema, all_commnents_path)

def clean_up(directory: str) -> None:
    """Delete data downloaded during execution"""
    if os.path.exists(directory):
        try:
            shutil.rmtree(directory)
            LOGGER.info("[clean_up] Successfully deleted the temporary output folder")
        except OSError as e:
            LOGGER.error("[clean_up] Error deleting directory: %s", e)

if __name__ == "__main__":
    get_data()
    clean_up(downloads_folder)
