import boto3
import csv
import hashlib
import requests
import os

S3_BUCKET = "phages-project"
BENCHLING_API_KEY = os.environ["BENCHLING_API_KEY"]
BENCHLING_BASE_URL = "https://api.benchling.com/v2"
NOTEBOOK_ID = os.environ["NOTEBOOK_ID"]

headers = {
    "X-Benchling-Api-Key": BENCHLING_API_KEY,
    "Content-Type": "application/json"
}

s3 = boto3.client("s3")


def calculate_md5(stream):
    hash_md5 = hashlib.md5()
    for chunk in iter(lambda: stream.read(4096), b""):
        hash_md5.update(chunk)
    return hash_md5.hexdigest()


def count_rows(stream):
    stream.seek(0)
    row_count = sum(1 for _ in csv.reader(stream)) - 1
    return row_count


def create_benchling_entry(file_name, s3_url, checksum, row_count):
    entry_data = {
        "name": file_name,
        "notes": f"CSV uploaded to S3: {s3_url}",
        "parentNotebookId": NOTEBOOK_ID,
        "fields": {
            "s3_url": s3_url,
            "checksum": checksum,
            "row_count": row_count,
            "status": "processed"
        }
    }

    response = requests.post(
        f"{BENCHLING_BASE_URL}/entries",
        headers=headers,
        json=entry_data
    )
    response.raise_for_status()
    return response.json()


def lambda_handler(event, context):
    """
    Triggered by S3 upload event.
    Processes the uploaded CSV and updates Benchling.
    """
    for record in event["Records"]:
        s3_key = record["s3"]["object"]["key"]
        file_name = s3_key.split("/")[-1]
        s3_url = f"s3://{S3_BUCKET}/{s3_key}"

        s3_obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        stream = s3_obj["Body"]

        data = stream.read()
        checksum = hashlib.md5(data).hexdigest()

        import io
        csv_stream = io.StringIO(data.decode("utf-8"))
        row_count = sum(1 for _ in csv.reader(csv_stream)) - 1

        entry = create_benchling_entry(file_name, s3_url, checksum, row_count)
        print(f"Processed {file_name}, Benchling entry ID: {entry['id']}")

    return {"status": "success"}
