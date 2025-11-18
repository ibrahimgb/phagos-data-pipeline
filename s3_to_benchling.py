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

