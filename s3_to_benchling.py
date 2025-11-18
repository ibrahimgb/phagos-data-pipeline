import boto3
import csv
import hashlib
import os

S3_BUCKET = "phages-project"
s3 = boto3.client("s3")


def calculate_md5(stream):
    # Calculate MD5 checksum
    hash_md5 = hashlib.md5()
    for chunk in iter(lambda: stream.read(4096), b""):
        hash_md5.update(chunk)
    return hash_md5.hexdigest()


def count_rows(stream):
    stream.seek(0)
    row_count = sum(1 for _ in csv.reader(stream)) - 1
    return row_count


def process_s3_file(s3_key):
    s3_obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    stream = s3_obj["Body"]

    data = stream.read()
    checksum = hashlib.md5(data).hexdigest()

    import io
    csv_stream = io.StringIO(data.decode("utf-8"))
    row_count = sum(1 for _ in csv.reader(csv_stream)) - 1

    print(f"File: {s3_key}, Rows: {row_count}, Checksum: {checksum}")
