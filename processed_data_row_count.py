import boto3
import json
from datetime import datetime
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Initialize Glue and Spark
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

def get_secret():
    secret_name = "phagos-rds-postgresql-credentials"
    region_name = "eu-north-1"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        return json.loads(get_secret_value_response['SecretString'])
    except ClientError as e:
        print(f"[{datetime.now()}] Error retrieving secret: {e}")
        raise

secret = get_secret()
jdbc_url = f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}"
connection_properties = {
    "user": secret["username"],
    "password": secret["password"],
    "driver": "org.postgresql.Driver"
}

tables = [
    "public.genome_metadata",
    "public.hosts",
    "public.phage_host_map",
    "public.phages",
    "public.samples_isolates"
]

for table in tables:
    print(f"\nReading table: {table}")
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table,
        properties=connection_properties
    )
    row_count = df.count()
    print(f"✔ Table {table} has {row_count} rows")
