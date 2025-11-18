import sys
import boto3
import json
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue and Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

def get_secret():
    secret_name = "phagos-rds-postgresql-credentials"
    region_name = "eu-north-1"
    client = boto3.client('secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise

secret = get_secret()

jdbc_url = f"jdbc:postgresql://{secret['host']}:{secret['port']}/{secret['dbname']}"
connection_properties = {
    "user": secret["username"],
    "password": secret["password"],
    "driver": "org.postgresql.Driver"
}

s3_paths = {
    "phages": "s3://phages-project/row-data/phages/",
    "hosts": "s3://phages-project/row-data/hosts/",
    "genome_metadata": "s3://phages-project/row-data/genome_metadata/",
    "samples_isolates": "s3://phages-project/row-data/samples_isolates/"
}

postgres_tables = {
    "phages": "public.phages",
    "hosts": "public.hosts",
    "genome_metadata": "public.genome_metadata",
    "samples_isolates": "public.samples_isolates",
    "phage_host_map": "public.phage_host_map"
}

def convert_data_types(df, table_name):
    if table_name == "genome_metadata":
        df = df.withColumn("release_date", df["release_date"].cast("date"))
    elif table_name == "phages":
        df = df.withColumn("discovery_date", df["discovery_date"].cast("date"))
    elif table_name == "samples_isolates":
        df = df.withColumn("collection_date", df["collection_date"].cast("date"))
    return df

table_order = ["phages", "hosts", "genome_metadata", "samples_isolates"]

for table_name in table_order:
    s3_path = s3_paths[table_name]
    print(f"Processing {table_name}...")

    df = spark.read.csv(s3_path, header=True, inferSchema=True)

    df = convert_data_types(df, table_name)

    df.write.jdbc(
        url=jdbc_url,
        table=postgres_tables[table_name],
        mode="append",
        properties=connection_properties
    )
    print(f"Successfully wrote {table_name} to PostgreSQL.")

print("Processing phage_host_map...")
phages_df = spark.read.jdbc(
    url=jdbc_url,
    table="public.phages",
    properties=connection_properties
)
samples_df = spark.read.jdbc(
    url=jdbc_url,
    table="public.samples_isolates",
    properties=connection_properties
)

phage_host_map_df = samples_df.select("phage_id", "host_id").distinct()

phage_host_map_df.write.jdbc(
    url=jdbc_url,
    table=postgres_tables["phage_host_map"],
    mode="append",
    properties=connection_properties
)
print("Successfully wrote phage_host_map to PostgreSQL.")

job.commit()
print("ETL job completed successfully!")
