import boto3
import json
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Initialize Glue and Spark
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

def get_secret(secret_name="phagos-rds-postgresql-credentials", region_name="eu-north-1"):

    client = boto3.client("secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
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
    df = spark.read.jdbc(url=jdbc_url, table=table, properties=connection_properties)
    print(f"Table {table} has {df.count()} rows")
