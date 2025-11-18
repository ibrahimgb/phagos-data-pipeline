import boto3
import json
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue and Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

def get_secret(secret_name="phagos-rds-postgresql-credentials", region="eu-north-1"):
    client = boto3.client("secretsmanager", region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
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

tables = [
    "public.genome_metadata",
    "public.hosts",
    "public.phage_host_map",
    "public.phages",
    "public.samples_isolates"
]

for table in tables:
    print(f"\nReading table: {table}")
    df = spark.read.jdbc(url=jdbc_url, table=table, properties=connection_properties)
    row_count = df.count()
    print(f"Table {table} has {row_count} rows")

job.commit()
print("ETL job completed successfully")
