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
print(f"Retrieved RDS credentials for host: {secret['host']}")

job.commit()
