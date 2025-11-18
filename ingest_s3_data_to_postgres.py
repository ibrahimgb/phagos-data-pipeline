import boto3
import json

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
print(f"Retrieved RDS credentials for host: {secret['host']}")
