import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Initialize Glue
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

database = "row-data"

tables = [
    "dim_raw_genome_metadata",
    "dim_raw_hosts",
    "dim_raw_phages",
    "dim_raw_samples_isolates"
]

for table in tables:
    print(f"\nReading table: {table}")

    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table
    )
    df = dyf.toDF()
    
    row_count = df.count()
    print(f"✔ Table {table} has {row_count} rows")
