from typing import Optional
from .config import SETTINGS
from pyspark.sql import SparkSession

def get_spark(app_name: str = "ScalablePipeline"):
    return (SparkSession.builder
            .appName(app_name)
            .getOrCreate())

def read_local_csv(spark: SparkSession, path: str):
    return (spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(path))

def read_s3_csv(spark: SparkSession, bucket: str, prefix: str):
    path = f"s3a://{bucket}/{prefix}"
    return (spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(path))

def extract(spark: Optional[SparkSession] = None):
    spark = spark or get_spark()
    if SETTINGS.run_local and not SETTINGS.use_s3:
        df = read_local_csv(spark, SETTINGS.input_path)
    else:
        df = read_s3_csv(spark, SETTINGS.s3_bucket, SETTINGS.s3_prefix)
    return df
