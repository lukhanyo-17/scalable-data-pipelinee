from pyspark.sql import DataFrame, functions as F, Window

def clean_transactions(df: DataFrame) -> DataFrame:
    return (df
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))
        .filter(F.col("user_id").isNotNull() & F.col("timestamp").isNotNull())
    )

def aggregates(df: DataFrame) -> DataFrame:
    return (df
        .groupBy("user_id")
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("amount").alias("total_spent"),
            F.avg("amount").alias("avg_amount")
        )
    )

def daily_volume(df: DataFrame) -> DataFrame:
    return (df
        .withColumn("day", F.to_date("timestamp"))
        .groupBy("day")
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("amount").alias("total_amount")
        )
    )

def simple_fraud_flags(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("user_id").orderBy(F.col("timestamp").cast("long")).rangeBetween(-3600, 0)
    return (df
        .withColumn("tx_in_last_hour", F.count("*").over(w))
        .withColumn("high_freq_flag", (F.col("tx_in_last_hour") >= 10).cast("int"))
    )
