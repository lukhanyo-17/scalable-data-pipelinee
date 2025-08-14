import os
from typing import Optional
from .config import SETTINGS
from pyspark.sql import DataFrame

def write_outputs(df_clean: DataFrame, df_user: DataFrame, df_daily: DataFrame, output_dir: Optional[str] = None):
    out = output_dir or SETTINGS.output_dir
    df_clean.write.mode("overwrite").parquet(os.path.join(out, "clean_transactions"))
    df_user.write.mode("overwrite").parquet(os.path.join(out, "user_aggregates"))
    df_daily.write.mode("overwrite").parquet(os.path.join(out, "daily_volume"))
    df_user.limit(1000).toPandas().to_csv(os.path.join(out, "user_aggregates_preview.csv"), index=False)
    df_daily.limit(1000).toPandas().to_csv(os.path.join(out, "daily_volume_preview.csv"), index=False)

def load_postgres(df_user: DataFrame, df_daily: DataFrame):
    if not SETTINGS.load_db:
        return
    url = f"jdbc:postgresql://{SETTINGS.db_host}:{SETTINGS.db_port}/{SETTINGS.db_name}"
    props = {"user": SETTINGS.db_user, "password": SETTINGS.db_password, "driver": "org.postgresql.Driver"}
    df_user.write.mode("overwrite").jdbc(url=url, table="user_aggregates", properties=props)
    df_daily.write.mode("overwrite").jdbc(url=url, table="daily_volume", properties=props)
