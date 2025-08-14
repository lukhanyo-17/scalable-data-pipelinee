import argparse
import os
from .config import SETTINGS
from .extract import get_spark, extract
from .transform import clean_transactions, aggregates, daily_volume, simple_fraud_flags
from .load import write_outputs, load_postgres

def parse_args():
    p = argparse.ArgumentParser(description="Scalable Data Pipeline")
    p.add_argument("--run-local", action="store_true", help="Use local CSV input")
    p.add_argument("--use-s3", action="store_true", help="Use S3 input")
    p.add_argument("--load-db", action="store_true", help="Load outputs to Postgres")
    p.add_argument("--dry-run", action="store_true", help="Build plan without writing outputs")
    return p.parse_args()

def main():
    args = parse_args()
    if args.run_local:
        os.environ["RUN_LOCAL"] = "1"
    if args.use_s3:
        os.environ["USE_S3"] = "1"
    if args.load_db:
        os.environ["LOAD_DB"] = "1"

    spark = get_spark()
    df_raw = extract(spark)
    df_clean = clean_transactions(df_raw)
    df_flags = simple_fraud_flags(df_clean)
    df_user = aggregates(df_flags)
    df_daily = daily_volume(df_flags)

    if not args.dry_run:
        os.makedirs(SETTINGS.output_dir, exist_ok=True)
        write_outputs(df_flags, df_user, df_daily, SETTINGS.output_dir)
        if args.load_db:
            load_postgres(df_user, df_daily)

    spark.stop()
    print("Pipeline finished. Outputs in:", SETTINGS.output_dir)

if __name__ == "__main__":
    main()
