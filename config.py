import os
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv()
@dataclass
class Settings:
    run_local: bool = bool(os.getenv("RUN_LOCAL", "1") == "1")
    input_path: str = os.getenv("INPUT_PATH", "sample_data/raw_transactions.csv")
    output_dir: str = os.getenv("OUTPUT_DIR", "output")
    use_s3: bool = bool(os.getenv("USE_S3", "0") == "1")
    s3_bucket: str = os.getenv("S3_BUCKET", "")
    s3_prefix: str = os.getenv("S3_PREFIX", "raw/transactions/")
    load_db: bool = bool(os.getenv("LOAD_DB", "0") == "1")
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "etl")
    db_user: str = os.getenv("DB_USER", "etl_user")
    db_password: str = os.getenv("DB_PASSWORD", "etl_password")
SETTINGS = Settings()

def validate_settings(cfg: Settings) -> None:
    if cfg.use_s3 and not cfg.s3_bucket:
        print("Use S3=1, but S3_BUCKET is not set.")
    if cfg.load_db and (cfg.db_password in ["", "etl_password"]):
        print("Load DB=1, but DB_PASSWORD is not set or is the default value.")   

validate_settings(SETTINGS)

if __name__ == "__main__":
    from pprint import pprint
    pprint(SETTINGS)
