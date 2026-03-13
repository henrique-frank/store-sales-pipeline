import argparse
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import snowflake.connector
import yaml

def detect_file_type(filename: str) -> str | None:
    name = Path(filename).name
    if name.startswith("stores_"):
        return "stores"
    if name.startswith("sales_"):
        return "sales"
    return None


def extract_batch_date(filename: str) -> str:
    """Extract YYYY-MM-DD batch_date from a filename like sales_20211001.csv."""
    name = Path(filename).stem
    match = re.search(r"(\d{8})", name)
    if not match:
        raise ValueError(f"Cannot extract batch_date from {filename}")
    d = match.group(1)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


def has_header(filepath: str) -> bool:
    """Heuristic: if first row contains known column names, treat it as a header."""
    known_store_headers = {"store_group", "store_token", "store_name"}
    known_sales_headers = {
        "store_token",
        "transaction_id",
        "receipt_token",
        "transaction_time",
        "amount",
        "user_role",
    }
    with open(filepath, "r", encoding="utf-8") as f:
        first_line = f.readline().strip().lower()
    fields = {col.strip() for col in first_line.split(",")}
    if len(fields & known_store_headers) >= 2:
        return True
    if len(fields & known_sales_headers) >= 2:
        return True
    return False


def get_connection(sf_cfg: dict):
    return snowflake.connector.connect(
        account=sf_cfg["account"],
        user=sf_cfg["user"],
        password=sf_cfg["password"],
        database=sf_cfg.get("database", "SALES_DW"),
        warehouse=sf_cfg.get("warehouse", "COMPUTE_WH"),
        role=sf_cfg.get("role", "SYSADMIN"),
    )


def is_already_processed(
    cursor, file_type: str, batch_date: str, file_name: str
) -> bool:
    """
    Simple idempotency: assume the same file (by name) is not resent
    with different content. If we have already logged this file as
    LOADED for the given type and batch_date, we skip it.
    """
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM BRONZE.INGESTION_LOG
        WHERE file_type = %s
          AND batch_date = %s
          AND file_name = %s
          AND status = 'LOADED'
        """,
        (file_type, batch_date, file_name),
    )
    return cursor.fetchone()[0] > 0


def copy_into_bronze(
    cursor, file_type: str, file_name: str, batch_date: str, skip_header: bool
):
    """
    Single COPY INTO for both stores and sales, driven by file_type.
    """
    skip = 1 if skip_header else 0

    if file_type == "stores":
        sql = f"""
            COPY INTO BRONZE.STORES_RAW (
                store_group, store_token, store_name,
                batch_date, file_name
            )
            FROM (
                SELECT
                    $1, $2, $3,
                    '{batch_date}'::DATE,
                    '{file_name}'
                FROM @BRONZE.STG_INBOX/{file_name}
            )
            FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV SKIP_HEADER = {skip})
            ON_ERROR = 'CONTINUE'
        """
    else:
        sql = f"""
            COPY INTO BRONZE.SALES_RAW (
                store_token, transaction_id, receipt_token,
                transaction_time, amount, user_role,
                batch_date, file_name
            )
            FROM (
                SELECT
                    $1, $2, $3, $4, $5, $6,
                    '{batch_date}'::DATE,
                    '{file_name}'
                FROM @BRONZE.STG_INBOX/{file_name}
            )
            FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV SKIP_HEADER = {skip})
            ON_ERROR = 'CONTINUE'
        """

    cursor.execute(sql)
    return cursor.fetchone()


def archive_file(filepath: str, file_type: str, batch_date: str, archive_dir: str):
    dest_dir = Path(archive_dir) / file_type / batch_date.replace("-", "")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / Path(filepath).name
    shutil.move(filepath, str(dest))


def process_file(cursor, filepath: str, archive_dir: str) -> dict:
    filename = Path(filepath).name
    file_type = detect_file_type(filename)
    if file_type is None:
        return {"file": filename, "status": "SKIPPED", "reason": "unknown file type"}

    batch_date = extract_batch_date(filename)

    if is_already_processed(cursor, file_type, batch_date, filename):
        # File has already been loaded previously; skip COPY but still
        # move it out of the inbox for cleanliness.
        archive_file(filepath, file_type, batch_date, archive_dir)
        return {
            "file": filename,
            "status": "SKIPPED",
            "reason": "already processed (filename)",
        }

    header = has_header(filepath)

    abs_path = str(Path(filepath).resolve()).replace("\\", "/")
    cursor.execute(
        f"PUT 'file://{abs_path}' @BRONZE.STG_INBOX AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    )

    result = copy_into_bronze(cursor, file_type, filename, batch_date, header)

    row_count = result[3] if result and len(result) > 3 else 0
    cursor.execute(
        """
        INSERT INTO BRONZE.INGESTION_LOG (file_type, batch_date, file_name, content_hash, row_count)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (file_type, batch_date, filename, filename, row_count),
    )
    archive_file(filepath, file_type, batch_date, archive_dir)

    return {
        "file": filename,
        "status": "LOADED",
        "type": file_type,
        "batch_date": batch_date,
        "rows": row_count,
    }


def run(config_path: str):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    inbox = cfg["paths"]["inbox"]
    archive = cfg["paths"]["archive"]

    if not os.path.isdir(inbox):
        raise FileNotFoundError(f"Inbox directory not found: {inbox}")

    files = sorted(
        str(p) for p in Path(inbox).glob("*.csv")
        if detect_file_type(p.name) is not None
    )

    if not files:
        logging.info("No files to process in inbox directory %s", inbox)
        return

    logging.info("Found %d file(s) to process in inbox directory %s", len(files), inbox)

    conn = get_connection(cfg["snowflake"])
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {cfg['snowflake'].get('database', 'SALES_DW')}")
        results = []
        for filepath in files:
            result = process_file(cursor, filepath, archive)
            results.append(result)
            msg = f"{result['file']}: {result['status']}"
            if result["status"] == "LOADED":
                msg += f" ({result.get('rows', 0)} rows)"
            logging.info(msg)

        loaded = sum(1 for r in results if r["status"] == "LOADED")
        skipped = sum(1 for r in results if r["status"] == "SKIPPED")
        logging.info("Done. Loaded: %d file(s), Skipped: %d file(s)", loaded, skipped)
    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest CSV files into Snowflake Bronze")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    args = parser.parse_args()

    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"ingestion_{datetime.now().date().isoformat()}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    run(args.config)


if __name__ == "__main__":
    main()
