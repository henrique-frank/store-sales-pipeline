"""
Store Sales Pipeline — CSV Ingestion to Snowflake Bronze

Usage:
    python -m ingestion.ingest --config config/config.yaml
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

import snowflake.connector

from ingestion.config import load_config
from ingestion.validate import (
    compute_file_hash,
    detect_file_type,
    extract_batch_date,
    has_header,
)

STORES_COLUMNS = 3
SALES_COLUMNS = 7


def get_connection(sf_cfg: dict):
    return snowflake.connector.connect(
        account=sf_cfg["account"],
        user=sf_cfg["user"],
        password=sf_cfg["password"],
        database=sf_cfg.get("database", "SALES_DW"),
        warehouse=sf_cfg.get("warehouse", "COMPUTE_WH"),
        role=sf_cfg.get("role", "SYSADMIN"),
    )


def is_already_processed(cursor, content_hash: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM BRONZE.INGESTION_LOG WHERE content_hash = %s",
        (content_hash,),
    )
    return cursor.fetchone()[0] > 0


def put_file(cursor, filepath: str):
    cursor.execute(f"PUT 'file://{filepath}' @BRONZE.STG_INBOX AUTO_COMPRESS=FALSE OVERWRITE=TRUE")


def copy_stores(cursor, file_name: str, batch_date: str, skip_header: bool):
    skip = 1 if skip_header else 0
    sql = f"""
        COPY INTO BRONZE.STORES_RAW (store_group, store_token, store_name, batch_date, file_name)
        FROM (
            SELECT $1, $2, $3,
                   '{batch_date}'::DATE,
                   '{file_name}'
            FROM @BRONZE.STG_INBOX/{file_name}
        )
        FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV SKIP_HEADER = {skip})
        ON_ERROR = 'CONTINUE'
    """
    cursor.execute(sql)
    return cursor.fetchone()


def copy_sales(cursor, file_name: str, batch_date: str, skip_header: bool):
    skip = 1 if skip_header else 0
    sql = f"""
        COPY INTO BRONZE.SALES_RAW (
            store_token, transaction_id, receipt_token,
            transaction_time, amount, source_id, user_role,
            batch_date, file_name
        )
        FROM (
            SELECT $1, $2, $3, $4, $5, $6, $7,
                   '{batch_date}'::DATE,
                   '{file_name}'
            FROM @BRONZE.STG_INBOX/{file_name}
        )
        FILE_FORMAT = (FORMAT_NAME = BRONZE.FF_CSV SKIP_HEADER = {skip})
        ON_ERROR = 'CONTINUE'
    """
    cursor.execute(sql)
    return cursor.fetchone()


def log_ingestion(cursor, file_type: str, batch_date: str, file_name: str,
                  content_hash: str, row_count: int):
    cursor.execute(
        """
        INSERT INTO BRONZE.INGESTION_LOG (file_type, batch_date, file_name, content_hash, row_count)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (file_type, batch_date, file_name, content_hash, row_count),
    )


def archive_file(filepath: str, file_type: str, batch_date: str, archive_dir: str):
    dest_dir = Path(archive_dir) / file_type / batch_date.replace("-", "")
    dest_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(filepath, str(dest_dir / Path(filepath).name))


def process_file(cursor, filepath: str, archive_dir: str) -> dict:
    filename = Path(filepath).name
    file_type = detect_file_type(filename)
    if file_type is None:
        return {"file": filename, "status": "SKIPPED", "reason": "unknown file type"}

    batch_date = extract_batch_date(filename)
    content_hash = compute_file_hash(filepath)

    if is_already_processed(cursor, content_hash):
        return {"file": filename, "status": "SKIPPED", "reason": "already processed"}

    header = has_header(filepath)

    abs_path = str(Path(filepath).resolve()).replace("\\", "/")
    put_file(cursor, abs_path)

    if file_type == "stores":
        result = copy_stores(cursor, filename, batch_date, header)
    else:
        result = copy_sales(cursor, filename, batch_date, header)

    row_count = result[3] if result and len(result) > 3 else 0
    log_ingestion(cursor, file_type, batch_date, filename, content_hash, row_count)
    archive_file(filepath, file_type, batch_date, archive_dir)

    return {
        "file": filename,
        "status": "LOADED",
        "type": file_type,
        "batch_date": batch_date,
        "rows": row_count,
    }


def run(config_path: str):
    cfg = load_config(config_path)
    inbox = cfg["paths"]["inbox"]
    archive = cfg["paths"]["archive"]

    if not os.path.isdir(inbox):
        print(f"Inbox directory not found: {inbox}")
        sys.exit(1)

    files = sorted(
        str(p) for p in Path(inbox).glob("*.csv")
        if detect_file_type(p.name) is not None
    )

    if not files:
        print("No files to process.")
        return

    print(f"Found {len(files)} file(s) to process.")

    conn = get_connection(cfg["snowflake"])
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE DATABASE {cfg['snowflake'].get('database', 'SALES_DW')}")
        results = []
        for filepath in files:
            result = process_file(cursor, filepath, archive)
            results.append(result)
            print(f"  {result['file']}: {result['status']}"
                  + (f" ({result.get('rows', 0)} rows)" if result['status'] == 'LOADED' else ""))

        loaded = sum(1 for r in results if r["status"] == "LOADED")
        skipped = sum(1 for r in results if r["status"] == "SKIPPED")
        print(f"\nDone. Loaded: {loaded}, Skipped: {skipped}")
    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest CSV files into Snowflake Bronze")
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
