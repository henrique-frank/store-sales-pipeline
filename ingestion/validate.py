import hashlib
import re
from pathlib import Path

KNOWN_HEADERS = {
    "stores": {"store_group", "store_token", "store_name"},
    "sales": {
        "store_token", "transaction_id", "receipt_token",
        "transaction_time", "amount", "user_role", "source_id",
    },
}


def detect_file_type(filename: str) -> str | None:
    name = Path(filename).name
    if name.startswith("stores_"):
        return "stores"
    if name.startswith("sales_"):
        return "sales"
    return None


def extract_batch_date(filename: str) -> str:
    name = Path(filename).stem
    match = re.search(r"(\d{8})$", name)
    if not match:
        raise ValueError(f"Cannot extract batch_date from {filename}")
    d = match.group(1)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


def has_header(filepath: str) -> bool:
    with open(filepath, "r", encoding="utf-8") as f:
        first_line = f.readline().strip().lower()
    fields = {col.strip() for col in first_line.split(",")}
    for header_set in KNOWN_HEADERS.values():
        if len(fields & header_set) >= 2:
            return True
    return False


def compute_file_hash(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
