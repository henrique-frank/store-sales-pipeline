"""Generate light and heavy sample datasets for pipeline testing."""

import csv
import os
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

STORE_COUNT_LIGHT = 10
STORE_COUNT_HEAVY = 500
# Light dataset total sales across all days; enough per day so we
# always have valid + duplicate + invalid rows to exercise the logic.
SALES_LIGHT = 500
SALES_HEAVY = 100_000
# Use 50 days in the *light* dataset so we can clearly show
# the "latest 40 batch dates" behavior in Output 1 without
# needing separate sample/heavy runs.
BATCH_DAYS_LIGHT = 50
BATCH_DAYS_HEAVY = 50
BASE_DATE = datetime(2024, 10, 1)
ROLES = ["Cashier", "Manager", "Supervisor", "Clerk", "Admin"]


def make_stores(n):
    stores = []
    for i in range(n):
        group = f"{random.randint(0, 0xFFFFFFFF):08X}"
        token = str(uuid.uuid4())
        name = f"Store {i+1:05d}"
        stores.append((group, token, name))
    return stores


def write_stores_csv(path, stores, with_header):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if with_header:
            w.writerow(["store_group", "store_token", "store_name"])
        for s in stores:
            w.writerow(s)


def write_sales_csv(path, rows, with_header):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if with_header:
            w.writerow([
                "store_token", "transaction_id", "receipt_token",
                "transaction_time", "amount", "user_role",
            ])
        for r in rows:
            w.writerow(r)


def random_receipt(length=None):
    length = length or random.randint(5, 20)
    chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(random.choice(chars) for _ in range(length))


def gen_sales_rows(stores, count, batch_date):
    rows = []
    seen_ids = set()
    duplicates = max(1, count // 100)
    invalid = max(1, count // 200)

    for _ in range(count - duplicates - invalid):
        store = random.choice(stores)
        tx_id = str(uuid.uuid4())
        seen_ids.add((store[1], tx_id))
        receipt = random_receipt()
        hour = random.randint(6, 22)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        ts = batch_date.replace(hour=hour, minute=minute, second=second)
        tx_time = ts.strftime("%Y%m%dT%H%M%S.000")
        amount = f"${random.uniform(1, 999.99):.2f}"
        role = random.choice(ROLES)
        rows.append((store[1], tx_id, receipt, tx_time, amount, role))

    dup_source = random.sample(rows, min(duplicates, len(rows)))
    for orig in dup_source:
        new_amount = f"${random.uniform(1, 999.99):.2f}"
        rows.append((orig[0], orig[1], orig[2], orig[3], new_amount, orig[5]))

    for _ in range(invalid):
        rows.append((
            "INVALID_TOKEN",
            "not-a-uuid",
            "AB",
            "NOT_A_TIMESTAMP",
            "NOT_MONEY",
            "X" * 40,
        ))

    random.shuffle(rows)
    return rows


def generate_light():
    out = "data/light"
    stores = make_stores(STORE_COUNT_LIGHT)

    for day in range(BATCH_DAYS_LIGHT):
        bd = BASE_DATE + timedelta(days=day)
        ds = bd.strftime("%Y%m%d")
        with_header = day % 2 == 0

        write_stores_csv(f"{out}/stores_{ds}.csv", stores, with_header)

        rows_per_day = SALES_LIGHT // BATCH_DAYS_LIGHT
        sales = gen_sales_rows(stores, rows_per_day, bd)
        write_sales_csv(f"{out}/sales_{ds}.csv", sales, with_header)

        # On one specific day, also generate a second sales file with
        # overlapping transactions and different amounts to simulate
        # "latest received" duplicates across files for the same batch_date.
        if day == BATCH_DAYS_LIGHT - 1:
            sales_v2 = []
            for i, row in enumerate(sales):
                store_token, tx_id, receipt, tx_time, amount, role = row
                if i % 2 == 0:
                    # Same transaction_id but with a different amount
                    new_amount = f"${random.uniform(1, 999.99):.2f}"
                    sales_v2.append((store_token, tx_id, receipt, tx_time, new_amount, role))
                else:
                    # Keep some rows identical
                    sales_v2.append(row)
            write_sales_csv(f"{out}/sales_{ds}_2.csv", sales_v2, with_header)

            # And generate a second stores file with renamed stores to
            # exercise SCD Type 2 on dim_store for the same batch_date.
            stores_v2 = []
            for i, (group, token, name) in enumerate(stores):
                if i < 3:
                    stores_v2.append((group, token, f"{name} Renamed"))
                else:
                    stores_v2.append((group, token, name))
            write_stores_csv(f"{out}/stores_{ds}_v2.csv", stores_v2, with_header)

    print(f"Light dataset: {STORE_COUNT_LIGHT} stores, ~{SALES_LIGHT} sales, {BATCH_DAYS_LIGHT} days -> {out}/")


def generate_heavy():
    out = "data/heavy"
    stores = make_stores(STORE_COUNT_HEAVY)

    for day in range(BATCH_DAYS_HEAVY):
        bd = BASE_DATE + timedelta(days=day)
        ds = bd.strftime("%Y%m%d")
        with_header = day % 2 == 0

        write_stores_csv(f"{out}/stores_{ds}.csv", stores, with_header)

        rows_per_day = SALES_HEAVY // BATCH_DAYS_HEAVY
        sales = gen_sales_rows(stores, rows_per_day, bd)
        write_sales_csv(f"{out}/sales_{ds}.csv", sales, with_header)

    print(f"Heavy dataset: {STORE_COUNT_HEAVY} stores, ~{SALES_HEAVY} sales, {BATCH_DAYS_HEAVY} days -> {out}/")


if __name__ == "__main__":
    # Unified approach: for the assessment demo we only need
    # the light dataset with 50 days (stores + sales).
    generate_light()
    print("Done (light dataset only).")
