import pytest
from ingestion.validate import (
    detect_file_type,
    extract_batch_date,
    has_header,
    compute_file_hash,
)


def test_detect_file_type_stores():
    assert detect_file_type("stores_20211001.csv") == "stores"


def test_detect_file_type_sales():
    assert detect_file_type("sales_20211001.csv") == "sales"


def test_detect_file_type_unknown():
    assert detect_file_type("other_20211001.csv") is None


def test_detect_file_type_nested_path():
    assert detect_file_type("inbox/sales_20211001.csv") == "sales"


def test_extract_batch_date():
    assert extract_batch_date("stores_20211001.csv") == "2021-10-01"


def test_extract_batch_date_sales():
    assert extract_batch_date("sales_20211215.csv") == "2021-12-15"


def test_extract_batch_date_invalid():
    with pytest.raises(ValueError):
        extract_batch_date("unknown_file.csv")


def test_has_header_true_stores(tmp_path):
    f = tmp_path / "test.csv"
    f.write_text("store_group,store_token,store_name\n00001A4A,abc,Store 1\n")
    assert has_header(str(f)) is True


def test_has_header_true_sales(tmp_path):
    f = tmp_path / "test.csv"
    f.write_text("store_token,transaction_id,receipt_token,transaction_time,amount,source_id,user_role\nabc,def,ghi,123,456,789,Cashier\n")
    assert has_header(str(f)) is True


def test_has_header_false(tmp_path):
    f = tmp_path / "test.csv"
    f.write_text("00001A4A,00001a4a-786e-49de-b9cf-2a3f06a1fad9,Store 1\n")
    assert has_header(str(f)) is False


def test_compute_file_hash_deterministic(tmp_path):
    f = tmp_path / "test.csv"
    f.write_text("some,data,here\n")
    h1 = compute_file_hash(str(f))
    h2 = compute_file_hash(str(f))
    assert h1 == h2
    assert len(h1) == 64


def test_compute_file_hash_different_content(tmp_path):
    f1 = tmp_path / "a.csv"
    f2 = tmp_path / "b.csv"
    f1.write_text("data1\n")
    f2.write_text("data2\n")
    assert compute_file_hash(str(f1)) != compute_file_hash(str(f2))
