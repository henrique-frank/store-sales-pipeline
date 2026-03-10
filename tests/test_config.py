import pytest
from ingestion.config import load_config


def test_load_config_returns_expected_keys(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(
        """
snowflake:
  account: test_account
  user: test_user
  password: test_pass
  database: SALES_DW
  warehouse: COMPUTE_WH
paths:
  inbox: ./inbox
  archive: ./archive
limits:
  batch_dates: 40
  transaction_dates: 40
  top_dates: 10
"""
    )
    cfg = load_config(str(cfg_file))
    assert cfg["snowflake"]["account"] == "test_account"
    assert cfg["paths"]["inbox"] == "./inbox"
    assert cfg["limits"]["batch_dates"] == 40


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.yaml")
