from pathlib import Path

from app.common.config import load_config
from app.common.state_db import StateDB


def test_doctor_paths_and_db_init() -> None:
    config = load_config("config/config.yaml")
    db = StateDB(config.paths.SQLITE_DB)
    db.init_schema()
    assert Path(config.paths.SQLITE_DB).exists()
