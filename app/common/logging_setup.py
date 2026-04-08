from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler

from .config import AppConfig


class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ["run_id", "pipeline", "component", "status", "task"]:
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(config: AppConfig) -> None:
    root = logging.getLogger()
    if root.handlers:
        return

    level = logging.DEBUG if config.runtime.debug else logging.INFO
    root.setLevel(level)

    text_formatter = logging.Formatter(
        fmt="%(asctime)sZ | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    json_formatter = JsonLineFormatter()

    stream = logging.StreamHandler()
    stream.setFormatter(text_formatter)
    root.addHandler(stream)

    app_log = config.paths.LOG_DIR / "app.log"
    app_handler = RotatingFileHandler(app_log, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    app_handler.setFormatter(text_formatter)
    root.addHandler(app_handler)

    json_log = config.paths.LOG_DIR / "json.log"
    json_handler = RotatingFileHandler(json_log, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    json_handler.setFormatter(json_formatter)
    root.addHandler(json_handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
