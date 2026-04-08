from __future__ import annotations

import os
from dataclasses import dataclass

from passlib.context import CryptContext

from app.common.config import AppConfig


# Keep passlib dependency with bcrypt extra installed, but use pbkdf2_sha256 as default
# to avoid bcrypt backend issues on some Python/bcrypt combinations.
PWD_CONTEXT = CryptContext(schemes=["pbkdf2_sha256", "bcrypt"], deprecated="auto")
SESSION_USER_KEY = "user"


@dataclass(slots=True)
class AuthManager:
    username: str
    password_hash: str
    secret_key: str

    @property
    def is_configured(self) -> bool:
        return bool(self.username and self.password_hash and self.secret_key)

    def verify(self, username: str, password: str) -> bool:
        if not self.is_configured:
            return False
        if username != self.username:
            return False
        return PWD_CONTEXT.verify(password or "", self.password_hash)


def build_auth_manager(config: AppConfig) -> AuthManager:
    webui_cfg = config.raw.get("webui", {})

    username = str(webui_cfg.get("admin_username", "admin")).strip()

    password_value = str(webui_cfg.get("admin_password", "")).strip()
    if not password_value:
        env_name = str(webui_cfg.get("admin_password_env", "WEBUI_ADMIN_PASSWORD")).strip()
        password_value = os.getenv(env_name, "").strip()

    identified = PWD_CONTEXT.identify(password_value or "")
    if password_value and identified and not str(password_value).startswith("$2"):
        password_hash = password_value
    elif password_value:
        password_hash = PWD_CONTEXT.hash(password_value)
    else:
        password_hash = ""

    secret_key = str(webui_cfg.get("secret_key", "")).strip()
    if not secret_key:
        env_name = str(webui_cfg.get("secret_key_env", "WEBUI_SECRET_KEY")).strip()
        secret_key = os.getenv(env_name, "").strip()
    if not secret_key:
        secret_key = "change-me-webui-secret"

    return AuthManager(username=username, password_hash=password_hash, secret_key=secret_key)

