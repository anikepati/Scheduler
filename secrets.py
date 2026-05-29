"""
Secret resolution — credentials are NEVER stored in Postgres.
auth_secret_ref in api_configs holds the name of an env var
that is injected by OCP via a Kubernetes Secret or ConfigMap.

Resolution order:
  1. Environment variable named exactly auth_secret_ref
  2. File at /var/run/secrets/<auth_secret_ref>  (mounted secret volume)
  3. Raises RuntimeError — fail fast, never silently skip auth
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from logger import get_logger

log = get_logger(__name__)

_SECRETS_MOUNT = Path(os.getenv("SECRETS_MOUNT_PATH", "/var/run/secrets"))


def resolve_secret(secret_ref: str) -> str:
    """
    Resolve a secret reference to its value.
    Raises RuntimeError if the secret cannot be found.
    """
    # 1. Environment variable
    value = os.environ.get(secret_ref)
    if value:
        log.debug("secret_resolved_from_env", ref=secret_ref)
        return value

    # 2. Mounted secret file
    secret_path = _SECRETS_MOUNT / secret_ref
    if secret_path.exists():
        value = secret_path.read_text().strip()
        log.debug("secret_resolved_from_file", ref=secret_ref, path=str(secret_path))
        return value

    raise RuntimeError(
        f"Secret '{secret_ref}' not found in env vars or mounted path "
        f"'{_SECRETS_MOUNT}'. Ensure the OCP Secret is mapped correctly."
    )


def build_auth_headers(auth_type: str, secret_ref: Optional[str]) -> dict[str, str]:
    """
    Build HTTP Authorization headers from auth config.
    Returns empty dict for auth_type='none'.
    """
    if auth_type == "none" or not secret_ref:
        return {}

    secret_value = resolve_secret(secret_ref)

    if auth_type == "bearer":
        return {"Authorization": f"Bearer {secret_value}"}

    if auth_type == "api_key":
        # Convention: secret value is "Header-Name:value"
        # e.g. "X-API-Key:abc123"
        if ":" in secret_value:
            header_name, header_val = secret_value.split(":", 1)
            return {header_name.strip(): header_val.strip()}
        return {"X-API-Key": secret_value}

    if auth_type == "basic":
        # Convention: secret value is "username:password" (plain)
        import base64
        encoded = base64.b64encode(secret_value.encode()).decode()
        return {"Authorization": f"Basic {encoded}"}

    raise ValueError(f"Unknown auth_type: '{auth_type}'")
