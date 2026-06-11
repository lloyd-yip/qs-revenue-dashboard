"""Central SMTP credential loader for QS report emails.

Loads the Gmail SMTP creds from the environment, falling back to a gitignored
`.env` file in this directory. NEVER hardcode the app password in source — it
ends up committed to the repo and its git history.

Set these in qs-dashboard/.env (gitignored — never commit):
    QS_SMTP_USER=lloyd@quantum-scaling.com
    QS_SMTP_PASS=<gmail app password>
"""
import os
from pathlib import Path


def _load_env_file() -> None:
    """Load KEY=VALUE pairs from ./.env into os.environ (real env vars win)."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        os.environ.setdefault(key.strip(), val.strip())


_load_env_file()

SMTP_USER = os.environ.get("QS_SMTP_USER", "").strip()
SMTP_PASS = os.environ.get("QS_SMTP_PASS", "").strip()

if not SMTP_USER or not SMTP_PASS:
    raise RuntimeError(
        "QS_SMTP_USER / QS_SMTP_PASS are not set. Add them to qs-dashboard/.env "
        "(gitignored) — never hardcode the Gmail app password in source."
    )
