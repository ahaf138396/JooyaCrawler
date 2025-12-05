import os
import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from crawler.utils.env_loader import load_environment


@pytest.fixture(autouse=True)
def load_dotenv_defaults(monkeypatch):
    """Ensure .env defaults are available for every test."""

    # Clear key variables so tests always use the .env baseline unless they
    # explicitly override values via monkeypatch or a custom env file.
    for key in [
        "REDIS_URL",
        "POSTGRES_URL",
        "DATABASE_URL",
        "RADAR_DATABASE_URL",
        "MONGO_URL",
        "MONGO_URI",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "WORKERS",
    ]:
        monkeypatch.delenv(key, raising=False)

    load_environment(override=True)

    yield

    # Clean up to avoid leaking state between tests.
    for key in list(os.environ.keys()):
        if key.startswith("TEST_"):
            monkeypatch.delenv(key, raising=False)
