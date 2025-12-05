import os

from crawler.utils.env_loader import load_environment


def test_load_environment_from_custom_file(monkeypatch, tmp_path):
    env_file = tmp_path / "custom.env"
    env_file.write_text("CUSTOM_URL=https://example.com\nWORKERS=4\n")

    monkeypatch.delenv("CUSTOM_URL", raising=False)
    monkeypatch.delenv("WORKERS", raising=False)

    loaded = load_environment(env_file, override=True)

    assert loaded is True
    assert os.getenv("CUSTOM_URL") == "https://example.com"
    assert os.getenv("WORKERS") == "4"


def test_load_environment_missing_file(tmp_path):
    missing_file = tmp_path / "missing.env"

    loaded = load_environment(missing_file)

    assert loaded is False
