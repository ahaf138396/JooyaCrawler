from crawler.utils.config_loader import load_config


def test_load_config_prefers_environment(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://custom:6379/1")
    monkeypatch.setenv("POSTGRES_URL", "postgresql://user:pass@host:1111/dbname")
    monkeypatch.setenv("MONGO_URL", "mongodb://custom:27018")

    config = load_config()

    assert config.redis_url == "redis://custom:6379/1"
    assert config.postgres_url == "postgresql://user:pass@host:1111/dbname"
    assert config.mongo_url == "mongodb://custom:27018"
