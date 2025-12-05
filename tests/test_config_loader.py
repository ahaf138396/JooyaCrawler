from crawler.utils.config_loader import get_crawler_user_agent, load_config


def test_load_config_prefers_environment(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://custom:6379/1")
    monkeypatch.setenv("POSTGRES_URL", "postgresql://user:pass@host:1111/dbname")
    monkeypatch.setenv("MONGO_URL", "mongodb://custom:27018")

    config = load_config()

    assert config.redis_url == "redis://custom:6379/1"
    assert config.postgres_url == "postgresql://user:pass@host:1111/dbname"
    assert config.mongo_url == "mongodb://custom:27018"


def test_get_crawler_user_agent_prefers_environment(monkeypatch):
    monkeypatch.setenv("CRAWLER_USER_AGENT", "CustomBot/2.0")

    assert get_crawler_user_agent() == "CustomBot/2.0"


def test_get_crawler_user_agent_falls_back_to_config_file(monkeypatch):
    monkeypatch.delenv("CRAWLER_USER_AGENT", raising=False)

    assert get_crawler_user_agent() == "JooyaBot/1.0"
