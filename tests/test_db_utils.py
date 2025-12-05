from crawler.utils.db_utils import to_asyncpg_dsn, to_postgres_dsn


def test_to_postgres_dsn_strips_driver_prefix():
    assert (
        to_postgres_dsn("postgresql+psycopg2://user:pass@host:5432/db")
        == "postgresql://user:pass@host:5432/db"
    )
    assert to_postgres_dsn("asyncpg://user:pw@host/db") == "postgresql://user:pw@host/db"


def test_to_asyncpg_dsn_normalizes_postgres_variants():
    assert (
        to_asyncpg_dsn("postgresql://user:pass@host:5432/db")
        == "asyncpg://user:pass@host:5432/db"
    )
    assert (
        to_asyncpg_dsn("postgres://user:pass@host:5432/db")
        == "asyncpg://user:pass@host:5432/db"
    )
