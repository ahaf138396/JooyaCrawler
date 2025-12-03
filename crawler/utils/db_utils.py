"""Utility helpers for database connection strings.

These helpers normalize PostgreSQL DSNs coming from different drivers so that
they can be reused by `asyncpg` (for direct access) and Tortoise ORM
(`asyncpg://` scheme).
"""

from __future__ import annotations


def to_postgres_dsn(url: str) -> str:
    """Normalize a SQLAlchemy-style URL into a plain PostgreSQL DSN.

    The Radar service uses ``postgresql+psycopg2://`` while ``asyncpg`` expects
    ``postgresql://`` (or ``postgres://``). This helper strips the driver part
    if present and also converts ``asyncpg://`` back to ``postgresql://`` when
    needed.
    """

    if url.startswith("postgresql+"):
        return "postgresql://" + url.split("://", 1)[1]
    if url.startswith("asyncpg://"):
        return "postgresql://" + url[len("asyncpg://") :]
    return url


def to_asyncpg_dsn(url: str) -> str:
    """Convert a PostgreSQL DSN to the ``asyncpg://`` scheme for Tortoise."""

    if url.startswith("postgresql+"):
        url = "postgresql://" + url.split("://", 1)[1]
    if url.startswith("postgresql://"):
        return "asyncpg://" + url[len("postgresql://") :]
    if url.startswith("postgres://"):
        return "asyncpg://" + url[len("postgres://") :]
    return url
