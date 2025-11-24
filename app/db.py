import logging
import os
import time
from urllib.parse import urlparse

import psycopg2
from flask import g
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError as SAOperationalError

logger = logging.getLogger(__name__)
_engine = None


def _int_env(name, default):
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def _describe_target(conn_string):
    if conn_string.startswith(("postgres://", "postgresql://")):
        parsed = urlparse(conn_string)
        host = parsed.hostname or "localhost"
        db_name = (parsed.path or "").lstrip("/") or "database"
        return f"postgresql://{host}/{db_name}"
    return "configured PostgreSQL database"


def _get_engine(conn_string):
    global _engine
    if _engine is not None:
        return _engine

    pool_size = _int_env("DB_POOL_SIZE", 5)
    max_overflow = _int_env("DB_POOL_MAX_OVERFLOW", 5)
    pool_recycle = _int_env("DB_POOL_RECYCLE_SECONDS", 1800)
    connect_timeout = _int_env("DB_CONNECT_TIMEOUT_SECONDS", 10)

    _engine = create_engine(
        conn_string,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,
        pool_recycle=pool_recycle,
        connect_args={"connect_timeout": connect_timeout},
    )
    return _engine


def get_db_connection():
    """Gets the raw psycopg2 connection."""
    conn_string = os.getenv("DATABASE_URL")

    if not conn_string:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set; cannot open a connection."
        )

    safe_target = _describe_target(conn_string)
    last_exception = None
    max_attempts = 3
    engine = _get_engine(conn_string)

    for attempt in range(1, max_attempts + 1):
        try:
            return engine.raw_connection()
        except (psycopg2.OperationalError, SAOperationalError) as exc:
            last_exception = exc
            logger.warning(
                "Database connection attempt %s/%s to %s failed: %s",
                attempt,
                max_attempts,
                safe_target,
                exc,
            )
            if attempt < max_attempts:
                time.sleep(0.25 * attempt)

    raise RuntimeError(f"Could not connect to {safe_target}") from last_exception


def get_db():
    """
    Opens a new database connection if one is not already open
    for the current request.
    """
    if "db" not in g:
        g.db = get_db_connection()
    return g.db


def close_db(e=None):
    """
    Closes the database connection at the end of the request.
    """
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_app(app):
    """
    Register database functions with the Flask app. This is called by
    the application factory.
    """
    app.teardown_appcontext(close_db)
