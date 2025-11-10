import os
import psycopg2
import click
from flask import current_app, g
from psycopg2.extras import DictCursor, RealDictCursor


def get_db_connection():
    """Gets the raw psycopg2 connection."""
    conn_string = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(conn_string)
    return conn


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
