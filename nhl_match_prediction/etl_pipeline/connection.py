import os

import psycopg2
from sqlalchemy import create_engine

DB_URL = os.environ["DATABASE_URL"]


def get_engine():
    return create_engine(DB_URL)


def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )
