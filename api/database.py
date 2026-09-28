import os
import psycopg2


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        database=os.getenv("POSTGRES_DB", "pulsestream"),
        user=os.getenv("POSTGRES_USER", "pulsestream"),
        password=os.getenv("POSTGRES_PASSWORD", "changeme"),
    )