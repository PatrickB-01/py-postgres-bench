import psycopg
from psycopg.rows import dict_row
import os
from dotenv import load_dotenv


class PostgresDB:
    def __init__(self):
        load_dotenv()
        self.conn_info = {
            "dbname": os.getenv("DB_NAME", "default_db"),
            "user": os.getenv("DB_USER", "default_user"),
            "password": os.getenv("DB_PASSWORD", "default_pass"),
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 5432)),
        }
        self.conn = None

    def connect(self):
        self.conn = psycopg.connect(**self.conn_info, row_factory=dict_row)
        print("Connected to PostgreSQL database.")

    def close(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")
