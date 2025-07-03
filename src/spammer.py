import os
import time
from psycopg import sql

from Database.PostgresRepository import PostgresDB

print("=== Spammer.py is running ===")


def spam_selects(database: PostgresDB, sleep_time: float = 0.01):
    print("1) SPAMMING..")
    table_name = os.getenv("TABLE_NAME", "mytable")
    while True:
        try:
            with database.conn.cursor() as cursor:
                query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
                cursor.execute(query)
                _ = cursor.fetchall()  # Or just fetchmany(10) if you want partials
                print("SPAMMING..")
        except Exception as e:
            print(f"Error during SELECT query: {e}")

        time.sleep(sleep_time)  # Small delay to avoid 100% CPU

def wait_for_db(max_retries=20, delay=1.0):
    for attempt in range(max_retries):
        try:
            db = PostgresDB()
            print("Database connection successful.", flush=True)
            return db
        except Exception as ex:
            print(f"[Attempt {attempt+1}] Database connection failed: {ex}", flush=True)
            time.sleep(delay)
    raise Exception("Could not connect to database after multiple attempts.")


if __name__ == "__main__":
    print("Spammer started..")
    db = wait_for_db()
    spam_selects(db, sleep_time=0.01)  # Adjust sleep_time as needed
    db.close()
