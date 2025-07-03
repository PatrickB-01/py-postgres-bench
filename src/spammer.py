import os
import time
from psycopg import sql

from Database.PostgresRepository import PostgresDB

print("=== Spammer.py is running ===")


def spam_selects(database: PostgresDB, sleep_time: float = 0.01):
    table_name = os.getenv("TABLE_NAME", "mytable")
    print(f"Starting SELECT spam on table '{table_name}'...")
    
    while True:
        try:
            with database.conn.cursor() as cursor:
                query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
                cursor.execute(query)
                _ = cursor.fetchall()  # Or just fetchmany(10) if you want partials
        except Exception as e:
            print(f"Error during SELECT query: {e}")
            break

        time.sleep(sleep_time)  # Small delay to avoid 100% CPU


if __name__ == "__main__":
    print("Spammer started..")
    db = PostgresDB()
    spam_selects(db, sleep_time=0.01)  # Adjust sleep_time as needed
    db.close()
