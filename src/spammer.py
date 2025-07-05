import os
import time
from psycopg import sql
from pathlib import Path

from Database.PostgresRepository import PostgresDB

print("=== Spammer.py is running ===")


# def spam_selects(database: PostgresDB, sleep_time: float = 0.01):
#     print("1) SPAMMING..")
#     table_name = os.getenv("TABLE_NAME", "mytable")
#     while True:
#         try:
#             with database.conn.cursor() as cursor:
#                 query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
#                 cursor.execute(query)
#                 _ = cursor.fetchall()  # Or just fetchmany(10) if you want partials
#                 print("SPAMMING..")
#         except Exception as e:
#             print(f"Error during SELECT query: {e}")

#         time.sleep(sleep_time)  # Small delay to avoid 100% CPU

STOP_FLAG_PATH = Path("/app/control/stop_spammer.flag")

def spam_selects(database: PostgresDB, sleep_time: float = 0.01):
    table_name = os.getenv("TABLE_NAME", "mytable")
    print(f"Starting SELECT spam on table '{table_name}'...")
    


    total_time = 0.0
    query_count = 0

    while not STOP_FLAG_PATH.exists():
        try:
            start = time.perf_counter()
            with database.conn.cursor() as cursor:
                query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
                cursor.execute(query)
                _ = cursor.fetchall()
            end = time.perf_counter()

            total_time += (end - start)
            query_count += 1

            time.sleep(sleep_time)
        except Exception as e:
            print(f"Error during SELECT query: {e}", flush=True)
            break

    # Save results and exit
    if query_count > 0:
        avg = total_time / query_count
    else:
        avg = 0

    os.remove(STOP_FLAG_PATH)
    
    with open("/app/control/spammer_result.txt", "a") as f:
        f.write(f"Queries: {query_count}\n")
        f.write(f"Average response time: {avg:.6f} seconds\n")

    print("Spammer received stop signal. Exiting...", flush=True)


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
    open("/app/control/spammer_result.txt", 'w').close()
    print("Spammer started..")
    if (STOP_FLAG_PATH.exists()):
        os.remove(STOP_FLAG_PATH)
    db = wait_for_db()
    spam_selects(db, sleep_time=0.01)
    spam_selects(db, sleep_time=0.01)
    spam_selects(db, sleep_time=0.01)
    spam_selects(db, sleep_time=0.01)
    db.close()
