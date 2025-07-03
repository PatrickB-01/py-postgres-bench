import csv
import os
import threading
from psycopg import sql, connect
from Database.PostgresRepository import PostgresDB


class MultithreadedInsert:
    def __init__(self, csv_file_path: str, num_threads: int = 4):
        self.file_path = csv_file_path
        self.num_threads = num_threads
        self.rows = []
        self.table_name = os.getenv("TABLE_NAME", "mytable")
        

    def _load_data(self):
        with open(self.file_path, mode="r", newline="") as file:
            reader = csv.DictReader(file)
            self.rows = [row for row in reader]

    def _insert_batch(self, batch_rows):
        try:
            db = PostgresDB()
            with db.conn.cursor() as cursor:
                insert_query = sql.SQL("""
                    INSERT INTO {} (id, name, age, email, signup_date)
                    VALUES (%s, %s, %s, %s, %s)
                """).format(sql.Identifier(self.table_name))

                for row in batch_rows:
                    cursor.execute(
                        insert_query,
                        (
                            int(row["id"]),
                            row["name"],
                            int(row["age"]),
                            row["email"],
                            row["signup_date"],
                        )
                    )
                db.conn.commit()
            db.conn.close()
        except Exception as e:
            print(f"[Thread {threading.get_ident()}] Error inserting batch: {e}")

    def perform_task(self):
        print("Started Multithreaded Insert")
        self._load_data()

        db = PostgresDB()
        # Optional: Clear existing data
        try:
            with db.conn.cursor() as cursor:
                delete_query = sql.SQL("DELETE FROM {}").format(sql.Identifier(self.table_name))
                cursor.execute(delete_query)
                db.conn.commit()
                db.conn.close()
        except Exception as ex:
            print(f"Error during deletion: {ex}")
            db.conn.close()

        batch_size = len(self.rows) // self.num_threads
        print("batch_size = ", batch_size)
        threads = []

        for i in range(self.num_threads):
            start = i * batch_size
            end = None if i == self.num_threads - 1 else (i + 1) * batch_size
            print("start = ", start)
            print("end = ", end)
            batch = self.rows[start:end]
            t = threading.Thread(target=self._insert_batch, args=(batch,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        print("Multithreaded data insert completed.")
