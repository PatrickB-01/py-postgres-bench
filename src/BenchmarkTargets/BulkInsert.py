import os
from Database.PostgresRepository import PostgresDB


class BulkInsert:
    def __init__(self, database: PostgresDB, csv_file_path: str):
        self.db = database
        self.file_path = csv_file_path

    def perform_task(self) -> None:
        table_name = os.getenv("TABLE_NAME", "mytable")
        print("Started Bulk Insert")

        try:
            # Optional: Clear existing data
            try:
                with self.db.conn.cursor() as cursor:
                    cursor.execute(f"DELETE FROM {table_name}")
                self.db.conn.commit()
            except Exception as ex:
                print(f"Error during deletion: {ex}")

            # Perform bulk insert using psycopg3's copy
            with self.db.conn.cursor() as cur:
                with open(self.file_path, "r", newline="") as f:
                    # Skip header
                    next(f)

                    with cur.copy(f"COPY {table_name} (id, name, age, email, signup_date) FROM STDIN WITH (FORMAT CSV)") as copy:
                        for line in f:
                            copy.write(line)
                self.db.conn.commit()

            print("Bulk data inserted successfully.")

        except Exception as ex:
            print(f"Error during bulk insert: {ex}")
