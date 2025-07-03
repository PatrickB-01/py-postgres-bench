import csv
import os
import time
from psycopg import sql
from Database.PostgresRepository import PostgresDB


class SequentialInsertBulk:

    def __init__(self, database: PostgresDB, csv_file_path: str):
        self.db = database
        self.file_path = csv_file_path

    def perform_task(self) -> None:
        table_name = os.getenv("TABLE_NAME", "mytable")
        print("Started Sequential Insert")

        try:
            # Optional: Clear existing data
            try:
                with self.db.conn.cursor() as cursor:
                    delete_query = sql.SQL("DELETE FROM {}").format(sql.Identifier(table_name))
                    cursor.execute(delete_query)
                    self.db.conn.commit()
            except Exception as ex:
                print(f"Error during deletion: {ex}")

            # Read and insert CSV data
            with open(self.file_path, mode="r", newline="") as file:
                reader = csv.DictReader(file)
                with self.db.conn.cursor() as cursor:
                    for row in reader:
                        insert_query = sql.SQL("""
                            INSERT INTO {} (id, name, age, email, signup_date)
                            VALUES (%s, %s, %s, %s, %s)
                        """).format(sql.Identifier(table_name))
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
                    self.db.conn.commit()
            print("Data inserted successfully.")
        except Exception as ex:
            print(f"Error during insert: {ex}")


# class SequentialInsert:

#     def __init__(self, database: PostgresDB, csv_file_path: str):
#         self.db = database
#         self.file_path = csv_file_path

#     def perform_task(self) -> None:
#         table_name = os.getenv("TABLE_NAME", "mytable")
#         # Read csv files then start inserting sequentially
#         try:
#             print("Started Sequential Insert")
#             self.db.connect()
#             try:        
#                 with self.conn.cursor() as cursor:
#                         delete_query = sql.SQL(
#                         "DELETE FROM {table}").format(table=sql.SQL(table_name), protocol=sql.Literal(proto))            
#                         cursor.execute(delete_query)
#                         self.conn.commit()
#             except Exception as ex:
#                 print(f"Error: {ex}")

#             # To-Do do your thing here
#             for i in range(5):
#                 # simulating something
#                 time.sleep(1)
#             self.db.close()
#         except Exception as ex:
#             print(str(ex))
#             # self.db.close()
