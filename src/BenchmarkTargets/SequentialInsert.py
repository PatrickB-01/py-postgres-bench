import psycopg
from psycopg.rows import dict_row
from Database.PostgresRepository import PostgresDB
import time


class SequentialInsert:

    def __init__(self, database: PostgresDB):
        self.db = database

    def perform_task(self) -> None:
        # Read csv files then start inserting sequentially
        try:
            print("Started Sequential Insert")
            # self.db.connect()

            # To-Do do your thing here
            for i in range(5):
                # simulating something
                time.sleep(1)
            # self.db.close()
        except Exception as ex:
            print(str(ex))
            # self.db.close()
