import csv
import random
from faker import Faker
from pathlib import Path


def generate_csv(filename, num_rows=100):
    fake = Faker()
    with open(filename, mode="w", newline="") as csvfile:
        fieldnames = ["id", "name", "age", "email", "signup_date"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(1, num_rows + 1):
            writer.writerow(
                {
                    "id": i,
                    "name": fake.name(),
                    "age": fake.random_number(),
                    "email": fake.email(),
                    "signup_date": fake.date_this_decade().isoformat(),
                }
            )


# To-Do have the number of rows from env
DATA = Path.cwd() / "Data" / "data.csv"
generate_csv(DATA, num_rows=100000000)
