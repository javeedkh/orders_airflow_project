
import csv
import random
from datetime import date
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
SOURCE_FILE = DATA_DIR / "orders.csv"

CUSTOMERS = ["Rahul", "Priya", "Arjun", "Sneha", "Aisha"]


def generate_orders():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    columns = ["order_id", "customer_name", "amount", "order_date"]

    # Create the CSV if it does not exist.
    if not SOURCE_FILE.exists():
        with open(SOURCE_FILE, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()

    # Find the highest existing order ID.
    last_id = 0

    with open(SOURCE_FILE, "r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        for row in reader:
            try:
                last_id = max(last_id, int(row["order_id"]))
            except (ValueError, TypeError, KeyError):
                pass

    # Generate three new orders.
    
    # Generate three new orders and print each one in Airflow logs.
    with open(SOURCE_FILE, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)

        for order_id in range(last_id + 1, last_id + 4):
            order = {
                "order_id": order_id,
                "customer_name": random.choice(CUSTOMERS),
                "amount": round(random.uniform(100, 5000), 2),
                "order_date": date.today().isoformat()
            }

            writer.writerow(order)
            print(f"GENERATED ORDER: {order}", flush=True)

    print(f"Generated orders {last_id + 1} to {last_id + 3}", flush=True)


if __name__ == "__main__":
    generate_orders()
