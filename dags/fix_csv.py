
from pathlib import Path

data_dir = Path(__file__).resolve().parent / "etl_project" / "data"
csv_file = data_dir / "orders.csv"
backup_file = data_dir / "orders.csv.bak"

# Read the original CSV
content = csv_file.read_text(encoding="utf-8-sig")

bad_row = (
    "1008,Nisha Rao,720.00,2026-10-061009,Rahul,1383.93,2026-10-16"
)

correct_rows = (
    "1008,Nisha Rao,720.00,2026-10-06\n"
    "1009,Rahul,1383.93,2026-10-16"
)

# Stop safely if the malformed row is not found exactly once
count = content.count(bad_row)

if count == 0:
    print("Malformed row not found. No changes made.")
elif count > 1:
    print("Multiple matches found. No changes made.")
else:
    # Back up the original CSV before editing it
    backup_file.write_text(content, encoding="utf-8")

    # Repair only the malformed row
    content = content.replace(bad_row, correct_rows, 1)
    csv_file.write_text(content, encoding="utf-8", newline="")

    print("CSV repaired successfully.")
    print(f"Backup saved at: {backup_file}")
