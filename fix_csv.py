from pathlib import Path

data_dir = Path(__file__).resolve().parent / "etl_project" / "data"
csv_file = data_dir / "orders.csv"
backup_file = data_dir / "orders.csv.bak"

if not csv_file.exists():
    print(f"CSV file not found: {csv_file}")
    raise SystemExit(1)

content = csv_file.read_text(encoding="utf-8-sig")

bad_row = "1008,Nisha Rao,720.00,2026-10-061009,Rahul,1383.93,2026-10-16"
correct_rows = (
    "1008,Nisha Rao,720.00,2026-10-06\n"
    "1009,Rahul,1383.93,2026-10-16"
)

count = content.count(bad_row)

if count == 0:
    print("Exact malformed row not found. No changes made.")
elif count > 1:
    print("Multiple matching rows found. No changes made.")
else:
    backup_file.write_text(content, encoding="utf-8")
    content = content.replace(bad_row, correct_rows, 1)
    csv_file.write_text(content, encoding="utf-8", newline="")
    print("CSV repaired successfully.")
    print(f"Backup created: {backup_file}")
