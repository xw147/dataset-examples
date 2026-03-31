"""
Update the SQLite table 'user_write_real_chinese_review' with
chinese_countryOrigin values from the CSV file.

Matches rows on the 'name' column. Adds the 'chinese_countryOrigin' column
to the table if it does not already exist.
"""

import csv
import os
import sqlite3

# ── File paths ──────────────────────────────────────────────────────────────
CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..",
    "yelp_dataset", "unique_user_names_preprocessed_filtered_origin_chinese_reclassified.csv"
)
DB_PATH = os.path.join(
    os.path.dirname(__file__), "..",
    "yelp_dataset", "full_yelp.db"
)
TABLE_NAME = "user_write_real_chinese_review"


def main():
    # ── 1. Read CSV ─────────────────────────────────────────────────────────
    name_to_chinese = {}
    with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["name"]
            value = row.get("chinese_countryOrigin", "")
            name_to_chinese[name] = value

    print(f"Loaded {len(name_to_chinese)} names from CSV")

    # ── 2. Connect to SQLite ────────────────────────────────────────────────
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ── 3. Add column if it doesn't exist ───────────────────────────────────
    cur.execute(f"PRAGMA table_info({TABLE_NAME})")
    existing_cols = {row[1] for row in cur.fetchall()}
    if "chinese_countryOrigin" not in existing_cols:
        cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN chinese_countryOrigin TEXT")
        print("Added 'chinese_countryOrigin' column to table")
    else:
        print("'chinese_countryOrigin' column already exists")

    # ── 4. Create index on name (if not exists) for fast UPDATE ────────────
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_name ON {TABLE_NAME}(name)")
    print("Ensured index on 'name' column")

    # ── 5. Update rows ─────────────────────────────────────────────────────
    # Get all distinct names from the table
    cur.execute(f"SELECT DISTINCT name FROM {TABLE_NAME}")
    db_names = {row[0] for row in cur.fetchall()}
    print(f"Found {len(db_names)} distinct names in table")

    matched = 0
    unmatched_names = []
    update_data = []

    for name in db_names:
        if name in name_to_chinese:
            update_data.append((name_to_chinese[name], name))
            matched += 1
        else:
            unmatched_names.append(name)

    # Batch update
    cur.executemany(
        f"UPDATE {TABLE_NAME} SET chinese_countryOrigin = ? WHERE name = ?",
        update_data
    )
    conn.commit()

    # ── 5. Summary ──────────────────────────────────────────────────────────
    # Count updated rows
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE chinese_countryOrigin IS NOT NULL AND chinese_countryOrigin != ''")
    non_empty = cur.fetchone()[0]

    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    total_rows = cur.fetchone()[0]

    print(f"\n--- Summary ---")
    print(f"Total rows in table:       {total_rows}")
    print(f"Distinct names in table:   {len(db_names)}")
    print(f"Names matched from CSV:    {matched}")
    print(f"Names not found in CSV:    {len(unmatched_names)}")
    print(f"Rows with non-empty value: {non_empty}")

    if unmatched_names:
        print(f"\nFirst 20 unmatched names:")
        for n in sorted(unmatched_names)[:20]:
            print(f"  {n}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
