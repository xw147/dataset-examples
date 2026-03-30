"""
Export a SQLite table to a CSV file with robust quoting.

Uses csv.QUOTE_ALL to ensure fields containing commas, newlines, or quotes
(e.g. review_text, friends, attributes) don't break the CSV structure.

Usage:
    python export_table_to_csv.py [table_name]

If no table name is given, defaults to 'real_chinese_restaurant_reviews'.
"""

import csv
import os
import sqlite3
import sys

# ── Defaults ────────────────────────────────────────────────────────────────
DEFAULT_TABLE = "real_chinese_restaurant_reviews"
DB_PATH = os.path.join(
    os.path.dirname(__file__), "..",
    "yelp_dataset", "full_yelp.db"
)
OUTPUT_DIR = os.path.join(
    os.path.dirname(__file__), "..",
    "yelp_dataset"
)


def export_table(table_name: str, db_path: str = DB_PATH, output_dir: str = OUTPUT_DIR):
    """Export a SQLite table to a CSV with QUOTE_ALL."""
    output_path = os.path.join(output_dir, f"{table_name}.csv")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Increase field size limit for large text fields
    csv.field_size_limit(sys.maxsize)

    cur.execute(f"SELECT * FROM [{table_name}]")
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(columns)
        writer.writerows(rows)

    conn.close()

    print(f"Table:   {table_name}")
    print(f"Rows:    {len(rows)}")
    print(f"Columns: {len(columns)}")
    print(f"Output:  {output_path}")
    return output_path


def main():
    table_name = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TABLE
    export_table(table_name)


if __name__ == "__main__":
    main()
