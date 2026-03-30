"""
Add a 'chinese_countryOrigin' column to the origin CSV based on country_origin.

Values:
  - 'yes'      : country_origin is a predominantly Chinese country (CN, HK, TW)
  - 'possible' : country_origin is a country with a significant ethnic Chinese
                  diaspora (MY, BN, ID, KH, MM, TH, VN, LA, SG)
  - 'no'       : everything else
  - ''         : country_origin is blank (API returned no result)
"""

import csv
import os

# ── Configurable country lists ──────────────────────────────────────────────
# Core Chinese-majority countries / territories
CHINESE_YES = {"CN", "HK", "TW"}

# Countries with notable ethnic-Chinese diaspora populations
CHINESE_POSSIBLE = {"MY", "BN", "ID", "KH", "MM", "TH", "VN", "LA"}
# Note: SG (Singapore) is not in the Namsor API country list, but is included
# here in case it ever appears in the data.
CHINESE_POSSIBLE.add("SG")

# ── File paths ──────────────────────────────────────────────────────────────
INPUT_CSV = os.path.join(
    os.path.dirname(__file__), "..",
    "yelp_dataset", "unique_user_names_preprocessed_filtered_origin.csv"
)
OUTPUT_CSV = os.path.join(
    os.path.dirname(__file__), "..",
    "yelp_dataset", "unique_user_names_preprocessed_filtered_origin_chinese.csv"
)


def classify_chinese(country_code: str) -> str:
    """Return 'yes', 'possible', 'no', or '' based on country_origin code."""
    if not country_code or country_code.strip() == "":
        return ""
    code = country_code.strip().upper()
    if code in CHINESE_YES:
        return "yes"
    if code in CHINESE_POSSIBLE:
        return "possible"
    return "no"


def main():
    with open(INPUT_CSV, "r", encoding="utf-8", newline="") as fin:
        reader = csv.DictReader(fin)
        fieldnames = list(reader.fieldnames) + ["chinese_countryOrigin"]

        rows = []
        for row in reader:
            row["chinese_countryOrigin"] = classify_chinese(row.get("country_origin", ""))
            rows.append(row)

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # ── Summary ─────────────────────────────────────────────────────────────
    total = len(rows)
    yes_count = sum(1 for r in rows if r["chinese_countryOrigin"] == "yes")
    possible_count = sum(1 for r in rows if r["chinese_countryOrigin"] == "possible")
    no_count = sum(1 for r in rows if r["chinese_countryOrigin"] == "no")
    blank_count = sum(1 for r in rows if r["chinese_countryOrigin"] == "")

    print(f"Total rows:  {total}")
    print(f"  yes:       {yes_count}")
    print(f"  possible:  {possible_count}")
    print(f"  no:        {no_count}")
    print(f"  (blank):   {blank_count}")
    print(f"\nOutput written to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
