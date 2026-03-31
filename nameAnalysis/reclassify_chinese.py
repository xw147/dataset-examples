"""
Reclassify chinese_countryOrigin into a clean five-way label,
and add an is_name column for future pre-filtering.

Adds two INDEPENDENT columns:

  'is_name' — whether the username is likely a personal name:
    - "yes"   — structurally looks like a personal name
    - "maybe" — ambiguous format (hyphenated, initial+name, unusual)
    - "no"    — clearly not a personal name (internet handle, business, junk)

  'chinese_final' — cleaned ethnicity label (5 values):
    - "yes"                — strong evidence of Chinese origin
    - "likely_chinese"     — probable Chinese (pinyin/WG match or API alt = Chinese)
    - "no"                 — strong evidence of non-Chinese origin (API prob >= 0.5)
    - "likely_non_chinese" — probable non-Chinese (API 0.35-0.5 or heuristic filter)
    - "undetermined"       — insufficient evidence to classify

Classification uses DIFFERENT rules depending on whether the row has
Namsor API data or not:

  Group A (has API data):
    yes               — country_origin in {CN, TW, HK}
    likely_chinese    — country_origin_alt in {CN, TW, HK}, probAltCalibrated >= 0.7,
                        AND primary origin NOT Korean or SE Asian
    no                — probCalibrated >= 0.5, Chinese not primary/alt
    likely_non_chinese— 0.35 <= probCalibrated < 0.5, Chinese not primary/alt
    undetermined      — probCalibrated < 0.35

  Group B (no API data):
    yes               — preprocess_label = chinese_cjk
    likely_chinese    — preprocess_label = chinese_romanised_multi or _single
    likely_non_chinese— ethnicity_filter = non_chinese_pattern/db/both
    undetermined      — everything else

Usage:
    python reclassify_chinese.py <input_csv> [-o output.csv]
"""

import csv
import argparse
import sys
import os
from collections import Counter

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

CHINESE_COUNTRIES = {"CN", "TW", "HK"}

SE_ASIAN_COUNTRIES = {"MY", "ID", "VN", "KH", "BN", "MM", "TH", "SG", "PH", "LA"}

# Countries excluded from likely_chinese alt-country rule
EXCLUDE_ALT_PRIMARY = SE_ASIAN_COUNTRIES | {"KR"}

NOT_NAME_LABELS = {
    "unanalysable_short",
    "unanalysable_numeric",
    "internet_handle",
    "business_name",
}

YES_NAME_LABELS = {
    "real_name",
    "chinese_cjk",
    "chinese_romanised_multi",
    "chinese_romanised_single",
    "non_latin_other",
    "single_word_name",
}

NON_CHINESE_FILTERS = {
    "non_chinese_pattern",
    "non_chinese_db",
    "non_chinese_both",
}


# ─────────────────────────────────────────────
# Classification functions
# ─────────────────────────────────────────────

def classify_is_name(preprocess_label: str) -> str:
    if preprocess_label in NOT_NAME_LABELS:
        return "no"
    elif preprocess_label in YES_NAME_LABELS:
        return "yes"
    else:
        return "maybe"


def classify_chinese_with_api(
    country_origin: str,
    country_origin_alt: str,
    prob_calibrated: float,
    prob_alt_calibrated: float,
) -> str:
    if country_origin in CHINESE_COUNTRIES:
        return "yes"

    if (
        country_origin_alt in CHINESE_COUNTRIES
        and country_origin not in EXCLUDE_ALT_PRIMARY
        and prob_alt_calibrated >= 0.7
    ):
        return "likely_chinese"

    if prob_calibrated >= 0.5:
        return "no"
    elif prob_calibrated >= 0.35:
        return "likely_non_chinese"
    else:
        return "undetermined"


def classify_chinese_without_api(
    preprocess_label: str,
    ethnicity_filter: str,
) -> str:
    if preprocess_label == "chinese_cjk":
        return "yes"

    if preprocess_label in ("chinese_romanised_multi", "chinese_romanised_single"):
        return "likely_chinese"

    if ethnicity_filter in NON_CHINESE_FILTERS:
        return "likely_non_chinese"

    return "undetermined"


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Reclassify chinese_countryOrigin and add is_name column."
    )
    parser.add_argument("input_csv", help="Path to the CSV after Namsor processing")
    parser.add_argument(
        "-o", "--output",
        help="Path for output CSV (default: <input>_reclassified.csv)"
    )
    args = parser.parse_args()

    input_path = args.input_csv

    if args.output:
        output_path = args.output
    else:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_reclassified{ext}"

    # Read
    with open(input_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        rows = list(reader)

    for col in ["is_name", "chinese_final"]:
        if col not in fieldnames:
            fieldnames.append(col)

    stats_name = Counter()
    stats_chinese = Counter()

    for row in rows:
        preprocess = row.get("preprocess_label", "").strip()
        ef = row.get("ethnicity_filter", "").strip()
        origin = row.get("country_origin", "").strip()
        alt = row.get("country_origin_alt", "").strip()

        try:
            prob = float(row.get("probability_calibrated", ""))
        except (ValueError, TypeError):
            prob = 0.0
        try:
            prob_alt = float(row.get("probability_alt_calibrated", ""))
        except (ValueError, TypeError):
            prob_alt = 0.0

        # is_name (independent of chinese_final)
        is_name = classify_is_name(preprocess)
        row["is_name"] = is_name
        stats_name[is_name] += 1

        # chinese_final
        has_api = bool(origin)
        if has_api:
            chinese = classify_chinese_with_api(origin, alt, prob, prob_alt)
        else:
            chinese = classify_chinese_without_api(preprocess, ef)

        row["chinese_final"] = chinese
        group = "api" if has_api else "no_api"
        stats_chinese[(group, chinese)] += 1

    # Write
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Summary
    total = len(rows)
    api_count = sum(v for (g, _), v in stats_chinese.items() if g == "api")
    noapi_count = sum(v for (g, _), v in stats_chinese.items() if g == "no_api")

    print(f"\n{'=' * 62}")
    print(f"  Input:   {input_path}")
    print(f"  Output:  {output_path}")
    print(f"{'=' * 62}")
    print(f"  is_name:")
    print(f"    yes:    {stats_name.get('yes', 0)}")
    print(f"    maybe:  {stats_name.get('maybe', 0)}")
    print(f"    no:     {stats_name.get('no', 0)}")
    print(f"{'=' * 62}")
    print(f"  chinese_final (overall, n={total}):")
    final_totals = Counter()
    for (_, label), count in stats_chinese.items():
        final_totals[label] += count
    for label in ["yes", "likely_chinese", "no", "likely_non_chinese", "undetermined"]:
        print(f"    {label:<20} {final_totals.get(label, 0)}")
    print(f"{'=' * 62}")
    print(f"  Group A — has API data (n={api_count}):")
    for label in ["yes", "likely_chinese", "no", "likely_non_chinese", "undetermined"]:
        print(f"    {label:<20} {stats_chinese.get(('api', label), 0)}")
    print(f"  Group B — no API data (n={noapi_count}):")
    for label in ["yes", "likely_chinese", "no", "likely_non_chinese", "undetermined"]:
        print(f"    {label:<20} {stats_chinese.get(('no_api', label), 0)}")
    print(f"{'=' * 62}\n")


if __name__ == "__main__":
    main()