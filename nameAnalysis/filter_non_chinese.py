"""
Second-pass filter: identify likely non-Chinese names among send_to_tool=yes rows.

Reads the output of preprocess_usernames.py (which must have columns:
name, first_name, last_name, preprocess_label, send_to_tool) and applies
two heuristics to flag names as likely non-Chinese:

  1. Letter-pattern analysis — detects consonant clusters, digraphs, and
     vowel combinations that never appear in Chinese pinyin OR Wade-Giles
     romanisation.
  2. names-dataset lookup — uses a free offline database of ~730k names
     with country-of-origin probabilities to score each name's likelihood
     of being Chinese.

Names already tagged as chinese_romanised_multi or chinese_romanised_single
by preprocess_usernames.py are protected from filtering (kept as send_to_tool=yes).

Names identified as likely non-Chinese get send_to_tool set to "no".
A new column 'ethnicity_filter' records the reason:
  - "non_chinese_pattern"   — letter patterns rule out Chinese
  - "non_chinese_db"        — names-dataset says < threshold Chinese
  - "non_chinese_both"      — both heuristics agree
  - "ambiguous_not_in_db"   — not found in database, no pattern match
  - ""                      — no change (row was already skip, Chinese romanised, or still needs tool)

Requires: pip install names-dataset

Usage:
    python filter_non_chinese.py <input_csv> [-o output.csv] [--threshold 0.10]

    --threshold: Chinese-region probability below which a name is considered
                 non-Chinese (default: 0.10). Lower = more conservative.
"""

import csv
import argparse
import sys
import os
from collections import Counter

# ─────────────────────────────────────────────
# Non-Chinese letter patterns
# ─────────────────────────────────────────────
# These letter combinations never appear in standard Chinese pinyin
# OR Wade-Giles romanisation. If a name contains any of them, it is
# very unlikely to be Chinese.
#
# NOTE: Some patterns that look "non-Chinese" actually appear in
# Wade-Giles (e.g., "ou" is valid in both pinyin and WG). Only
# patterns that are absent from BOTH systems are included here.

NON_CHINESE_PATTERNS = [
    # English / European digraphs and clusters
    "th", "ph", "ck", "ght", "wh", "wr", "kn", "gh", "tch",
    # Double consonants (neither pinyin nor WG doubles consonants)
    "bb", "dd", "ff", "gg", "kk", "ll", "mm", "pp", "rr", "ss", "tt", "nn", "cc",
    # Multi-consonant clusters (WG uses hs-, ts- but not these)
    "sch", "str", "chr", "spr", "scr",
    # Vowel combinations not found in pinyin or Wade-Giles
    # NOTE: "ou" removed — it IS valid in both pinyin (zhou, dou) and WG (chou, tsou)
    "ey", "ow", "oo", "ee", "ea",
    # Common Western suffixes
    "tion", "sion",
]

# Chinese-speaking regions for scoring
CHINESE_REGIONS = {
    "China",
    "Taiwan",
    "Hong Kong",
    "Macao",
    "Singapore",
    "Malaysia",
}


def has_non_chinese_pattern(name: str) -> bool:
    """Check if name contains letter patterns impossible in Chinese pinyin."""
    lower = name.lower()
    return any(p in lower for p in NON_CHINESE_PATTERNS)


def get_chinese_score(nd, name: str) -> float | None:
    """
    Look up a name in names-dataset and return the probability it comes
    from a Chinese-speaking region. Returns None if name not found.
    """
    result = nd.search(name)
    if result is None:
        return None
    fn = result.get("first_name") or {}
    countries = fn.get("country") or {}
    if not countries:
        return None
    return sum(countries.get(c, 0) for c in CHINESE_REGIONS)


def main():
    parser = argparse.ArgumentParser(
        description="Filter likely non-Chinese names from preprocessed CSV."
    )
    parser.add_argument("input_csv", help="Path to the preprocessed CSV file")
    parser.add_argument(
        "-o", "--output",
        help="Path for output CSV (default: <input>_filtered.csv)"
    )
    parser.add_argument(
        "--threshold", type=float, default=0.10,
        help="Chinese-region probability threshold (default: 0.10). "
             "Names scoring below this are flagged as non-Chinese."
    )
    parser.add_argument(
        "--name-col", default="first_name",
        help="Column to use for name lookup (default: 'first_name')"
    )
    args = parser.parse_args()

    input_path = args.input_csv
    threshold = args.threshold

    # Derive output path
    if args.output:
        output_path = args.output
    else:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_filtered{ext}"

    # Load names-dataset
    print("Loading names-dataset (this may take a moment)...")
    try:
        from names_dataset import NameDataset
        nd = NameDataset()
    except ImportError:
        print("Error: names-dataset not installed.")
        print("Install with: pip install names-dataset")
        sys.exit(1)
    print("Names-dataset loaded.\n")

    # Read CSV
    with open(input_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)

        # Validate required columns
        required = {"name", "first_name", "preprocess_label", "send_to_tool"}
        missing = required - set(fieldnames)
        if missing:
            print(f"Error: missing columns {missing}. Is this the output of preprocess_usernames.py?")
            sys.exit(1)

        rows = list(reader)

    # Add new column
    if "ethnicity_filter" not in fieldnames:
        fieldnames.append("ethnicity_filter")

    # Process each row
    name_col = args.name_col
    stats = Counter()

    for row in rows:
        # Only process rows that are currently send_to_tool=yes
        if row["send_to_tool"] != "yes":
            row["ethnicity_filter"] = ""
            stats["already_skip"] += 1
            continue

        name = row[name_col].strip()
        if not name:
            row["ethnicity_filter"] = ""
            stats["empty"] += 1
            continue

        # Names already identified as Chinese romanisation (pinyin or
        # Wade-Giles) by preprocess_usernames.py should NOT be filtered
        # out as non-Chinese — they are likely Chinese and should still
        # go to the paid tool for confirmation.
        label = row.get("preprocess_label", "")
        if label in {"chinese_romanised_multi", "chinese_romanised_single"}:
            row["ethnicity_filter"] = ""
            stats["chinese_romanised_kept"] += 1
            continue

        pattern_match = has_non_chinese_pattern(name)
        chinese_score = get_chinese_score(nd, name)
        db_says_non_chinese = (chinese_score is not None and chinese_score < threshold)

        if pattern_match and db_says_non_chinese:
            row["ethnicity_filter"] = "non_chinese_both"
            row["send_to_tool"] = "no"
            stats["non_chinese_both"] += 1
        elif pattern_match:
            row["ethnicity_filter"] = "non_chinese_pattern"
            row["send_to_tool"] = "no"
            stats["non_chinese_pattern"] += 1
        elif db_says_non_chinese:
            row["ethnicity_filter"] = "non_chinese_db"
            row["send_to_tool"] = "no"
            stats["non_chinese_db"] += 1
        elif chinese_score is None and not pattern_match:
            row["ethnicity_filter"] = "ambiguous_not_in_db"
            # Keep send_to_tool = yes — we can't tell
            stats["ambiguous_not_in_db"] += 1
        else:
            row["ethnicity_filter"] = ""
            stats["still_send"] += 1

    # Write output
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Summary
    total = len(rows)
    chinese_kept = stats.get("chinese_romanised_kept", 0)
    total_send_before = sum(1 for r in rows if r["send_to_tool"] == "yes") + \
                        stats["non_chinese_both"] + stats["non_chinese_pattern"] + stats["non_chinese_db"]
    total_filtered = stats["non_chinese_both"] + stats["non_chinese_pattern"] + stats["non_chinese_db"]
    total_send_after = sum(1 for r in rows if r["send_to_tool"] == "yes")

    print(f"{'=' * 60}")
    print(f"  Input:     {input_path}")
    print(f"  Output:    {output_path}")
    print(f"  Threshold: {threshold}")
    print(f"{'=' * 60}")
    print(f"  Total rows:                    {total}")
    print(f"  Already skipped (from pass 1): {stats['already_skip']}")
    print(f"  Sent to this filter:           {total_send_before}")
    print(f"{'=' * 60}")
    print(f"  Chinese romanised (kept):      {chinese_kept}")
    print(f"  Non-Chinese (letter pattern):  {stats['non_chinese_pattern']}")
    print(f"  Non-Chinese (name database):   {stats['non_chinese_db']}")
    print(f"  Non-Chinese (both):            {stats['non_chinese_both']}")
    print(f"  ─────────────────────────────────")
    print(f"  TOTAL filtered out:            {total_filtered}  "
          f"({total_filtered / max(total_send_before, 1) * 100:.1f}% reduction)")
    print(f"{'=' * 60}")
    print(f"  Ambiguous (not in DB):         {stats['ambiguous_not_in_db']}")
    print(f"  Still send to paid tool:       {total_send_after}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
