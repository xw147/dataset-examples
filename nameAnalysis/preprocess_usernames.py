"""
Preprocess online usernames for ethnicity identification tool.

Adds two columns to the input CSV:
  - preprocess_label: detailed classification of each name
  - send_to_tool: whether the name should be sent to the paid ethnicity tool

Labels:
  chinese_cjk              - Contains Chinese characters → skip tool (already Chinese)
  unanalysable_short       - ≤2 meaningful chars → skip tool (no signal)
  unanalysable_numeric     - Purely numbers/punctuation → skip tool (no signal)
  internet_handle          - Digits mixed with letters → skip tool (not a real name)
  business_name            - Contains business keywords → skip tool (not personal)
  non_latin_other          - Accented/European/Vietnamese chars → send to tool
  chinese_romanised_multi  - Multi-word pinyin/Wade-Giles match → send to tool (hint)
  chinese_romanised_single - Single-word pinyin/Wade-Giles match → send to tool (hint)
  real_name                - Multi-word First-Last pattern → send to tool (high value)
  single_word_name         - Single alphabetic word, plausible name → send to tool
  other                    - Anything else → send to tool

Usage:
    python preprocess_usernames.py <input_csv> [--name-col name]

If --name-col is not specified, defaults to "name".
The script overwrites the input CSV with the two new columns added.
"""

import csv
import re
import argparse
import sys
import os

# ─────────────────────────────────────────────
# Reference lists
# ─────────────────────────────────────────────

# Common Chinese surnames in pinyin (top ~100)
PINYIN_SURNAMES = {
    "bai", "bao", "bi", "cai", "cao", "chang", "chen", "cheng", "chi",
    "cui", "dai", "deng", "ding", "dong", "du", "duan", "fan", "fang",
    "feng", "fu", "gao", "ge", "gong", "gu", "guan", "guo", "han",
    "hao", "he", "hou", "hu", "hua", "huang", "jia", "jiang", "jin",
    "kang", "kong", "lai", "lei", "li", "liang", "liao", "lin", "ling",
    "liu", "long", "lu", "luo", "lv", "ma", "mao", "meng", "mo", "mu",
    "ni", "niu", "ou", "pan", "pang", "pei", "peng", "qi", "qian",
    "qiao", "qin", "qiu", "qu", "ren", "ruan", "shao", "shen", "shi",
    "song", "su", "sun", "tan", "tang", "tao", "tian", "wan", "wang",
    "wei", "wen", "wu", "xia", "xiang", "xiao", "xie", "xin", "xiong",
    "xu", "xue", "yan", "yang", "yao", "ye", "yi", "yin", "yu", "yuan",
    "yue", "zeng", "zha", "zhai", "zhang", "zhao", "zheng", "zhong",
    "zhou", "zhu", "zhuang", "zhuo", "zou",
}

# Common Chinese given names / syllables in pinyin
# These are common standalone pinyin syllables that frequently appear as
# Chinese given names. Note: many overlap with other cultures (e.g., "Lin"
# is also Scandinavian), so these are HINTS, not classifications.
PINYIN_GIVEN_NAMES = {
    "ai", "an", "ang", "bao", "bei", "bin", "bing", "bo", "cai", "can",
    "chang", "chao", "chen", "cheng", "chi", "chun", "cong", "cui",
    "da", "dan", "dao", "de", "di", "dong", "du", "en", "er",
    "fan", "fang", "fei", "fen", "feng", "fu", "gang", "gao", "ge",
    "guang", "gui", "guo", "hai", "han", "hang", "hao", "he", "heng",
    "hong", "hua", "huai", "huan", "huang", "hui", "huo",
    "ji", "jia", "jian", "jiang", "jiao", "jie", "jin", "jing", "jiu",
    "juan", "jun", "kai", "kan", "kang", "ke", "kun", "lan", "lang",
    "lei", "li", "lian", "liang", "lien", "lin", "ling", "liu", "long",
    "lu", "lun", "luo", "mei", "meng", "mi", "mian", "miao", "min",
    "ming", "mo", "mu", "na", "nai", "nan", "neng", "ni", "nian",
    "ning", "niu", "nuo", "pei", "peng", "ping", "po", "pu",
    "qi", "qian", "qiang", "qiao", "qin", "qing", "qiong", "qiu", "qu",
    "quan", "que", "ran", "rang", "ren", "rong", "ru", "rui", "ruo",
    "shan", "shang", "shao", "shen", "sheng", "shi", "shu", "shuang",
    "shui", "shun", "si", "song", "su", "tai", "tan", "tang", "tao",
    "teng", "tian", "ting", "tong", "wan", "wang", "wei", "wen", "wu",
    "xi", "xia", "xian", "xiang", "xiao", "xin", "xing", "xiong",
    "xiu", "xu", "xuan", "xue", "ya", "yan", "yang", "yao", "ye",
    "yi", "yin", "ying", "yong", "you", "yu", "yuan", "yue", "yun",
    "zai", "zan", "ze", "zeng", "zhan", "zhang", "zhao", "zhe", "zhen",
    "zheng", "zhi", "zhong", "zhou", "zhu", "zhuan", "zhuang", "zhui",
    "zhun", "zhuo", "zi", "zong", "zou", "zu", "zuo",
}

# Business / non-personal name keywords
BUSINESS_KEYWORDS = [
    "llc", "inc", "corp", "ltd", "restaurant", "cafe", "coffee",
    "shop", "store", "pest", "termite", "company", "service",
    "salon", "studio", "realty", "plumbing", "dental", "clinic",
    "agency", "group", "team", "enterprise", "solutions", "consulting",
    "photography", "catering", "cleaning", "roofing", "landscaping",
    "automotive", "towing",
]

# ─────────────────────────────────────────────
# Wade-Giles romanisation (used in Taiwan, older texts)
# Only syllables that DIFFER from pinyin are listed here;
# overlapping ones (e.g., 'chang', 'wang') are already in the pinyin sets.
# ─────────────────────────────────────────────

# Wade-Giles surnames that differ from pinyin
WADEGILES_SURNAMES = {
    # hs- → pinyin x- (highly distinctive, never appears in non-Chinese names)
    "hsiao", "hsieh", "hsu", "hsueh", "hsing",
    # ts- → pinyin z-/c-
    "tsai", "tsao", "tseng", "tsou", "tso", "tsung",
    # ch- → pinyin j-/q- (before i)
    "chiang", "chien", "chiu", "chou", "chieh",
    # k- → pinyin g-
    "kung", "kuo", "kuang", "ku", "ko",
    # p- → pinyin b-
    "pai", "pao",
    # t- → pinyin d-
    "tuan", "tung",
    # j- → pinyin r-
    "jen",
    # other common Taiwan WG surnames
    "yeh", "ho", "lo", "hung", "shih", "sung", "tai", "tu",
    "kao", "lee",
}

# Wade-Giles given-name syllables that differ from pinyin
WADEGILES_GIVEN_NAMES = {
    # hs- initial → pinyin x-
    "hsi", "hsia", "hsiang", "hsiao", "hsieh", "hsien", "hsin", "hsing",
    "hsiung", "hsiu", "hsu", "hsuan", "hsueh", "hsun",
    # ts- initial → pinyin z-
    "tsa", "tsai", "tsan", "tsang", "tsao", "tse", "tsen", "tseng",
    "tso", "tsou", "tsu", "tsuan", "tsui", "tsun", "tsung",
    # ch- before i/ü → pinyin j-/q-
    "chia", "chiang", "chiao", "chieh", "chien", "chih", "chin", "ching",
    "chiu", "chiung",
    # k- → pinyin g-/j-
    "ka", "kai", "kan", "kao", "kei", "ken", "keng",
    "ko", "kou", "ku", "kua", "kuai", "kuan", "kuang", "kuei", "kun",
    "kung", "kuo",
    # p- → pinyin b-
    "pa", "pai", "pan", "pao", "pi", "piao", "pieh", "pien", "pin",
    "po", "pu",
    # t- → pinyin d-
    "ta", "tai", "tao", "te", "tei", "ten",
    "ti", "tiao", "tieh", "tien", "tiu", "to", "tou",
    "tu", "tuan", "tui", "tun", "tung", "tuo",
    # j- → pinyin r-
    "jan", "jang", "jao", "je", "jen", "jeng", "jih", "jo", "jou",
    "ju", "juan", "jui", "jun", "jung",
}

# Combined Chinese romanisation sets (pinyin + Wade-Giles)
CHINESE_SURNAMES = PINYIN_SURNAMES | WADEGILES_SURNAMES
CHINESE_GIVEN_NAMES = PINYIN_GIVEN_NAMES | WADEGILES_GIVEN_NAMES


# ─────────────────────────────────────────────
# Classification functions
# ─────────────────────────────────────────────

def has_cjk(name: str) -> bool:
    """Check if name contains CJK Unified Ideographs."""
    return any(
        "\u4e00" <= c <= "\u9fff" or   # CJK Unified
        "\u3400" <= c <= "\u4dbf" or   # CJK Extension A
        "\uf900" <= c <= "\ufaff"      # CJK Compatibility
        for c in name
    )


def is_numeric_or_punctuation(name: str) -> bool:
    """Check if name is purely numbers, punctuation, or whitespace."""
    return bool(re.match(r"^[\d\s\W]+$", name.strip())) and len(name.strip()) > 0


def is_short(name: str) -> bool:
    """Check if the name has ≤2 meaningful (letter) characters."""
    letters = re.sub(r"[^a-zA-Z\u4e00-\u9fff]", "", name)
    return len(letters) <= 2


def is_internet_handle(name: str) -> bool:
    """Detect internet handles: letters mixed with digits, no spaces."""
    stripped = name.strip()
    if " " in stripped:
        return False
    has_letter = bool(re.search(r"[a-zA-Z]", stripped))
    has_digit = bool(re.search(r"\d", stripped))
    return has_letter and has_digit


def is_business_name(name: str) -> bool:
    """Check if name contains business-related keywords as whole words."""
    lower = name.lower()
    return any(re.search(r"\b" + re.escape(kw) + r"\b", lower) for kw in BUSINESS_KEYWORDS)


def has_non_latin_accented(name: str) -> bool:
    """Check for accented / non-ASCII Latin characters (European, Vietnamese, etc.)."""
    # Has letters above ASCII but NOT CJK
    return any(
        ord(c) > 127 and c.isalpha()
        and not ("\u4e00" <= c <= "\u9fff")
        and not ("\u3400" <= c <= "\u4dbf")
        for c in name
    )


def is_chinese_romanised_multi(name: str) -> bool:
    """Check if multi-word name matches Chinese romanisation (pinyin or Wade-Giles).
    Also handles hyphenated given names common in Taiwanese usage (e.g., Kuo-Hua)."""
    stripped = name.strip().lower()
    # Split on spaces first, then split each part on hyphens
    space_parts = stripped.split()
    if len(space_parts) < 2:
        # Single space-word — check if it's hyphenated (e.g., Tsz-Cheong)
        parts = stripped.replace("'", "").split("-")
        if len(parts) < 2:
            return False
    else:
        # Flatten: split each space-part on hyphens too
        parts = []
        for sp in space_parts:
            parts.extend(sp.replace("'", "").split("-"))

    if len(parts) < 2:
        return False

    return parts[0] in CHINESE_SURNAMES and all(
        p in CHINESE_GIVEN_NAMES or p in CHINESE_SURNAMES for p in parts[1:]
    )


def is_chinese_romanised_single(name: str) -> bool:
    """Check if single-word name matches a Chinese romanisation syllable
    (pinyin or Wade-Giles)."""
    stripped = name.strip().lower().replace("'", "")
    if " " in stripped or not stripped.isalpha():
        return False
    return stripped in CHINESE_GIVEN_NAMES or stripped in CHINESE_SURNAMES


def is_real_name_pattern(name: str) -> bool:
    """Check for classic 'First Last' pattern (capitalised words, letters only)."""
    parts = name.strip().split()
    if len(parts) < 2:
        return False
    return all(
        re.match(r"^[A-Z][a-z]+$", p) or re.match(r"^[A-Z]\.$", p)
        for p in parts
    )


def is_single_word_name(name: str) -> bool:
    """Single alphabetic word, >2 chars — plausible name."""
    stripped = name.strip()
    return stripped.isalpha() and len(stripped) > 2 and " " not in stripped


# ─────────────────────────────────────────────
# Main classifier — order matters (first match wins)
# ─────────────────────────────────────────────

def split_name(name: str) -> tuple[str, str]:
    """
    Split a name into (first_name, last_name).

    Rules:
      - Single word or non-splittable → (original_name, "")
      - Two words → (first_word, second_word)
      - Three+ words → (first_word, remaining_words joined)
      - CJK-only names: 1 char → (char, ""); 2+ chars → (first_char, rest)
      - Mixed CJK + Latin or other edge cases → (original_name, "")
    """
    stripped = name.strip()
    if not stripped:
        return ("", "")

    # CJK-only names: split by character
    if all(
        "\u4e00" <= c <= "\u9fff" or "\u3400" <= c <= "\u4dbf"
        for c in stripped
    ):
        if len(stripped) == 1:
            return (stripped, "")
        else:
            return (stripped[0], stripped[1:])

    # Space-delimited names
    parts = stripped.split()
    if len(parts) == 1:
        return (stripped, "")
    elif len(parts) == 2:
        return (parts[0], parts[1])
    else:
        # First word = first name, rest = last name
        return (parts[0], " ".join(parts[1:]))


# ─────────────────────────────────────────────
# Main classifier — order matters (first match wins)
# ─────────────────────────────────────────────

def classify_name(name: str) -> tuple[str, bool]:
    """
    Classify a username and decide whether to send to paid tool.

    Returns:
        (preprocess_label, send_to_tool)
    """
    if not name or not name.strip():
        return ("unanalysable_short", False)

    # 1. Chinese characters — definite Chinese
    if has_cjk(name):
        return ("chinese_cjk", False)

    # 2. Purely numeric / punctuation
    if is_numeric_or_punctuation(name):
        return ("unanalysable_numeric", False)

    # 3. Very short (≤2 letters)
    if is_short(name):
        return ("unanalysable_short", False)

    # 4. Business names
    if is_business_name(name):
        return ("business_name", False)

    # 5. Internet handles (letters + digits, no spaces)
    if is_internet_handle(name):
        return ("internet_handle", False)

    # 6. Non-Latin accented characters (European / Vietnamese etc.)
    if has_non_latin_accented(name):
        return ("non_latin_other", True)

    # 7. Multi-word Chinese romanisation (pinyin or Wade-Giles)
    if is_chinese_romanised_multi(name):
        return ("chinese_romanised_multi", True)

    # 8. Classic First Last real-name pattern
    if is_real_name_pattern(name):
        return ("real_name", True)

    # 9. Single-word Chinese romanisation match (hint only)
    if is_chinese_romanised_single(name):
        return ("chinese_romanised_single", True)

    # 10. Single alphabetic word — plausible name
    if is_single_word_name(name):
        return ("single_word_name", True)

    # 11. Everything else
    return ("other", True)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Preprocess usernames for ethnicity identification tools."
    )
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument(
        "--name-col", default="name",
        help="Name of the column containing usernames (default: 'name')"
    )
    parser.add_argument(
        "-o", "--output",
        help="Path for output CSV (default: <input>_preprocessed.csv)"
    )
    args = parser.parse_args()

    input_path = args.input_csv
    name_col = args.name_col

    # Derive output path
    if args.output:
        output_path = args.output
    else:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_preprocessed{ext}"

    # Read
    with open(input_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if name_col not in reader.fieldnames:
            print(f"Error: column '{name_col}' not found. Available: {reader.fieldnames}")
            sys.exit(1)
        rows = list(reader)
        fieldnames = list(reader.fieldnames)

    # Add new columns
    new_cols = ["first_name", "last_name", "preprocess_label", "send_to_tool"]
    for col in new_cols:
        if col not in fieldnames:
            fieldnames.append(col)

    # Classify and split names
    for row in rows:
        name = row[name_col]
        label, send = classify_name(name)
        row["preprocess_label"] = label
        row["send_to_tool"] = "yes" if send else "no"

        # Split into first_name / last_name only for likely real personal names
        SPLITTABLE_LABELS = {
            "real_name", "chinese_romanised_multi", "non_latin_other",
            "chinese_romanised_single", "single_word_name", "chinese_cjk",
        }
        if label in SPLITTABLE_LABELS:
            first, last = split_name(name)
        else:
            first, last = name.strip(), ""
        row["first_name"] = first
        row["last_name"] = last

    # Write to NEW file (original is untouched)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Print summary
    from collections import Counter
    label_counts = Counter(row["preprocess_label"] for row in rows)
    send_yes = sum(1 for row in rows if row["send_to_tool"] == "yes")
    send_no = len(rows) - send_yes

    print(f"\n{'='*55}")
    print(f"  Input:  {input_path}")
    print(f"  Output: {output_path}")
    print(f"{'='*55}")
    print(f"  Total names: {len(rows)}")
    print(f"  Send to tool: {send_yes}  |  Skip tool: {send_no}")
    print(f"{'='*55}")
    print(f"  {'Label':<25} {'Count':>6}  {'Action'}")
    print(f"  {'-'*50}")
    for label in [
        "chinese_cjk", "unanalysable_short", "unanalysable_numeric",
        "internet_handle", "business_name", "non_latin_other",
        "chinese_romanised_multi", "chinese_romanised_single", "real_name",
        "single_word_name", "other",
    ]:
        count = label_counts.get(label, 0)
        action = "SKIP" if label in {
            "chinese_cjk", "unanalysable_short",
            "unanalysable_numeric", "internet_handle", "business_name"
        } else "SEND"
        print(f"  {label:<25} {count:>6}  {action}")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()