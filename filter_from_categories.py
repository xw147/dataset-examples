"""
Create Filtered Table from Categories Column
=============================================

This script:
1. Reads the 'categories' column from 'chinese_restaurants' view
2. Applies the filtering logic directly to the categories
3. Saves filtered results as a new table in the database

No need for external ID files - filters based on tag logic.

Usage:
    python filter_from_categories.py
"""

import sqlite3
import pandas as pd
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

DATABASE_PATH = 'yelp_dataset/full_yelp.db'
SOURCE_TABLE = 'chinese_restaurants'
CATEGORIES_COLUMN = 'categories'
OUTPUT_TABLE = 'real_chinese_restaurants'
OVERWRITE_IF_EXISTS = True

# ============================================================================
# FILTERING LOGIC (same as the analysis)
# ============================================================================

# Tags that indicate NON-RESTAURANT businesses (always exclude)
NON_RESTAURANT_TAGS = {
    'Traditional Chinese Medicine', 'Health & Medical', 'Beauty & Spas', 
    'Massage Therapy', 'Massage', 'Acupuncture', 'Tui Na', 'Reflexology',
    'Naturopathic/Holistic', 'Doctors', 'Medical Centers', 'Active Life',
    'Fitness & Instruction', 'Professional Services', 'Life Coach',
    'Shopping', 'Grocery', 'Candy Stores', 'Department Stores', 
    'Health Markets', 'Meat Shops', 'Sporting Goods', 'Fashion', 
    'Arts & Entertainment', 'Education', 'Hotels & Travel', 'Tours', 
    'Local Services', 'Day Spas', 'Skin Care', 'Weight Loss Centers', 
    'Chiropractors', 'Physical Therapy', 'Martial Arts',
    'Chinese Martial Arts', 'Yoga', 'Meditation Centers', 'Qi Gong', 
    'Tai Chi', 'Reiki', 'Hypnosis/Hypnotherapy', 
    'Counseling & Mental Health', 'Supernatural Readings', 'Psychics', 
    'Cannabis Clinics', 'Pain Management', 'Sports Medicine', 'Fertility', 
    'Eyebrow Services', 'Waxing', 'Hair Removal', 'Nail Salons', 
    'Tattoo', 'Medical Spas', 'Nutritionists'
}

# Tags that indicate MIXED CUISINES (exclude in strict mode)
MIXED_CUISINE_TAGS = {
 'Asian Fusion', 'Pan Asian', 'Japanese', 'Sushi Bars', 
    'Thai', 'Korean', 'Vietnamese', 'Italian', 'Indian', 
    'Mexican', 'Tex-Mex', 'Mediterranean', 'Middle Eastern',
    'Filipino', 'Malaysian', 'Singaporean', 'Himalayan/Nepalese',
    'Greek', 'Latin American', 'Spanish', 'American (New)', 
    'American (Traditional)'
}


def parse_categories(categories_str):
    """Parse comma-separated categories string into a list (case insensitive)"""
    if pd.isna(categories_str) or categories_str == '':
        return []
    # Convert to lowercase for case-insensitive matching
    return [tag.strip().lower() for tag in categories_str.split(',')]


def has_chinese_tag(tags):
    """Check if has Chinese tag (case insensitive, exact match)"""
    return 'chinese' in tags


def has_restaurants_tag(tags):
    """Check if has Restaurants tag (case insensitive, exact match)"""
    return 'restaurants' in tags


def is_non_restaurant_business(tags):
    """Check if this is a non-restaurant business (case insensitive, exact match)"""
    # Convert NON_RESTAURANT_TAGS to lowercase for comparison
    non_restaurant_lower = {tag.lower() for tag in NON_RESTAURANT_TAGS}
    
    # Check for exact matches
    return bool(set(tags) & non_restaurant_lower)


def has_mixed_cuisine(tags):
    """Check if restaurant serves mixed cuisines (case insensitive, exact match)"""
    # Convert MIXED_CUISINE_TAGS to lowercase for comparison
    mixed_cuisine_lower = {tag.lower() for tag in MIXED_CUISINE_TAGS}
    
    # Check for exact matches
    return bool(set(tags) & mixed_cuisine_lower)


def is_real_chinese_restaurant(categories_str):
    """
    Determine if a restaurant is a 'real Chinese restaurant'
    
    Criteria:
    1. Must have 'Chinese' tag
    2. Must have 'Restaurants' tag
    3. Must NOT be a non-restaurant business
    4. Must NOT have mixed cuisine tags
    """
    tags = parse_categories(categories_str)
    
    # Must have Chinese and Restaurants tags
    if not has_chinese_tag(tags):
        return False
    if not has_restaurants_tag(tags):
        return False
    
    # Exclude non-restaurant businesses
    if is_non_restaurant_business(tags):
        return False
    
    # Exclude mixed cuisines
    if has_mixed_cuisine(tags):
        return False
    
    return True


# ============================================================================
# MAIN SCRIPT
# ============================================================================

def main():
    """Main function"""
    print("\n" + "=" * 80)
    print("FILTER REAL CHINESE RESTAURANTS FROM CATEGORIES")
    print("=" * 80)
    print()
    
    # Check if database exists
    if not Path(DATABASE_PATH).exists():
        print(f"✗ Error: Database file '{DATABASE_PATH}' not found!")
        return
    
    # Connect to database
    print(f"🔌 Connecting to database: {DATABASE_PATH}")
    conn = sqlite3.connect(DATABASE_PATH)
    print(f"   ✓ Connected successfully\n")
    
    try:
        # Check if source table exists
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type IN ('table', 'view') AND name = ?
        """, (SOURCE_TABLE,))
        
        if not cursor.fetchone():
            raise ValueError(f"Table/view '{SOURCE_TABLE}' not found in database!")
        
        print(f"✓ Found table/view: '{SOURCE_TABLE}'")
        
        # Check if categories column exists
        cursor.execute(f"PRAGMA table_info({SOURCE_TABLE})")
        columns = [row[1] for row in cursor.fetchall()]
        
        if CATEGORIES_COLUMN not in columns:
            raise ValueError(f"Column '{CATEGORIES_COLUMN}' not found! Available: {columns}")
        
        print(f"✓ Found column: '{CATEGORIES_COLUMN}'\n")
        
        # Load data
        print("=" * 80)
        print("LOADING DATA")
        print("=" * 80)
        print(f"📊 Loading data from '{SOURCE_TABLE}'...")
        
        df = pd.read_sql_query(f"SELECT * FROM {SOURCE_TABLE}", conn)
        print(f"   Total rows: {len(df)}")
        
        # Apply filtering logic
        print("\n" + "=" * 80)
        print("APPLYING FILTERING LOGIC")
        print("=" * 80)
        
        print(f"🔍 Filtering based on categories...")
        df['is_real_chinese'] = df[CATEGORIES_COLUMN].apply(is_real_chinese_restaurant)
        
        df_filtered = df[df['is_real_chinese']].copy()
        df_filtered = df_filtered.drop('is_real_chinese', axis=1)
        
        print(f"   Filtered restaurants: {len(df_filtered)}")
        print(f"   Percentage kept: {len(df_filtered)/len(df)*100:.1f}%")
        
        # Show filtering breakdown
        print("\n📈 Filtering breakdown:")
        
        df['tags'] = df[CATEGORIES_COLUMN].apply(parse_categories)
        
        has_chinese = df['tags'].apply(has_chinese_tag).sum()
        has_restaurants = df['tags'].apply(has_restaurants_tag).sum()
        both_tags = df['tags'].apply(lambda t: has_chinese_tag(t) and has_restaurants_tag(t)).sum()
        non_restaurant = df['tags'].apply(is_non_restaurant_business).sum()
        mixed_cuisine = df['tags'].apply(has_mixed_cuisine).sum()
        
        print(f"   Has 'Chinese' tag: {has_chinese}")
        print(f"   Has 'Restaurants' tag: {has_restaurants}")
        print(f"   Has both tags: {both_tags}")
        print(f"   Non-restaurant businesses: {non_restaurant}")
        print(f"   Mixed cuisines: {mixed_cuisine}")
        print(f"   ✅ Real Chinese (kept): {len(df_filtered)}")
        
        # Save to new table
        print("\n" + "=" * 80)
        print("SAVING TO DATABASE")
        print("=" * 80)
        
        # Check if output table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type = 'table' AND name = ?
        """, (OUTPUT_TABLE,))
        
        if cursor.fetchone() and OVERWRITE_IF_EXISTS:
            print(f"⚠️  Table '{OUTPUT_TABLE}' already exists. Dropping it...")
            cursor.execute(f"DROP TABLE IF EXISTS {OUTPUT_TABLE}")
            conn.commit()
        
        print(f"💾 Saving to table '{OUTPUT_TABLE}'...")
        df_filtered.to_sql(OUTPUT_TABLE, conn, if_exists='replace', index=False)
        conn.commit()
        
        print(f"   ✓ Created table '{OUTPUT_TABLE}' with {len(df_filtered)} rows")
        
        # Verify
        print("\n" + "=" * 80)
        print("VERIFICATION")
        print("=" * 80)
        
        cursor.execute(f"SELECT COUNT(*) FROM {OUTPUT_TABLE}")
        count = cursor.fetchone()[0]
        print(f"✓ Table '{OUTPUT_TABLE}' created successfully")
        print(f"  Rows in table: {count}")
        
        # Sample verification
        print(f"\n  Sample verification:")
        cursor.execute(f"SELECT {CATEGORIES_COLUMN} FROM {OUTPUT_TABLE} LIMIT 3")
        samples = cursor.fetchall()
        for i, (cats,) in enumerate(samples, 1):
            tags = parse_categories(cats)
            has_chinese = 'Chinese' in tags
            has_fusion = any(tag in MIXED_CUISINE_TAGS for tag in tags)
            print(f"    {i}. Chinese: {has_chinese}, Fusion: {has_fusion}")
        
        print("\n" + "=" * 80)
        print("✅ SUCCESS!")
        print("=" * 80)
        print(f"\n✓ Created table '{OUTPUT_TABLE}' in {DATABASE_PATH}")
        print(f"✓ Contains {len(df_filtered)} real Chinese restaurants")
        print(f"\n📂 Open DB Browser for SQLite to query the new table:")
        print(f"   SELECT * FROM {OUTPUT_TABLE};")
        
    except Exception as e:
        print(f"\n✗ Error occurred: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        conn.close()
        print("\n✓ Database connection closed")


if __name__ == "__main__":
    main()
