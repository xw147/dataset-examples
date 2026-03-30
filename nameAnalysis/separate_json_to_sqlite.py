#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Convert separate Yelp JSON files to SQLite database for exploration with DB Browser.
This converts ALL records from the JSON files to SQLite format.

Usage:
    python separate_json_to_sqlite.py yelp_dataset full_yelp.db
"""

import json
import sqlite3
import sys
import os
from collections import defaultdict

def create_tables(conn):
    """Create tables for businesses, reviews, and users."""
    
    # Business table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS businesses (
            business_id TEXT PRIMARY KEY,
            name TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            latitude REAL,
            longitude REAL,
            stars REAL,
            review_count INTEGER,
            is_open INTEGER,
            categories TEXT,
            attributes TEXT,
            hours TEXT
        )
    ''')
    
    # Reviews table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS reviews (
            review_id TEXT PRIMARY KEY,
            user_id TEXT,
            business_id TEXT,
            stars INTEGER,
            useful INTEGER,
            funny INTEGER,
            cool INTEGER,
            text TEXT,
            date TEXT
        )
    ''')
    
    # Users table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            name TEXT,
            review_count INTEGER,
            yelping_since TEXT,
            useful INTEGER,
            funny INTEGER,
            cool INTEGER,
            elite TEXT,
            friends TEXT,
            fans INTEGER,
            average_stars REAL,
            compliment_hot INTEGER,
            compliment_more INTEGER,
            compliment_profile INTEGER,
            compliment_cute INTEGER,
            compliment_list INTEGER,
            compliment_note INTEGER,
            compliment_plain INTEGER,
            compliment_cool INTEGER,
            compliment_funny INTEGER,
            compliment_writer INTEGER,
            compliment_photos INTEGER
        )
    ''')
    
    conn.commit()

def insert_business(conn, data):
    """Insert business record into database."""
    try:
        conn.execute('''
            INSERT OR REPLACE INTO businesses 
            (business_id, name, address, city, state, postal_code, latitude, longitude, 
             stars, review_count, is_open, categories, attributes, hours)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('business_id'),
            data.get('name'),
            data.get('address'),
            data.get('city'),
            data.get('state'),
            data.get('postal_code'),
            data.get('latitude'),
            data.get('longitude'),
            data.get('stars'),
            data.get('review_count'),
            data.get('is_open'),
            data.get('categories'),
            json.dumps(data.get('attributes', {})) if data.get('attributes') else None,
            json.dumps(data.get('hours', {})) if data.get('hours') else None
        ))
    except Exception as e:
        print(f"Error inserting business: {e}")

def insert_review(conn, data):
    """Insert review record into database."""
    try:
        conn.execute('''
            INSERT OR REPLACE INTO reviews 
            (review_id, user_id, business_id, stars, useful, funny, cool, text, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('review_id'),
            data.get('user_id'),
            data.get('business_id'),
            data.get('stars'),
            data.get('useful'),
            data.get('funny'),
            data.get('cool'),
            data.get('text'),
            data.get('date')
        ))
    except Exception as e:
        print(f"Error inserting review: {e}")

def insert_user(conn, data):
    """Insert user record into database."""
    try:
        conn.execute('''
            INSERT OR REPLACE INTO users 
            (user_id, name, review_count, yelping_since, useful, funny, cool, elite, friends, 
             fans, average_stars, compliment_hot, compliment_more, compliment_profile, 
             compliment_cute, compliment_list, compliment_note, compliment_plain, 
             compliment_cool, compliment_funny, compliment_writer, compliment_photos)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('user_id'),
            data.get('name'),
            data.get('review_count'),
            data.get('yelping_since'),
            data.get('useful'),
            data.get('funny'),
            data.get('cool'),
            json.dumps(data.get('elite', [])) if data.get('elite') else None,
            json.dumps(data.get('friends', [])) if data.get('friends') else None,
            data.get('fans'),
            data.get('average_stars'),
            data.get('compliment_hot'),
            data.get('compliment_more'),
            data.get('compliment_profile'),
            data.get('compliment_cute'),
            data.get('compliment_list'),
            data.get('compliment_note'),
            data.get('compliment_plain'),
            data.get('compliment_cool'),
            data.get('compliment_funny'),
            data.get('compliment_writer'),
            data.get('compliment_photos')
        ))
    except Exception as e:
        print(f"Error inserting user: {e}")

def process_file(conn, file_path, data_type, max_records=None):
    """Process a single JSON file and insert records into database."""
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return 0
    
    print(f"Processing {data_type} file: {file_path}")
    if max_records:
        print(f"  Limited to {max_records:,} records")
    else:
        print(f"  Processing ALL records")
    
    count = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if max_records and count >= max_records:
                print(f"Reached maximum records ({max_records}) for {data_type}")
                break
                
            if line_num % 50000 == 0:
                print(f"  Processed {line_num:,} lines, inserted {count:,} records...")
                conn.commit()
            
            try:
                data = json.loads(line.strip())
                
                if data_type == 'business':
                    insert_business(conn, data)
                elif data_type == 'review':
                    insert_review(conn, data)
                elif data_type == 'user':
                    insert_user(conn, data)
                
                count += 1
                
            except json.JSONDecodeError as e:
                print(f"Error parsing line {line_num}: {e}")
                continue
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")
                continue
    
    conn.commit()
    print(f"Completed {data_type}: {count:,} records inserted")
    return count

def create_views(conn):
    """Create useful views for data exploration."""
    
    print("Creating useful views...")
    
    # Chinese restaurants view
    conn.execute('''
        CREATE VIEW IF NOT EXISTS chinese_restaurants AS
        SELECT * FROM businesses 
        WHERE categories IS NOT NULL AND (
            LOWER(categories) LIKE '%restaurant%' AND
            LOWER(categories) LIKE '%chinese%' 
        )
    ''')
    
    # Restaurant categories view
    conn.execute('''
        CREATE VIEW IF NOT EXISTS restaurants AS
        SELECT * FROM businesses 
        WHERE categories IS NOT NULL AND 
              LOWER(categories) LIKE '%restaurant%'
    ''')
    
    
    # Chinese restaurant reviews view (complete dataset)
    conn.execute('''
        CREATE VIEW IF NOT EXISTS chinese_restaurant_reviews AS
        SELECT 
            r.review_id,
            r.stars as review_stars,
            r.text as review_text,
            r.date as review_date,
            u.name as user_name,
            u.review_count as user_total_reviews,
            u.average_stars as user_avg_stars,
            b.name as business_name,
            b.city as business_city,
            b.state as business_state,
            b.categories as business_categories,
            b.stars as business_avg_stars,
            b.address as business_address
        FROM reviews r
        JOIN businesses b ON r.business_id = b.business_id
        JOIN users u ON r.user_id = u.user_id
        WHERE b.categories IS NOT NULL AND (
            LOWER(b.categories) LIKE '%chinese%' 
        )
    ''')
    
    conn.commit()

def convert_separate_json_to_sqlite(dataset_folder, output_db, max_records_per_type=None):
    """Convert separate Yelp JSON files to SQLite database."""
    
    # File paths
    business_file = os.path.join(dataset_folder, 'yelp_academic_dataset_business.json')
    review_file = os.path.join(dataset_folder, 'yelp_academic_dataset_review.json')
    user_file = os.path.join(dataset_folder, 'yelp_academic_dataset_user.json')
    
    # Check if files exist
    print("Checking for files...")
    for file_path, name in [(business_file, 'Business'), (review_file, 'Review'), (user_file, 'User')]:
        if os.path.exists(file_path):
            print(f"✓ Found {name} file: {file_path}")
        else:
            print(f"✗ Missing {name} file: {file_path}")
    
    # Connect to SQLite database
    conn = sqlite3.connect(output_db)
    create_tables(conn)
    
    print(f"\nConverting Yelp dataset files to {output_db}...")
    if max_records_per_type:
        print(f"Limiting to {max_records_per_type:,} records per type for browsing")
    else:
        print("Processing ALL records (this may take a while for large datasets)")
    print()
    
    # Track record counts
    counts = {}
    
    # Process each file
    counts['business'] = process_file(conn, business_file, 'business', max_records_per_type)
    counts['review'] = process_file(conn, review_file, 'review', max_records_per_type)
    counts['user'] = process_file(conn, user_file, 'user', max_records_per_type)
    
    # Create views
    create_views(conn)
    
    conn.close()
    
    print(f"\nConversion complete! Database saved as: {output_db}")
    print("\nRecord counts:")
    for data_type, count in counts.items():
        print(f"  {data_type}: {count:,}")
    
    print(f"\nYou can now open {output_db} with DB Browser for SQLite!")
    print("\nUseful tables and views created:")
    print("  Tables:")
    print("    - businesses: All business data")
    print("    - reviews: All review data")
    print("    - users: All user data")
    print("  Views:")
    print("    - chinese_restaurants: All Chinese restaurants")
    print("    - restaurants: All restaurants")
    print("    - reviews_with_business: Reviews joined with business info")
    print("    - chinese_restaurant_reviews: Complete data for Chinese restaurant reviews")
    
    print(f"\nSample queries to try in DB Browser:")
    print("  SELECT COUNT(*) FROM chinese_restaurants;")
    print("  SELECT city, COUNT(*) FROM chinese_restaurants GROUP BY city ORDER BY COUNT(*) DESC LIMIT 10;")
    print("  SELECT * FROM chinese_restaurant_reviews LIMIT 10;")

if __name__ == '__main__':
    if len(sys.argv) not in [3, 4]:
        print("Usage: python separate_json_to_sqlite.py <dataset_folder> <output.db> [max_records]")
        print("Example: python separate_json_to_sqlite.py yelp_dataset full_yelp.db")
        print("Example: python separate_json_to_sqlite.py yelp_dataset sample.db 10000")
        sys.exit(1)
    
    dataset_folder = sys.argv[1]
    output_db = sys.argv[2]
    max_records = int(sys.argv[3]) if len(sys.argv) == 4 else None
    
    convert_separate_json_to_sqlite(dataset_folder, output_db, max_records)

    #Usage: type in terminal
