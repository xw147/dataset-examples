"""
Country of origin estimator using Namsor API.

Filters preprocessed user names and calls the Namsor origin API
to estimate country of origin based on first and last names.

Usage:
    python name_origin.py <input_csv> [api_key] [--output output.csv] [--batch-size 100]

Example:
    python name_origin.py unique_user_names_preprocessed.csv

The script will create output CSV with country of origin predictions.
"""

import csv
import requests
import json
import argparse
import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import time
from dotenv import load_dotenv

# Namsor API endpoint
NAMSOR_API_URL = "https://v2.namsor.com/NamSorAPIv2/api2/json/originBatch"


def read_all_data(csv_path: str) -> List[Dict[str, str]]:
    """
    Read all rows from the CSV file.

    Args:
        csv_path: Path to the input CSV file

    Returns:
        List of dictionaries with all rows
    """
    all_rows = []

    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_rows.append(row)
    except FileNotFoundError:
        print(f"Error: File '{csv_path}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        sys.exit(1)

    send_count = sum(1 for r in all_rows if r.get('send_to_tool', '').lower() == 'yes')
    print(f"Read {len(all_rows)} total rows ({send_count} with send_to_tool='yes')")
    return all_rows


def prepare_api_batch(all_rows: List[Dict[str, str]], batch_size: int = 100) -> List[List[Dict[str, Any]]]:
    """
    Prepare batches of names for API calls (Namsor accepts max 100 per request).
    Only rows with send_to_tool='yes' are included.

    Note: Unlike the diaspora endpoint, the origin endpoint accepts firstName
    and lastName as both optional, so rows without a last name are still sent.

    Args:
        all_rows: List of all data rows (uses original row index as ID)
        batch_size: Number of records per API call (max 100)

    Returns:
        List of batches, where each batch is a list of personal name objects
    """
    # Collect names to send, using the original row index as the ID
    names_to_send = []
    for idx, row in enumerate(all_rows):
        if row.get('send_to_tool', '').lower() != 'yes':
            continue

        first_name = row.get('first_name', '').strip()
        last_name = row.get('last_name', '').strip()

        # Origin API accepts firstName and lastName as both optional,
        # but at least one should be present
        if not first_name and not last_name:
            print(f"Warning: Skipping row {idx} - no first or last name found")
            continue

        names_to_send.append({
            "id": f"{idx}",
            "firstName": first_name,
            "lastName": last_name,
        })

    # Split into batches
    batches = []
    for i in range(0, len(names_to_send), batch_size):
        batches.append(names_to_send[i:i + batch_size])

    print(f"Prepared {len(batches)} batch(es) for API calls ({len(names_to_send)} names)")
    return batches


def call_namsor_api(batch: List[Dict[str, Any]], api_key: str) -> Optional[Dict[str, Any]]:
    """
    Call the Namsor origin API with a batch of names.

    Args:
        batch: List of personal name objects
        api_key: Namsor API key

    Returns:
        API response as dictionary, or None if request failed
    """
    headers = {
        "X-API-KEY": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    payload = {
        "personalNames": batch
    }

    try:
        response = requests.post(NAMSOR_API_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 401:
            print("Error: Invalid API key (401 Unauthorized)")
        elif response.status_code == 403:
            print("Error: Insufficient credits or permission denied (403)")
        else:
            print(f"HTTP Error {response.status_code}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error calling API: {e}")
        return None


def process_all_batches(batches: List[List[Dict[str, Any]]], api_key: str) -> List[Dict[str, Any]]:
    """
    Process all batches and collect results.

    Args:
        batches: List of batches prepared for API
        api_key: Namsor API key

    Returns:
        List of all personal names with origin predictions
    """
    all_results = []

    for batch_num, batch in enumerate(batches, 1):
        print(f"Processing batch {batch_num}/{len(batches)}...")
        response = call_namsor_api(batch, api_key)

        if response and "personalNames" in response:
            all_results.extend(response["personalNames"])
            print(f"  - Got {len(response['personalNames'])} results")
        else:
            print(f"  - Batch {batch_num} failed")
            return []

        # Add slight delay between requests to avoid rate limiting
        if batch_num < len(batches):
            time.sleep(0.5)

    print(f"Total results received: {len(all_results)}")
    return all_results


def merge_results_with_original(all_rows: List[Dict[str, str]],
                               api_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge ALL original rows with API results.
    Rows that were sent to the API get populated origin columns;
    rows that were not sent get blank origin columns.

    Args:
        all_rows: All rows from the original CSV
        api_results: Results from Namsor API (keyed by original row index)

    Returns:
        Merged list with all rows, origin data filled where available
    """
    # Create a mapping from original row index to API result
    result_map = {int(r.get("id", -1)): r for r in api_results}

    origin_fields = [
        'country_origin', 'country_origin_alt', 'countries_origin_top', 'score',
        'region_origin', 'top_region_origin', 'sub_region_origin',
        'probability_calibrated', 'probability_alt_calibrated', 'script'
    ]

    merged = []
    for idx, row in enumerate(all_rows):
        merged_row = dict(row)  # Copy original row

        if idx in result_map:
            api_result = result_map[idx]
            merged_row['country_origin'] = api_result.get('countryOrigin', '')
            merged_row['country_origin_alt'] = api_result.get('countryOriginAlt', '')
            merged_row['countries_origin_top'] = json.dumps(api_result.get('countriesOriginTop', []))
            merged_row['score'] = api_result.get('score', '')
            merged_row['region_origin'] = api_result.get('regionOrigin', '')
            merged_row['top_region_origin'] = api_result.get('topRegionOrigin', '')
            merged_row['sub_region_origin'] = api_result.get('subRegionOrigin', '')
            merged_row['probability_calibrated'] = api_result.get('probabilityCalibrated', '')
            merged_row['probability_alt_calibrated'] = api_result.get('probabilityAltCalibrated', '')
            merged_row['script'] = api_result.get('script', '')
        else:
            # Blank origin columns for rows not sent to API
            for field in origin_fields:
                merged_row[field] = ''

        merged.append(merged_row)

    return merged


def write_results_to_csv(merged_data: List[Dict[str, Any]], output_path: str):
    """
    Write merged results to output CSV file.

    Args:
        merged_data: List of dictionaries with merged data
        output_path: Path to output CSV file
    """
    if not merged_data:
        print("No data to write")
        return

    # Define fieldnames (original columns + new origin columns)
    origin_fields = [
        'country_origin', 'country_origin_alt', 'countries_origin_top', 'score',
        'region_origin', 'top_region_origin', 'sub_region_origin',
        'probability_calibrated', 'probability_alt_calibrated', 'script'
    ]

    original_fields = list(merged_data[0].keys())
    # Remove origin fields from original fields if they exist
    original_fields = [f for f in original_fields if f not in origin_fields]
    fieldnames = original_fields + origin_fields

    try:
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(merged_data)

        print(f"Results written to: {output_path}")
        print(f"Total records: {len(merged_data)}")
    except Exception as e:
        print(f"Error writing to output file: {e}")
        sys.exit(1)


def main():
    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        description='Estimate country of origin from names using Namsor API'
    )
    parser.add_argument('input_csv', help='Input CSV file with preprocessed names')
    parser.add_argument('api_key', nargs='?', default=None,
                       help='Namsor API key (or use NAMSOR_API_KEY environment variable)')
    parser.add_argument('--output', '-o', help='Output CSV file (default: input_origin.csv)')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size for API calls (max 100, default: 100)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be processed without calling API')

    args = parser.parse_args()

    # Get API key from argument or environment variable
    api_key = args.api_key or os.getenv('NAMSOR_API_KEY')
    if not api_key:
        print("Error: API key not provided")
        print("Provide it as an argument: python name_origin.py input.csv YOUR_API_KEY")
        print("Or set the environment variable: $env:NAMSOR_API_KEY = 'your_api_key'")
        print("Or add NAMSOR_API_KEY=your_key to a .env file in the project root")
        sys.exit(1)

    args.api_key = api_key

    # Validate input file
    if not Path(args.input_csv).exists():
        print(f"Error: Input file '{args.input_csv}' not found")
        sys.exit(1)

    # Set output file
    if args.output:
        output_csv = args.output
    else:
        input_stem = Path(args.input_csv).stem
        output_csv = f"{input_stem}_origin.csv"

    print(f"Input file: {args.input_csv}")
    print(f"Output file: {output_csv}")
    print("-" * 50)

    # Step 1: Read all data
    print("Step 1: Reading data...")
    all_rows = read_all_data(args.input_csv)

    if not all_rows:
        print("No rows found in input file")
        sys.exit(1)

    # Step 2: Prepare batches (only send_to_tool='yes' rows)
    print("\nStep 2: Preparing API batches...")
    batches = prepare_api_batch(all_rows, args.batch_size)

    if args.dry_run:
        print("\n[DRY RUN] Would process the following:")
        for batch_num, batch in enumerate(batches, 1):
            print(f"  Batch {batch_num}: {len(batch)} names")
        print(f"Total names to process: {sum(len(b) for b in batches)}")
        return

    # Step 3: Call API for all batches
    print("\nStep 3: Calling Namsor API (origin)...")
    api_results = process_all_batches(batches, args.api_key)

    if not api_results:
        print("Failed to get results from API")
        sys.exit(1)

    # Step 4: Merge results with ALL original rows
    print("\nStep 4: Merging results with original data...")
    merged_data = merge_results_with_original(all_rows, api_results)

    # Step 5: Write to output CSV
    print("\nStep 5: Writing results...")
    write_results_to_csv(merged_data, output_csv)

    print("\n" + "=" * 50)
    print("Processing completed successfully!")
    print(f"Results saved to: {output_csv}")


if __name__ == '__main__':
    main()
