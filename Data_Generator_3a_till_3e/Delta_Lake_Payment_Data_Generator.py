import pandas as pd
import random
import os
from datetime import datetime, timedelta
import numpy as np

# ==================== CONFIGURATION SECTION ====================

# === OUTPUT CONFIGURATION ===
# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "generated_data")  # Local path (same folder as script)
# OUTPUT_PATH = "gs://delta-lake-payment-gateway-476820/raw/20241202"  # GCS (commented for now)

# === DATA GENERATION CONFIGURATION ===
ROWS_PER_DAY = 15000
START_DATE = "2024-11-01"  # First day to generate
DAYS_TO_GENERATE = [1, 2, 3, 4, 5,6]  # Modify this list to add Day 6, 7, 8...

# === DATA QUALITY ISSUES (Percentages) ===
TIER1_ISSUES_PCT = 0.67   # NULL transaction_id, duplicates → quarantine
TIER2_ISSUES_PCT = 2.67   # Negative amounts, future timestamps, unknown merchant → flag
TIER3_ISSUES_PCT = 1.33   # NULL device_type, location_type, product_name → fix

LATE_ARRIVAL_COUNT = 50   # Rows per day with old transaction_timestamp
STATUS_UPDATE_COUNT = 100 # Day 4+ only: Update previous days' transactions

# === MASTER DATA ===
NUM_CUSTOMERS = 1000
NUM_MERCHANTS = 500

# ==================== MASTER DATA LISTS ====================

PRODUCT_CATEGORIES = ['Electronics', 'Fashion', 'Food', 'Groceries', 'Travel', 
                      'Entertainment', 'Health', 'Education', 'Bills', 'Others']

MERCHANT_NAMES = [
    "Amazon India", "Flipkart", "Meesho", "Myntra", "Ajio", "Snapdeal",
    "Swiggy", "Zomato", "Dunzo", "BigBasket", "Blinkit",
    "MakeMyTrip", "Goibibo", "Ola", "Uber India", "Redbus",
    "BookMyShow", "Netflix India", "Amazon Prime Video", "Hotstar",
    "Paytm Mall", "PhonePe Store", "Reliance Digital", "Croma",
    "ShopEasy India", "QuickMart", "TechWorld", "FashionHub", "FoodExpress",
    "GroceryKing", "TravelMate", "EntertainPlus", "HealthFirst", "EduPoint"
]

PRODUCTS_BY_CATEGORY = {
    'Electronics': ["Samsung Galaxy S23", "iPhone 15", "OnePlus 11", "MacBook Air M2", 
                    "Dell XPS 13", "Sony WH-1000XM5", "Boat Airdopes", "Samsung 55\" TV"],
    'Fashion': ["Nike Air Max", "Adidas Ultraboost", "Levi's Jeans", "H&M T-Shirt", 
                "Zara Dress", "Woodland Shoes", "Fastrack Watch", "Ray-Ban Sunglasses"],
    'Food': ["Biryani Meal", "Pizza Large", "Burger Combo", "Masala Dosa", 
             "Chicken Tikka", "Paneer Butter Masala", "Pasta Carbonara", "Ice Cream Tub"],
    'Groceries': ["Rice 25kg", "Wheat Flour 10kg", "Cooking Oil 5L", "Sugar 5kg",
                  "Tea Powder 1kg", "Milk 2L", "Eggs Dozen", "Vegetables Basket"],
    'Travel': ["Flight Delhi-Mumbai", "Train Ticket AC", "Bus Ticket Volvo", "Cab Ride 20km",
               "Hotel 1 Night", "Resort Package", "Car Rental Daily", "Metro Card Recharge"],
    'Entertainment': ["Movie Ticket 2", "Concert Pass", "IPL Match Ticket", "Game CD",
                      "Spotify Premium", "YouTube Premium", "Gaming Console", "Book Novel"],
    'Health': ["Medicine Pack", "Vitamin Supplements", "Protein Powder 1kg", "Gym Membership",
               "Yoga Mat", "Fitness Band", "Doctor Consultation", "Medical Checkup"],
    'Education': ["Online Course Fee", "Textbook Set", "School Fee Payment", "Tuition Fee",
                  "Certification Exam", "Workshop Pass", "E-Learning Subscription", "Study Lamp"],
    'Bills': ["Electricity Bill", "Water Bill", "Gas Cylinder", "Mobile Recharge",
              "DTH Recharge", "Broadband Bill", "Credit Card Payment", "Loan EMI"],
    'Others': ["Gift Card", "Donation", "Pet Food", "Plants", "Home Decor Item",
               "Kitchenware", "Tools Set", "Sports Equipment", "Toys Pack", "Cosmetics"]
}

PAYMENT_METHODS = {'UPI': 0.60, 'Credit Card': 0.15, 'Debit Card': 0.15, 
                   'Wallet Balance': 0.07, 'Bank Transfer': 0.03}

TRANSACTION_STATUSES = {'Successful': 0.95, 'Failed': 0.04, 'Pending': 0.01}

DEVICE_TYPES = {'Android': 0.60, 'iOS': 0.30, 'Web': 0.10}

LOCATION_TYPES = {'Urban': 0.70, 'Suburban': 0.20, 'Rural': 0.10}

# ==================== HELPER FUNCTIONS ====================

def get_existing_files(output_path):
    """
    Check which day files already exist
    Returns: list of day numbers [1, 2, 3] if day1-3 exist
    """
    if output_path.startswith("gs://"):
        # GCS check (implement later)
        # Use: gsutil ls gs://bucket/path/day*.csv
        print("⚠️  GCS support not yet implemented. Will generate all files locally.")
        return []
    else:
        # Local check
        existing = []
        if not os.path.exists(output_path):
            return existing
        
        for day in DAYS_TO_GENERATE:
            filepath = os.path.join(output_path, f"day{day}_transactions.csv")
            if os.path.exists(filepath):
                existing.append(day)
        return existing

# def upload_to_gcs(local_file, gcs_path):
#     """
#     Upload file to GCS using gsutil
#     """
#     import subprocess
#     cmd = f"gsutil cp {local_file} {gcs_path}"
#     subprocess.run(cmd, shell=True, check=True)
#     print(f"✅ Uploaded {local_file} to {gcs_path}")

def get_date_for_day(day_num):
    """Get date string for a given day number"""
    base = datetime.strptime(START_DATE, "%Y-%m-%d")
    target = base + timedelta(days=day_num - 1)
    return target.strftime("%Y-%m-%d")

def generate_transaction_id(date_str, sequence):
    clean_date = date_str.replace("-", "")
    return f"TXN_{clean_date}_{sequence:06d}"

def generate_customer_id(num):
    return f"USER_{num:04d}"

def generate_merchant_id(num):
    return f"MERCH_{num:04d}"

def get_random_timestamp(date_str, start_hour=0, end_hour=23):
    base_date = datetime.strptime(date_str, "%Y-%m-%d")
    random_hour = random.randint(start_hour, end_hour)
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)
    return base_date + timedelta(hours=random_hour, minutes=random_minute, seconds=random_second)

def get_merchant_name(merchant_id_num):
    if merchant_id_num < len(MERCHANT_NAMES):
        return MERCHANT_NAMES[merchant_id_num]
    return f"Merchant_{merchant_id_num:04d}"

def get_product_name(category):
    return random.choice(PRODUCTS_BY_CATEGORY[category])

def calculate_fee(amount):
    return round(amount * random.uniform(0.015, 0.03), 2)

def calculate_cashback(amount, status):
    if status == 'Successful':
        return round(amount * random.uniform(0, 0.05), 2)
    return 0.0

def calculate_loyalty_points(amount, status):
    if status == 'Successful':
        return int(amount / random.uniform(10, 20))
    return 0

def weighted_random_choice(choices_dict):
    items = list(choices_dict.keys())
    weights = list(choices_dict.values())
    return random.choices(items, weights=weights, k=1)[0]

def generate_amount():
    amount = np.random.lognormal(mean=7.5, sigma=1.0)
    amount = max(100, min(50000, amount))
    return round(amount, 2)

def format_file_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f}{unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f}TB"

def create_base_record(transaction_id, date_str):
    """Create a base transaction record"""
    customer_id = generate_customer_id(random.randint(1, NUM_CUSTOMERS))
    merchant_id_num = random.randint(1, NUM_MERCHANTS)
    merchant_id = generate_merchant_id(merchant_id_num)
    
    transaction_timestamp = get_random_timestamp(date_str)
    merchant_name = get_merchant_name(merchant_id_num - 1)
    product_category = random.choice(PRODUCT_CATEGORIES)
    product_name = get_product_name(product_category)
    
    amount = generate_amount()
    fee_amount = calculate_fee(amount)
    transaction_status = weighted_random_choice(TRANSACTION_STATUSES)
    cashback_amount = calculate_cashback(amount, transaction_status)
    loyalty_points = calculate_loyalty_points(amount, transaction_status)
    
    payment_method = weighted_random_choice(PAYMENT_METHODS)
    device_type = weighted_random_choice(DEVICE_TYPES)
    location_type = weighted_random_choice(LOCATION_TYPES)
    
    return {
        'transaction_id': transaction_id,
        'customer_id': customer_id,
        'transaction_timestamp': transaction_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        'merchant_id': merchant_id,
        'merchant_name': merchant_name,
        'product_category': product_category,
        'product_name': product_name,
        'amount': amount,
        'fee_amount': fee_amount,
        'cashback_amount': cashback_amount,
        'loyalty_points': loyalty_points,
        'payment_method': payment_method,
        'transaction_status': transaction_status,
        'device_type': device_type,
        'location_type': location_type,
        'currency': 'INR',
        'updated_at': transaction_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    }

# ==================== TRANSACTION ID STORAGE ====================

all_transaction_ids = {}  # {day_num: [txid1, txid2, ...]}

# ==================== DAY-SPECIFIC GENERATION ====================

def generate_day1_data(day_num, date_str):
    """Generate Day 1 data with specific issues"""
    print(f"\n🔄 Generating Day {day_num} ({date_str}) - {ROWS_PER_DAY:,} rows...")
    
    null_txid = 100
    negative_amt = 200
    null_device = 200
    clean = ROWS_PER_DAY - null_txid - negative_amt - null_device
    
    data = []
    txids = []
    counter = 1
    
    # Clean records
    for i in range(clean):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        data.append(record)
        txids.append(txid)
        counter += 1
    
    # Tier 1: NULL transaction_id
    print(f"   ⚠️  Tier 1: {null_txid} NULL transaction_id")
    for i in range(null_txid):
        record = create_base_record(None, date_str)
        data.append(record)
    
    # Tier 2: Negative amounts
    print(f"   ⚠️  Tier 2: {negative_amt} negative amounts")
    for i in range(negative_amt):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['amount'] = -1 * abs(record['amount'])
        record['cashback_amount'] = 0.0
        record['loyalty_points'] = 0
        data.append(record)
        txids.append(txid)
        counter += 1
    
    # Tier 3: NULL device_type
    print(f"   ⚠️  Tier 3: {null_device} NULL device_type")
    for i in range(null_device):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['device_type'] = None
        data.append(record)
        txids.append(txid)
        counter += 1
    
    all_transaction_ids[day_num] = txids
    return pd.DataFrame(data)

def generate_day2_data(day_num, date_str):
    """Generate Day 2 data with specific issues"""
    print(f"\n🔄 Generating Day {day_num} ({date_str}) - {ROWS_PER_DAY:,} rows...")
    
    dup_txid = 50
    future_ts = 150
    null_loc = 100
    clean = ROWS_PER_DAY - dup_txid - future_ts - null_loc
    
    data = []
    txids = []
    counter = 1
    
    # Clean records
    for i in range(clean):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        data.append(record)
        txids.append(txid)
        counter += 1
    
    # Tier 1: Duplicate transaction_ids
    print(f"   ⚠️  Tier 1: {dup_txid} duplicate transaction_ids")
    if 1 in all_transaction_ids:
        for i in range(dup_txid):
            dup_txid_val = random.choice(all_transaction_ids[1])
            record = create_base_record(dup_txid_val, date_str)
            data.append(record)
    
    # Tier 2: Future timestamps
    print(f"   ⚠️  Tier 2: {future_ts} future timestamps")
    for i in range(future_ts):
        txid = generate_transaction_id(date_str, counter)
        future_date = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
        record = create_base_record(txid, future_date)
        data.append(record)
        txids.append(txid)
        counter += 1
    
    # Tier 3: NULL location_type
    print(f"   ⚠️  Tier 3: {null_loc} NULL location_type")
    for i in range(null_loc):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['location_type'] = None
        data.append(record)
        txids.append(txid)
        counter += 1
    
    all_transaction_ids[day_num] = txids
    return pd.DataFrame(data)

def generate_day3_data(day_num, date_str):
    """Generate Day 3 data with specific issues"""
    print(f"\n🔄 Generating Day {day_num} ({date_str}) - {ROWS_PER_DAY:,} rows...")
    
    late_arr = LATE_ARRIVAL_COUNT
    unknown_merch = 100
    null_prod = 50
    clean = ROWS_PER_DAY - late_arr - unknown_merch - null_prod
    
    data = []
    txids = []
    counter = 1
    
    # Clean records
    for i in range(clean):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        data.append(record)
        txids.append(txid)
        counter += 1
    
    # Late arrivals
    print(f"   ⚠️  {late_arr} late arrivals (txn_date=Day 2)")
    prev_date = get_date_for_day(day_num - 1)
    for i in range(late_arr):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, prev_date)
        record['updated_at'] = get_random_timestamp(date_str).strftime("%Y-%m-%d %H:%M:%S")
        data.append(record)
        txids.append(txid)
        counter += 1
    
    # Tier 2: Unknown merchant_id
    print(f"   ⚠️  Tier 2: {unknown_merch} unknown merchant_ids")
    for i in range(unknown_merch):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['merchant_id'] = f"MERCH_{random.randint(9000, 9999)}"
        record['merchant_name'] = "Unknown Merchant"
        data.append(record)
        txids.append(txid)
        counter += 1
    
    # Tier 3: NULL product_name
    print(f"   ⚠️  Tier 3: {null_prod} NULL product_name")
    for i in range(null_prod):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['product_name'] = None
        data.append(record)
        txids.append(txid)
        counter += 1
    
    all_transaction_ids[day_num] = txids
    return pd.DataFrame(data)

def generate_day4plus_data(day_num, date_str):
    """Generate Day 4+ data with late arrivals and CDC updates"""
    print(f"\n🔄 Generating Day {day_num} ({date_str}) - {ROWS_PER_DAY:,} rows...")
    
    late_arr = LATE_ARRIVAL_COUNT
    cdc_updates = STATUS_UPDATE_COUNT
    clean = ROWS_PER_DAY - late_arr - cdc_updates
    
    data = []
    txids = []
    counter = 1
    
    # Clean records
    for i in range(clean):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        data.append(record)
        txids.append(txid)
        counter += 1
    
    # Late arrivals
    print(f"   ⚠️  {late_arr} late arrivals (txn_date=Day {day_num-1})")
    prev_date = get_date_for_day(day_num - 1)
    for i in range(late_arr):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, prev_date)
        record['updated_at'] = get_random_timestamp(date_str).strftime("%Y-%m-%d %H:%M:%S")
        data.append(record)
        txids.append(txid)
        counter += 1
    
    # CDC Updates
    print(f"   ⚠️  {cdc_updates} CDC updates (status changes from previous days)")
    prev_txids = []
    for prev_day in range(1, day_num):
        if prev_day in all_transaction_ids:
            prev_txids.extend(all_transaction_ids[prev_day])
    
    if prev_txids:
        selected = random.sample(prev_txids, min(cdc_updates, len(prev_txids)))
        for txid in selected:
            record = create_base_record(txid, date_str)
            record['transaction_status'] = 'Successful'
            record['updated_at'] = get_random_timestamp(date_str).strftime("%Y-%m-%d %H:%M:%S")
            if record['merchant_name'] in ["Amazon India", "Flipkart", "Swiggy"]:
                record['merchant_name'] += " Pvt Ltd"
            data.append(record)
    
    all_transaction_ids[day_num] = txids
    return pd.DataFrame(data)

def generate_day_data(day_num, date_str):
    """Route to appropriate day generation function"""
    if day_num == 1:
        return generate_day1_data(day_num, date_str)
    elif day_num == 2:
        return generate_day2_data(day_num, date_str)
    elif day_num == 3:
        return generate_day3_data(day_num, date_str)
    else:
        return generate_day4plus_data(day_num, date_str)

# ==================== VALIDATION & SAVING ====================

def save_day_file(df, day_num, output_path):
    """Save day file to output path"""
    os.makedirs(output_path, exist_ok=True)
    filepath = os.path.join(output_path, f"day{day_num}_transactions.csv")
    df.to_csv(filepath, index=False, encoding='utf-8')
    file_size = os.path.getsize(filepath)
    print(f"   ✅ day{day_num}_transactions.csv saved ({format_file_size(file_size)})")
    return filepath, file_size

def print_day_statistics(df, day_num, date_str):
    """Print validation statistics for a day"""
    print(f"\n{'='*70}")
    print(f"Day {day_num} Statistics ({date_str})")
    print(f"{'='*70}")
    print(f"Total rows: {len(df):,}")
    print(f"NULL transaction_id: {df['transaction_id'].isna().sum()}")
    print(f"Duplicate transaction_id: {df['transaction_id'].duplicated().sum()}")
    print(f"Negative amounts: {(df['amount'] < 0).sum()}")
    print(f"NULL device_type: {df['device_type'].isna().sum()}")
    print(f"NULL location_type: {df['location_type'].isna().sum()}")
    print(f"NULL product_name: {df['product_name'].isna().sum()}")
    
    tier1 = df['transaction_id'].isna().sum() + df['transaction_id'].duplicated().sum()
    tier2 = (df['amount'] < 0).sum()
    tier3 = df['device_type'].isna().sum() + df['location_type'].isna().sum() + df['product_name'].isna().sum()
    clean = len(df) - tier1 - tier2 - tier3
    
    print(f"\nClean records: {clean:,}")
    print(f"Tier 1 issues (quarantine): {tier1}")
    print(f"Tier 2 issues (flag): {tier2}")
    print(f"Tier 3 issues (fix): {tier3}")

def write_generation_log(days_generated, days_skipped, file_info, output_path):
    """Write generation log"""
    log_path = os.path.join(output_path, "generation_log.txt")
    
    with open(log_path, 'w', encoding='utf-8') as f:
        f.write("="*70 + "\n")
        f.write("PAYMENT GATEWAY DATA GENERATOR - LOG\n")
        f.write("="*70 + "\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Output path: {output_path}\n")
        f.write(f"Days generated: {days_generated}\n")
        f.write(f"Days skipped (already exist): {days_skipped}\n\n")
        
        for day_num, date_str, rows, size in file_info:
            f.write(f"Day {day_num} ({date_str}): {rows:,} rows ({format_file_size(size)})\n")
    
    print(f"\n   ✅ generation_log.txt saved")

# ==================== MAIN EXECUTION ====================

def main():
    print("="*70)
    print("🚀 STATEFUL PAYMENT GATEWAY DATA GENERATOR")
    print("="*70)
    print(f"\nConfiguration:")
    print(f"  - Output Path: {OUTPUT_PATH}")
    print(f"  - Rows Per Day: {ROWS_PER_DAY:,}")
    print(f"  - Start Date: {START_DATE}")
    print(f"  - Days to Generate: {DAYS_TO_GENERATE}")
    
    # Check existing files
    print(f"\n🔍 Checking for existing files...")
    existing_days = get_existing_files(OUTPUT_PATH)
    days_to_create = [d for d in DAYS_TO_GENERATE if d not in existing_days]
    
    if existing_days:
        print(f"   ✅ Found existing files for days: {existing_days}")
    
    if not days_to_create:
        print("\n✅ All files already exist. Nothing to generate.")
        return
    
    print(f"   📝 Will generate days: {days_to_create}")
    
    # Set random seed
    random.seed(42)
    np.random.seed(42)
    
    start_time = datetime.now()
    file_info = []
    
    # Generate missing days
    for day_num in days_to_create:
        date_str = get_date_for_day(day_num)
        df = generate_day_data(day_num, date_str)
        filepath, file_size = save_day_file(df, day_num, OUTPUT_PATH)
        print_day_statistics(df, day_num, date_str)
        file_info.append((day_num, date_str, len(df), file_size))
    
    # Write log
    write_generation_log(days_to_create, existing_days, file_info, OUTPUT_PATH)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "="*70)
    print("🎉 GENERATION COMPLETE!")
    print("="*70)
    print(f"\n⏱️  Execution time: {duration:.2f} seconds")
    print(f"📂 Output location: {OUTPUT_PATH}")
    print(f"\n🎯 Next Steps:")
    print(f"   1. Review files in: {OUTPUT_PATH}")
    print(f"   2. To add more days, modify DAYS_TO_GENERATE = [1,2,3,4,5,6,7,8]")
    print(f"   3. Run script again - it will only generate missing days!")
    
    # if OUTPUT_PATH.startswith("gs://"):
    #     print(f"   4. Files will be uploaded to GCS")
    
    print("\n" + "="*70)

if __name__ == "__main__":
    main()