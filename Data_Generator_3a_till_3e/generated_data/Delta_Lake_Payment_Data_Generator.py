import pandas as pd
import random
import os
from datetime import datetime, timedelta
import numpy as np

# ==================== CONFIGURATION SECTION ====================

# === OUTPUT CONFIGURATION ===
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "generated_data")

# === DATA GENERATION CONFIGURATION ===
ROWS_PER_DAY = 15000

# INITIAL LOAD: Generate 90 days of historical data
DAYS_TO_GENERATE = list(range(1, 102))  # [1, 2, 3, ..., 90]

# INCREMENTAL MODE: Uncomment and modify when adding more days
# DAYS_TO_GENERATE = list(range(1, 94))  # Adds day91, 92, 93

# Auto-calculate start date (Day 1 = N days ago, Day N = today)
START_DATE = (datetime.now() - timedelta(days=len(DAYS_TO_GENERATE))).strftime("%Y-%m-%d")

# === DATA QUALITY ISSUES (Bronze Tiers) ===
TIER1_ISSUES_PCT = 0.67   # NULL transaction_id, NULL amount, NULL timestamp, future timestamp → quarantine
TIER2_ISSUES_PCT = 60.67   # Negative amounts, unknown merchant → flag (data exists)
TIER3_ISSUES_PCT = 1.33   # NULL device_type, location_type, product_name → fix with defaults

# === SILVER-SPECIFIC TEST DATA ===
SOFT_DELETE_COUNT = 50        # Records marked is_deleted=true per day
LATE_ARRIVAL_COUNT = 50       # Rows per day with old transaction_timestamp
STATUS_UPDATE_COUNT = 100     # Day 4+ only: Update previous days' transactions
EXTRA_DUPLICATES_COUNT = 50   # Additional duplicates beyond intra-batch (10)

# === TIME-AWARE INCREMENTAL (Day 4+) ===
FRESH_DATA_PCT = 0.30  # 30% of records have updated_at between watermark and NOW
HISTORICAL_DATA_PCT = 0.70  # 70% random historical timestamps

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
    if not os.path.exists(output_path):
        return []
    
    existing = []
    for day in DAYS_TO_GENERATE:
        filepath = os.path.join(output_path, f"day{day}_transactions.csv")
        if os.path.exists(filepath):
            existing.append(day)
    return existing

def get_date_for_day(day_num):
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
    """Generate random timestamp within a day"""
    base_date = datetime.strptime(date_str, "%Y-%m-%d")
    random_hour = random.randint(start_hour, end_hour)
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)
    return base_date + timedelta(hours=random_hour, minutes=random_minute, seconds=random_second)

def get_fresh_timestamp(start_datetime, end_datetime):
    """Generate timestamp between two datetime objects (for incremental testing)"""
    if start_datetime >= end_datetime:
        return end_datetime
    
    time_diff = (end_datetime - start_datetime).total_seconds()
    random_seconds = random.uniform(0, time_diff)
    return start_datetime + timedelta(seconds=random_seconds)

def get_historical_timestamp(day_num):
    """Generate random historical timestamp (Day 1 to Day day_num-1)"""
    history_days = random.randint(1, max(1, day_num - 1))
    history_date = get_date_for_day(history_days)
    return get_random_timestamp(history_date)

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

def create_base_record(transaction_id, date_str, custom_timestamp=None):
    """Create a base transaction record"""
    customer_id = generate_customer_id(random.randint(1, NUM_CUSTOMERS))
    merchant_id_num = random.randint(1, NUM_MERCHANTS)
    merchant_id = generate_merchant_id(merchant_id_num)
    
    transaction_timestamp = custom_timestamp if custom_timestamp else get_random_timestamp(date_str)
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

# ==================== GLOBAL STORAGE ====================

all_base_records = {}  # {day_num: [full_record_dicts]}

# ==================== TIER CALCULATIONS ====================

def calculate_tier_counts(total_rows):
    """Calculate issue counts based on percentages"""
    tier1 = int(total_rows * (TIER1_ISSUES_PCT / 100))
    tier2 = int(total_rows * (TIER2_ISSUES_PCT / 100))
    tier3 = int(total_rows * (TIER3_ISSUES_PCT / 100))
    
    # Distribute Tier 1 among sub-issues
    tier1_null_txid = tier1 // 4
    tier1_null_amount = tier1 // 4
    tier1_null_ts = tier1 // 4
    tier1_future_ts = tier1 - (tier1_null_txid + tier1_null_amount + tier1_null_ts)
    
    # Distribute Tier 2
    tier2_negative = tier2 // 2
    tier2_unknown_merch = tier2 - tier2_negative
    
    # Distribute Tier 3
    tier3_null_device = tier3 // 3
    tier3_null_loc = tier3 // 3
    tier3_null_prod = tier3 - (tier3_null_device + tier3_null_loc)
    
    return {
        'tier1_null_txid': tier1_null_txid,
        'tier1_null_amount': tier1_null_amount,
        'tier1_null_ts': tier1_null_ts,
        'tier1_future_ts': tier1_future_ts,
        'tier2_negative': tier2_negative,
        'tier2_unknown_merch': tier2_unknown_merch,
        'tier3_null_device': tier3_null_device,
        'tier3_null_loc': tier3_null_loc,
        'tier3_null_prod': tier3_null_prod
    }

# ==================== DAY-SPECIFIC GENERATION ====================

def generate_day1_to_3_data(day_num, date_str):
    """Generate Day 1-3 with Bronze Tier issues (historical data)"""
    print(f"\n🔄 Generating Day {day_num} ({date_str}) - {ROWS_PER_DAY:,} rows...")
    
    tier_counts = calculate_tier_counts(ROWS_PER_DAY)
    
    # Calculate clean records
    total_issues = sum(tier_counts.values())
    if day_num == 3:
        total_issues += LATE_ARRIVAL_COUNT
    clean = ROWS_PER_DAY - total_issues
    
    data = []
    base_records = []
    counter = 1
    
    # Clean records
    for i in range(clean):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        data.append(record)
        base_records.append(record.copy())
        counter += 1
    
    # === BRONZE TIER 1 ISSUES (Quarantine) ===
    print(f"   ⚠️  [BRONZE TIER 1] Quarantine Issues:")
    
    # NULL transaction_id
    print(f"      - {tier_counts['tier1_null_txid']} NULL transaction_id")
    for i in range(tier_counts['tier1_null_txid']):
        record = create_base_record(None, date_str)
        data.append(record)
    
    # NULL amount
    print(f"      - {tier_counts['tier1_null_amount']} NULL amount")
    for i in range(tier_counts['tier1_null_amount']):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['amount'] = None
        data.append(record)
        counter += 1
    
    # NULL transaction_timestamp
    print(f"      - {tier_counts['tier1_null_ts']} NULL transaction_timestamp")
    for i in range(tier_counts['tier1_null_ts']):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['transaction_timestamp'] = None
        data.append(record)
        counter += 1
    
    # Future timestamps (Day 2+)
    if day_num >= 2:
        print(f"      - {tier_counts['tier1_future_ts']} Future timestamps")
        for i in range(tier_counts['tier1_future_ts']):
            txid = generate_transaction_id(date_str, counter)
            future_date = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=random.randint(1, 7))).strftime("%Y-%m-%d")
            record = create_base_record(txid, future_date)
            data.append(record)
            base_records.append(record.copy())
            counter += 1
    
    # === BRONZE TIER 2 ISSUES (Flag - data exists but not flagged) ===
    print(f"   ⚠️  [BRONZE TIER 2] Business Rule Violations (not flagged yet):")
    
    # Negative amounts
    print(f"      - {tier_counts['tier2_negative']} Negative amounts")
    for i in range(tier_counts['tier2_negative']):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['amount'] = -1 * abs(record['amount'])
        record['cashback_amount'] = 0.0
        record['loyalty_points'] = 0
        data.append(record)
        base_records.append(record.copy())
        counter += 1
    
    # Unknown merchant (Day 3+)
    if day_num >= 3:
        print(f"      - {tier_counts['tier2_unknown_merch']} Unknown merchant_id")
        for i in range(tier_counts['tier2_unknown_merch']):
            txid = generate_transaction_id(date_str, counter)
            record = create_base_record(txid, date_str)
            record['merchant_id'] = f"MERCH_{random.randint(9000, 9999)}"
            record['merchant_name'] = "Unknown Merchant"
            data.append(record)
            base_records.append(record.copy())
            counter += 1
    
    # === BRONZE TIER 3 ISSUES (Fix with defaults) ===
    print(f"   ⚠️  [BRONZE TIER 3] Missing Optional Data (will be fixed):")
    
    # NULL device_type
    print(f"      - {tier_counts['tier3_null_device']} NULL device_type")
    for i in range(tier_counts['tier3_null_device']):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['device_type'] = None
        data.append(record)
        base_records.append(record.copy())
        counter += 1
    
    # NULL location_type
    print(f"      - {tier_counts['tier3_null_loc']} NULL location_type")
    for i in range(tier_counts['tier3_null_loc']):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['location_type'] = None
        data.append(record)
        base_records.append(record.copy())
        counter += 1
    
    # NULL product_name
    print(f"      - {tier_counts['tier3_null_prod']} NULL product_name")
    for i in range(tier_counts['tier3_null_prod']):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['product_name'] = None
        data.append(record)
        base_records.append(record.copy())
        counter += 1
    
    # === LATE ARRIVALS (Day 3+) ===
    if day_num >= 3:
        print(f"   ⚠️  [CDC] {LATE_ARRIVAL_COUNT} Late arrivals (txn_date=Day {day_num-1})")
        prev_date = get_date_for_day(day_num - 1)
        for i in range(LATE_ARRIVAL_COUNT):
            txid = generate_transaction_id(date_str, counter)
            record = create_base_record(txid, prev_date)
            record['updated_at'] = get_random_timestamp(date_str).strftime("%Y-%m-%d %H:%M:%S")
            data.append(record)
            base_records.append(record.copy())
            counter += 1
    
    all_base_records[day_num] = base_records
    return pd.DataFrame(data)

def generate_day4plus_data(day_num, date_str):
    """Generate Day 4+ with CDC + Silver test data + TIME-AWARE fresh data"""
    print(f"\n🔄 Generating Day {day_num} ({date_str}) - {ROWS_PER_DAY:,} rows...")
    print(f"   📅 TIME-AWARE MODE: {int(FRESH_DATA_PCT*100)}% fresh data (watermark → NOW)")
    
    tier_counts = calculate_tier_counts(ROWS_PER_DAY)
    
    # Calculate counts
    total_special = (
        sum(tier_counts.values()) + 
        LATE_ARRIVAL_COUNT + 
        STATUS_UPDATE_COUNT + 
        SOFT_DELETE_COUNT + 
        EXTRA_DUPLICATES_COUNT + 
        10  # intra-batch duplicates
    )
    
    clean = ROWS_PER_DAY - total_special
    fresh_count = int(clean * FRESH_DATA_PCT)
    historical_count = clean - fresh_count
    
    data = []
    base_records = []
    counter = 1
    
    # Get watermark date (yesterday for fresh data calculation)
    watermark_date = datetime.strptime(get_date_for_day(day_num - 1), "%Y-%m-%d")
    now_datetime = datetime.now()
    
    # === FRESH DATA (30% - Between watermark and NOW) ===
    print(f"   ✅ {fresh_count} FRESH records (updated_at between watermark and NOW)")
    for i in range(fresh_count):
        txid = generate_transaction_id(date_str, counter)
        # Transaction happened historically, but updated recently
        hist_ts = get_historical_timestamp(day_num)
        fresh_updated_ts = get_fresh_timestamp(watermark_date, now_datetime)
        
        record = create_base_record(txid, date_str, custom_timestamp=hist_ts)
        record['updated_at'] = fresh_updated_ts.strftime("%Y-%m-%d %H:%M:%S")
        data.append(record)
        base_records.append(record.copy())
        counter += 1
    
    # === HISTORICAL DATA (70% - Random past dates) ===
    print(f"   📦 {historical_count} HISTORICAL records (random past dates)")
    for i in range(historical_count):
        txid = generate_transaction_id(date_str, counter)
        hist_ts = get_historical_timestamp(day_num)
        record = create_base_record(txid, date_str, custom_timestamp=hist_ts)
        data.append(record)
        base_records.append(record.copy())
        counter += 1
    
    # === BRONZE TIER ISSUES (same as Day 1-3) ===
    print(f"   ⚠️  [BRONZE TIERS] Same as Day 1-3...")
    
    # Tier 1 issues (abbreviated output)
    for i in range(tier_counts['tier1_null_txid']):
        data.append(create_base_record(None, date_str))
    
    for i in range(tier_counts['tier1_null_amount']):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['amount'] = None
        data.append(record)
        counter += 1
    
    for i in range(tier_counts['tier1_future_ts']):
        txid = generate_transaction_id(date_str, counter)
        future_date = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=random.randint(1, 7))).strftime("%Y-%m-%d")
        record = create_base_record(txid, future_date)
        data.append(record)
        counter += 1
    
    # Tier 2 & 3 (abbreviated)
    for i in range(tier_counts['tier2_negative']):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['amount'] = -1 * abs(record['amount'])
        data.append(record)
        base_records.append(record.copy())
        counter += 1
    
    for i in range(tier_counts['tier3_null_device']):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, date_str)
        record['device_type'] = None
        data.append(record)
        base_records.append(record.copy())
        counter += 1
    
    # === CDC UPDATES ===
    print(f"   ⚠️  [CDC] {STATUS_UPDATE_COUNT} Status updates from previous days")
    prev_records = []
    for prev_day in range(1, day_num):
        if prev_day in all_base_records:
            prev_records.extend(all_base_records[prev_day])
    
    if prev_records:
        selected = random.sample(prev_records, min(STATUS_UPDATE_COUNT, len(prev_records)))
        for orig_record in selected:
            record = orig_record.copy()
            record['transaction_status'] = 'Successful'
            fresh_ts = get_fresh_timestamp(watermark_date, now_datetime)
            record['transaction_timestamp'] = fresh_ts.strftime("%Y-%m-%d %H:%M:%S")
            record['updated_at'] = fresh_ts.strftime("%Y-%m-%d %H:%M:%S")
            
            if record['merchant_name'] in ["Amazon India", "Flipkart", "Swiggy"]:
                record['merchant_name'] += " Pvt Ltd"
            
            data.append(record)
    
    # === LATE ARRIVALS ===
    print(f"   ⚠️  [CDC] {LATE_ARRIVAL_COUNT} Late arrivals")
    prev_date = get_date_for_day(day_num - 1)
    for i in range(LATE_ARRIVAL_COUNT):
        txid = generate_transaction_id(date_str, counter)
        record = create_base_record(txid, prev_date)
        fresh_ts = get_fresh_timestamp(watermark_date, now_datetime)
        record['updated_at'] = fresh_ts.strftime("%Y-%m-%d %H:%M:%S")
        data.append(record)
        base_records.append(record.copy())
        counter += 1
    
    # === SILVER TIER 1: SOFT DELETES ===
    print(f"   ⚠️  [SILVER TIER 1] {SOFT_DELETE_COUNT} Soft deleted records (for Silver testing)")
    if prev_records and len(prev_records) >= SOFT_DELETE_COUNT:
        soft_delete_candidates = random.sample(prev_records, SOFT_DELETE_COUNT)
        for orig_record in soft_delete_candidates:
            record = orig_record.copy()
            record['transaction_status'] = 'Failed'  # Mark as failed before delete
            fresh_ts = get_fresh_timestamp(watermark_date, now_datetime)
            record['updated_at'] = fresh_ts.strftime("%Y-%m-%d %H:%M:%S")
            # Note: is_deleted will be added by Bronze pipeline, we just prep the data
            data.append(record)
    
    # === SILVER TIER 2: EXTRA DUPLICATES (Beyond intra-batch) ===
    print(f"   ⚠️  [SILVER TIER 2] {EXTRA_DUPLICATES_COUNT} Extra duplicates (Silver dedup testing)")
    if prev_records and len(prev_records) >= EXTRA_DUPLICATES_COUNT:
        extra_dup_candidates = random.sample(prev_records, EXTRA_DUPLICATES_COUNT)
        for orig_record in extra_dup_candidates:
            dup_record = orig_record.copy()
            fresh_ts = get_fresh_timestamp(watermark_date, now_datetime)
            dup_record['updated_at'] = fresh_ts.strftime("%Y-%m-%d %H:%M:%S")
            data.append(dup_record)
    
    # === INTRA-BATCH DUPLICATES (Within same file) ===
    num_intra_dups = 10
    print(f"   ⚠️  [SILVER TIER 3] {num_intra_dups} Intra-batch duplicates")
    
    if len(base_records) >= num_intra_dups:
        intra_dupe_samples = random.sample(base_records, num_intra_dups)
        for orig_record in intra_dupe_samples:
            new_record = orig_record.copy()
            current_ts = datetime.strptime(orig_record['updated_at'], "%Y-%m-%d %H:%M:%S")
            new_record['updated_at'] = (current_ts + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
            data.append(new_record)
    
    all_base_records[day_num] = base_records
    return pd.DataFrame(data)

def generate_day_data(day_num, date_str):
    """Route to appropriate day generation function"""
    if day_num <= 3:
        return generate_day1_to_3_data(day_num, date_str)
    else:
        return generate_day4plus_data(day_num, date_str)

# ==================== SAVING & VALIDATION ====================

def save_day_file(df, day_num, output_path):
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
    print(f"NULL amount: {df['amount'].isna().sum()}")
    print(f"Negative amounts: {(df['amount'] < 0).sum()}")
    print(f"NULL device_type: {df['device_type'].isna().sum()}")
    print(f"NULL location_type: {df['location_type'].isna().sum()}")
    print(f"NULL product_name: {df['product_name'].isna().sum()}")
    
    tier1 = (df['transaction_id'].isna().sum() + 
             df['amount'].isna().sum() + 
             df['transaction_timestamp'].isna().sum())
    tier2 = (df['amount'] < 0).sum()
    tier3 = (df['device_type'].isna().sum() + 
             df['location_type'].isna().sum() + 
             df['product_name'].isna().sum())
    clean = len(df) - tier1 - tier2 - tier3 - df['transaction_id'].duplicated().sum()
    
    print(f"\nClean records: {clean:,}")
    print(f"[BRONZE] Tier 1 issues (quarantine): {tier1}")
    print(f"[BRONZE] Tier 2 issues (flag): {tier2}")
    print(f"[BRONZE] Tier 3 issues (fix): {tier3}")

def write_generation_log(days_generated, days_skipped, file_info, output_path):
    """Write generation log"""
    log_path = os.path.join(output_path, "generation_log.txt")
    
    with open(log_path, 'w', encoding='utf-8') as f:
        f.write("="*70 + "\n")
        f.write("SMART PAYMENT GATEWAY DATA GENERATOR - LOG\n")
        f.write("="*70 + "\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Output path: {output_path}\n")
        f.write(f"Start date: {START_DATE}\n")
        f.write(f"Days generated: {days_generated}\n")
        f.write(f"Days skipped (already exist): {days_skipped}\n\n")
        
        f.write("CONFIGURATION:\n")
        f.write(f"  - Rows per day: {ROWS_PER_DAY:,}\n")
        f.write(f"  - Bronze Tier 1 (quarantine): {TIER1_ISSUES_PCT}%\n")
        f.write(f"  - Bronze Tier 2 (flag): {TIER2_ISSUES_PCT}%\n")
        f.write(f"  - Bronze Tier 3 (fix): {TIER3_ISSUES_PCT}%\n")
        f.write(f"  - Fresh data % (Day 4+): {int(FRESH_DATA_PCT*100)}%\n")
        f.write(f"  - Historical data % (Day 4+): {int(HISTORICAL_DATA_PCT*100)}%\n")
        f.write(f"  - Late arrivals per day: {LATE_ARRIVAL_COUNT}\n")
        f.write(f"  - CDC updates per day (Day 4+): {STATUS_UPDATE_COUNT}\n")
        f.write(f"  - Silver soft deletes per day (Day 4+): {SOFT_DELETE_COUNT}\n")
        f.write(f"  - Silver extra duplicates per day (Day 4+): {EXTRA_DUPLICATES_COUNT}\n\n")
        
        f.write("FILES GENERATED:\n")
        for day_num, date_str, rows, size in file_info:
            f.write(f"Day {day_num} ({date_str}): {rows:,} rows ({format_file_size(size)})\n")
    
    print(f"\n   ✅ generation_log.txt saved")

# ==================== MAIN EXECUTION ====================

def main():
    print("="*70)
    print("🚀 SMART PAYMENT GATEWAY DATA GENERATOR")
    print("   (90-Day Historical + Time-Aware Incremental)")
    print("="*70)
    print(f"\nConfiguration:")
    print(f"  - Output Path: {OUTPUT_PATH}")
    print(f"  - Rows Per Day: {ROWS_PER_DAY:,}")
    print(f"  - Start Date: {START_DATE}")
    print(f"  - Days to Generate: {len(DAYS_TO_GENERATE)} ({min(DAYS_TO_GENERATE)} to {max(DAYS_TO_GENERATE)})")
    print(f"  - Bronze Tier 1/2/3: {TIER1_ISSUES_PCT}% / {TIER2_ISSUES_PCT}% / {TIER3_ISSUES_PCT}%")
    print(f"  - Fresh Data % (Day 4+): {int(FRESH_DATA_PCT*100)}%")
    
    print(f"\n🔍 Checking for existing files...")
    existing_days = get_existing_files(OUTPUT_PATH)
    days_to_create = [d for d in DAYS_TO_GENERATE if d not in existing_days]
    
    if existing_days:
        print(f"   ✅ Found existing files for {len(existing_days)} days")
        print(f"   📝 Will skip: {existing_days[:10]}{'...' if len(existing_days) > 10 else ''}")
    
    if not days_to_create:
        print("\n✅ All files already exist. Nothing to generate.")
        print("💡 To add more days: Edit DAYS_TO_GENERATE = list(range(1, 94))")
        return
    
    print(f"   📝 Will generate {len(days_to_create)} new days")
    
    random.seed(42)
    np.random.seed(42)
    
    start_time = datetime.now()
    file_info = []
    
    for day_num in days_to_create:
        date_str = get_date_for_day(day_num)
        df = generate_day_data(day_num, date_str)
        filepath, file_size = save_day_file(df, day_num, OUTPUT_PATH)
        print_day_statistics(df, day_num, date_str)
        file_info.append((day_num, date_str, len(df), file_size))
    
    write_generation_log(days_to_create, existing_days, file_info, OUTPUT_PATH)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "="*70)
    print("🎉 GENERATION COMPLETE!")
    print("="*70)
    print(f"\n⏱️  Execution time: {duration:.2f} seconds")
    print(f"📂 Output location: {OUTPUT_PATH}")
    print(f"📊 Total files: {len(existing_days) + len(days_to_create)}")
    print(f"\n🎯 Next Steps:")
    print(f"   1. Review files in: {OUTPUT_PATH}")
    print(f"   2. Upload to GCS: gsutil -m cp {OUTPUT_PATH}/*.csv gs://your-bucket/raw/")
    print(f"   3. Delete Bronze data (optional - for clean start)")
    print(f"   4. Run Bronze full_refresh with FIXED validate_bronze.py")
    print(f"   5. Verify quarantine table has Tier 1 issues")
    print(f"   6. To add more days: Edit DAYS_TO_GENERATE = list(range(1, 94))")
    print(f"   7. Run script again - generates FRESH data for new days!")
    print("\n💡 KEY FEATURES:")
    print(f"   - Bronze Tier 1/2/3 issues included (quarantine will NOT be empty)")
    print(f"   - Day 4+ has 30% FRESH data (watermark → NOW) for incremental testing")
    print(f"   - Silver test data included (soft deletes, extra duplicates)")
    print(f"   - No data collisions (CDC/duplicates preserve original data)")
    print("\n" + "="*70)

if __name__ == "__main__":
    main()