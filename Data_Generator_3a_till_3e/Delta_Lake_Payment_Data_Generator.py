import pandas as pd
import random
import os
from datetime import datetime, timedelta
import numpy as np

# ==================== CONFIGURATION SECTION ====================

# Row counts for each day
DAY1_ROWS = 15000
DAY2_ROWS = 15000
DAY3_ROWS = 15000
DAY4_ROWS = 15000

# Data Quality Issues - Tier 1 (Quarantine - Critical)
DAY1_NULL_TXID = 100
DAY2_DUPLICATE_TXID = 50

# Data Quality Issues - Tier 2 (Flag but Load - Warning)
DAY1_NEGATIVE_AMOUNT = 200
DAY2_FUTURE_TIMESTAMP = 150
DAY3_UNKNOWN_MERCHANT = 100

# Data Quality Issues - Tier 3 (Fix with Defaults - Info)
DAY1_NULL_DEVICE = 200
DAY2_NULL_LOCATION = 100
DAY3_NULL_PRODUCT = 50

# Late Arrivals & CDC
DAY3_LATE_ARRIVALS = 50
DAY4_LATE_ARRIVALS = 50
DAY4_CDC_UPDATES = 100

# Master data counts
NUM_CUSTOMERS = 1000
NUM_MERCHANTS = 500

# Date configuration (dynamic - uses today's date)
BASE_DATE = datetime.now()
DAY1_DATE = (BASE_DATE - timedelta(days=3)).strftime("%Y-%m-%d")
DAY2_DATE = (BASE_DATE - timedelta(days=2)).strftime("%Y-%m-%d")
DAY3_DATE = (BASE_DATE - timedelta(days=1)).strftime("%Y-%m-%d")
DAY4_DATE = BASE_DATE.strftime("%Y-%m-%d")
FOLDER_DATE = BASE_DATE.strftime("%Y%m%d")

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

def create_base_record(transaction_id, date_str, is_clean=True):
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

# ==================== DAY 1 DATA GENERATION ====================

def generate_day1_data():
    print(f"\n🔄 Generating Day 1 data ({DAY1_ROWS:,} rows)...")
    
    clean_count = DAY1_ROWS - DAY1_NULL_TXID - DAY1_NEGATIVE_AMOUNT - DAY1_NULL_DEVICE
    data = []
    transaction_counter = 1
    all_txids = []
    
    # Clean records
    for i in range(clean_count):
        txid = generate_transaction_id(DAY1_DATE, transaction_counter)
        record = create_base_record(txid, DAY1_DATE)
        data.append(record)
        all_txids.append(txid)
        transaction_counter += 1
    
    # Tier 1: NULL transaction_id
    print(f"   ⚠️  Tier 1: {DAY1_NULL_TXID} NULL transaction_id...")
    for i in range(DAY1_NULL_TXID):
        record = create_base_record(None, DAY1_DATE)
        data.append(record)
    
    # Tier 2: Negative amounts
    print(f"   ⚠️  Tier 2: {DAY1_NEGATIVE_AMOUNT} negative amounts...")
    for i in range(DAY1_NEGATIVE_AMOUNT):
        txid = generate_transaction_id(DAY1_DATE, transaction_counter)
        record = create_base_record(txid, DAY1_DATE)
        record['amount'] = -1 * abs(record['amount'])
        record['cashback_amount'] = 0.0
        record['loyalty_points'] = 0
        data.append(record)
        all_txids.append(txid)
        transaction_counter += 1
    
    # Tier 3: NULL device_type
    print(f"   ⚠️  Tier 3: {DAY1_NULL_DEVICE} NULL device_type...")
    for i in range(DAY1_NULL_DEVICE):
        txid = generate_transaction_id(DAY1_DATE, transaction_counter)
        record = create_base_record(txid, DAY1_DATE)
        record['device_type'] = None
        data.append(record)
        all_txids.append(txid)
        transaction_counter += 1
    
    df = pd.DataFrame(data)
    print(f"✅ Day 1 complete: {len(df):,} rows")
    return df, transaction_counter, all_txids

# ==================== DAY 2 DATA GENERATION ====================

def generate_day2_data(start_counter, day1_txids):
    print(f"\n🔄 Generating Day 2 data ({DAY2_ROWS:,} rows)...")
    
    clean_count = DAY2_ROWS - DAY2_DUPLICATE_TXID - DAY2_FUTURE_TIMESTAMP - DAY2_NULL_LOCATION
    data = []
    transaction_counter = start_counter
    all_txids = []
    
    # Clean records
    for i in range(clean_count):
        txid = generate_transaction_id(DAY2_DATE, transaction_counter)
        record = create_base_record(txid, DAY2_DATE)
        data.append(record)
        all_txids.append(txid)
        transaction_counter += 1
    
    # Tier 1: Duplicate transaction_ids
    print(f"   ⚠️  Tier 1: {DAY2_DUPLICATE_TXID} duplicate transaction_ids...")
    for i in range(DAY2_DUPLICATE_TXID):
        dup_txid = random.choice(day1_txids[:1000])  # Duplicate from Day 1
        record = create_base_record(dup_txid, DAY2_DATE)
        data.append(record)
    
    # Tier 2: Future timestamps
    print(f"   ⚠️  Tier 2: {DAY2_FUTURE_TIMESTAMP} future timestamps...")
    for i in range(DAY2_FUTURE_TIMESTAMP):
        txid = generate_transaction_id(DAY2_DATE, transaction_counter)
        future_date = (datetime.strptime(DAY2_DATE, "%Y-%m-%d") + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
        record = create_base_record(txid, future_date)
        data.append(record)
        all_txids.append(txid)
        transaction_counter += 1
    
    # Tier 3: NULL location_type
    print(f"   ⚠️  Tier 3: {DAY2_NULL_LOCATION} NULL location_type...")
    for i in range(DAY2_NULL_LOCATION):
        txid = generate_transaction_id(DAY2_DATE, transaction_counter)
        record = create_base_record(txid, DAY2_DATE)
        record['location_type'] = None
        data.append(record)
        all_txids.append(txid)
        transaction_counter += 1
    
    df = pd.DataFrame(data)
    print(f"✅ Day 2 complete: {len(df):,} rows")
    return df, transaction_counter, all_txids

# ==================== DAY 3 DATA GENERATION ====================

def generate_day3_data(start_counter):
    print(f"\n🔄 Generating Day 3 data ({DAY3_ROWS:,} rows)...")
    
    clean_count = DAY3_ROWS - DAY3_LATE_ARRIVALS - DAY3_UNKNOWN_MERCHANT - DAY3_NULL_PRODUCT
    data = []
    transaction_counter = start_counter
    all_txids = []
    
    # Clean records
    for i in range(clean_count):
        txid = generate_transaction_id(DAY3_DATE, transaction_counter)
        record = create_base_record(txid, DAY3_DATE)
        data.append(record)
        all_txids.append(txid)
        transaction_counter += 1
    
    # Late arrivals (transaction_timestamp = Day 2, updated_at = Day 3)
    print(f"   ⚠️  {DAY3_LATE_ARRIVALS} late arrivals (txn_date=Day2, updated=Day3)...")
    for i in range(DAY3_LATE_ARRIVALS):
        txid = generate_transaction_id(DAY3_DATE, transaction_counter)
        record = create_base_record(txid, DAY2_DATE)  # Transaction on Day 2
        record['updated_at'] = get_random_timestamp(DAY3_DATE).strftime("%Y-%m-%d %H:%M:%S")  # Updated on Day 3
        data.append(record)
        all_txids.append(txid)
        transaction_counter += 1
    
    # Tier 2: Unknown merchant_id
    print(f"   ⚠️  Tier 2: {DAY3_UNKNOWN_MERCHANT} unknown merchant_ids...")
    for i in range(DAY3_UNKNOWN_MERCHANT):
        txid = generate_transaction_id(DAY3_DATE, transaction_counter)
        record = create_base_record(txid, DAY3_DATE)
        record['merchant_id'] = f"MERCH_{random.randint(9000, 9999)}"  # Out of range
        record['merchant_name'] = "Unknown Merchant"
        data.append(record)
        all_txids.append(txid)
        transaction_counter += 1
    
    # Tier 3: NULL product_name
    print(f"   ⚠️  Tier 3: {DAY3_NULL_PRODUCT} NULL product_name...")
    for i in range(DAY3_NULL_PRODUCT):
        txid = generate_transaction_id(DAY3_DATE, transaction_counter)
        record = create_base_record(txid, DAY3_DATE)
        record['product_name'] = None
        data.append(record)
        all_txids.append(txid)
        transaction_counter += 1
    
    df = pd.DataFrame(data)
    print(f"✅ Day 3 complete: {len(df):,} rows")
    return df, transaction_counter, all_txids

# ==================== DAY 4 DATA GENERATION ====================

def generate_day4_data(start_counter, day1_txids, day2_txids, day3_txids):
    print(f"\n🔄 Generating Day 4 data ({DAY4_ROWS:,} rows)...")
    
    clean_count = DAY4_ROWS - DAY4_LATE_ARRIVALS - DAY4_CDC_UPDATES
    data = []
    transaction_counter = start_counter
    
    # Clean records
    for i in range(clean_count):
        txid = generate_transaction_id(DAY4_DATE, transaction_counter)
        record = create_base_record(txid, DAY4_DATE)
        data.append(record)
        transaction_counter += 1
    
    # Late arrivals (transaction_timestamp = Day 3, updated_at = Day 4)
    print(f"   ⚠️  {DAY4_LATE_ARRIVALS} late arrivals (txn_date=Day3, updated=Day4)...")
    for i in range(DAY4_LATE_ARRIVALS):
        txid = generate_transaction_id(DAY4_DATE, transaction_counter)
        record = create_base_record(txid, DAY3_DATE)  # Transaction on Day 3
        record['updated_at'] = get_random_timestamp(DAY4_DATE).strftime("%Y-%m-%d %H:%M:%S")  # Updated on Day 4
        data.append(record)
        transaction_counter += 1
    
    # CDC Updates (status changes from Day 1-3)
    print(f"   ⚠️  {DAY4_CDC_UPDATES} CDC updates (status changes from Day 1-3)...")
    all_previous_txids = day1_txids + day2_txids + day3_txids
    cdc_txids = random.sample([t for t in all_previous_txids if t], min(DAY4_CDC_UPDATES, len(all_previous_txids)))
    
    for txid in cdc_txids:
        record = create_base_record(txid, DAY4_DATE)
        # Update status: Pending -> Successful or keep Successful
        record['transaction_status'] = 'Successful'
        record['updated_at'] = get_random_timestamp(DAY4_DATE).strftime("%Y-%m-%d %H:%M:%S")
        # Merchant name update
        if record['merchant_name'] in ["Amazon India", "Flipkart", "Swiggy"]:
            record['merchant_name'] += " Pvt Ltd"
        data.append(record)
    
    df = pd.DataFrame(data)
    print(f"✅ Day 4 complete: {len(df):,} rows")
    return df

# ==================== VALIDATION & FILE SAVING ====================

def validate_and_save_data(df_day1, df_day2, df_day3, df_day4):
    print("\n" + "="*70)
    print("📊 DATA VALIDATION & SAVING")
    print("="*70)
    
    # Create output folder
    folder_name = f"raw_payment_data_{FOLDER_DATE}"
    current_dir = os.getcwd()
    output_dir = os.path.join(current_dir, folder_name)
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\n📁 Output folder: {output_dir}")
    
    # Save all days
    files = [
        (df_day1, "day1_transactions.csv"),
        (df_day2, "day2_transactions.csv"),
        (df_day3, "day3_transactions.csv"),
        (df_day4, "day4_transactions.csv")
    ]
    
    total_size = 0
    for df, filename in files:
        filepath = os.path.join(output_dir, filename)
        df.to_csv(filepath, index=False, encoding='utf-8')
        file_size = os.path.getsize(filepath)
        total_size += file_size
        print(f"   ✅ {filename} saved ({format_file_size(file_size)})")
    
    # Validation statistics
    print("\n" + "="*70)
    print("🔍 VALIDATION STATISTICS")
    print("="*70)
    
    def analyze_day(df, day_name):
        print(f"\n=== {day_name} ===")
        print(f"Total Rows: {len(df):,}")
        print(f"NULL transaction_id: {df['transaction_id'].isna().sum()}")
        print(f"Duplicate transaction_id: {df['transaction_id'].duplicated().sum()}")
        print(f"Negative amounts: {(df['amount'] < 0).sum()}")
        print(f"NULL device_type: {df['device_type'].isna().sum()}")
        print(f"NULL location_type: {df['location_type'].isna().sum()}")
        print(f"NULL product_name: {df['product_name'].isna().sum()}")
    
    analyze_day(df_day1, "DAY 1")
    analyze_day(df_day2, "DAY 2")
    analyze_day(df_day3, "DAY 3")
    analyze_day(df_day4, "DAY 4")
    
    # Save validation report
    report_path = os.path.join(output_dir, "validation_report.txt")
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("="*70 + "\n")
        f.write("DELTA LAKE PAYMENT GATEWAY - VALIDATION REPORT\n")
        f.write("="*70 + "\n\n")
        f.write(f"Generation Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Rows: {DAY1_ROWS + DAY2_ROWS + DAY3_ROWS + DAY4_ROWS:,}\n")
        f.write(f"Total Size: {format_file_size(total_size)}\n\n")
        
        for i, (df, day) in enumerate([(df_day1, "Day 1"), (df_day2, "Day 2"), 
                                        (df_day3, "Day 3"), (df_day4, "Day 4")], 1):
            f.write(f"=== {day} Statistics ===\n")
            f.write(f"Total rows: {len(df):,}\n")
            f.write(f"Tier 1 issues (quarantine): {df['transaction_id'].isna().sum() + df['transaction_id'].duplicated().sum()}\n")
            f.write(f"Tier 2 issues (flag): {(df['amount'] < 0).sum()}\n")
            f.write(f"Tier 3 issues (fix): {df['device_type'].isna().sum() + df['location_type'].isna().sum() + df['product_name'].isna().sum()}\n\n")
        
        f.write(f"=== Late Arrivals ===\n")
        f.write(f"Day 3 late arrivals: {DAY3_LATE_ARRIVALS}\n")
        f.write(f"Day 4 late arrivals: {DAY4_LATE_ARRIVALS}\n\n")
        
        f.write(f"=== CDC Updates ===\n")
        f.write(f"Day 4 status updates: {DAY4_CDC_UPDATES}\n")
    
    print(f"\n   ✅ validation_report.txt saved")
    print("\n" + "="*70)
    print("🎉 DATA GENERATION COMPLETE!")
    print("="*70)
    print(f"\n📂 All files saved in: {output_dir}")
    print(f"✅ Ready for GCS upload: gs://delta-lake-payment-gateway-476820/raw/{FOLDER_DATE}/")
    
    return output_dir

# ==================== MAIN EXECUTION ====================

def main():
    print("="*70)
    print("🚀 DELTA LAKE PAYMENT GATEWAY DATA GENERATOR")
    print("="*70)
    print(f"\nConfiguration:")
    print(f"  - Day 1-4 Rows: {DAY1_ROWS:,} each")
    print(f"  - Total Rows: {DAY1_ROWS + DAY2_ROWS + DAY3_ROWS + DAY4_ROWS:,}")
    print(f"  - Customers: {NUM_CUSTOMERS}")
    print(f"  - Merchants: {NUM_MERCHANTS}")
    print(f"  - Output Date: {FOLDER_DATE}")
    
    random.seed(42)
    np.random.seed(42)
    
    start_time = datetime.now()
    
    # Generate data
    df_day1, counter1, txids1 = generate_day1_data()
    df_day2, counter2, txids2 = generate_day2_data(counter1, txids1)
    df_day3, counter3, txids3 = generate_day3_data(counter2)
    df_day4 = generate_day4_data(counter3, txids1, txids2, txids3)
    
    # Validate and save
    output_dir = validate_and_save_data(df_day1, df_day2, df_day3, df_day4)
    
    duration = (datetime.now() - start_time).total_seconds()
    print(f"\n⏱️  Execution time: {duration:.2f} seconds")
    print(f"\n🎯 Next Steps:")
    print(f"   1. Upload to GCS: gs://delta-lake-payment-gateway-476820/raw/{FOLDER_DATE}/")
    print(f"   2. Run Bronze layer PySpark job")
    print(f"   3. Validate data quality tiers")
    print("\n" + "="*70)

if __name__ == "__main__":
    main()