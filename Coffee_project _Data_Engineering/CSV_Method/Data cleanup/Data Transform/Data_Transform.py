import pandas as pd
from datetime import datetime
import json

# ======================
# CONFIG
# ======================
INPUT_FILE = r'C:\Users\kenne\Documents\Business Stuffs\Github\Coffee_project _Data_Engineering\CSV_Method\Data cleanup\Raw data\raw_coffee_data.csv'
OUTPUT_FILE = r"C:\Users\kenne\Documents\Business Stuffs\Github\Coffee_project _Data_Engineering\CSV_Method\Data cleanup\Clean data\clean_coffee_sales.csv"
SCHEMA_FILE = r"C:\Users\kenne\Documents\Business Stuffs\Github\Coffee_project _Data_Engineering\CSV_Method\Data cleanup\Clean data\bq_schema_optional.json"

# ======================
# READ RAW DATA
# ======================
df = pd.read_csv(INPUT_FILE)

# ======================
# 1️⃣ CLEAN COLUMN NAMES
# ======================
df.columns = [c.strip() for c in df.columns]

# ======================
# 2️⃣ TRIM STRING VALUES
# ======================
string_cols = ["order_id", "customer_id", "payment_method", "store_id"]

for col in string_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# ======================
# 3️⃣ FIX DATA TYPES
# ======================
df["order_id"] = pd.to_numeric(df["order_id"], errors="coerce")
df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce")
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

# ======================
# 4️⃣ NORMALIZE VALUES
# ======================
df["payment_method"] = (
    df["payment_method"]
    .str.lower()
    .replace({
        "c@sh": "cash",
        "gcash": "cash",
        "nan": None
    })
)

df["store_id"] = df["store_id"].str.lower().replace("nan", None)

# ======================
# 5️⃣ PARSE DATES
# ======================
def parse_date(val):
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(val, fmt).date()
        except Exception:
            pass
    return pd.NaT

df["order_date"] = df["order_date"].astype(str).apply(parse_date)

# ======================
# 6️⃣ REMOVE DUPLICATES
# ======================
df = df.drop_duplicates(
    subset=["order_id", "customer_id", "order_date", "amount"],
    keep="first"
)

# ======================
# 7️⃣ DROP CRITICAL NULLS
# ======================
df = df.dropna(
    subset=["order_id", "customer_id", "order_date", "amount"]
)

# ======================
# 8️⃣ BUSINESS RULES
# ======================
df = df[df["amount"] > 0]

# ======================
# 9️⃣ FINAL TYPES (BQ SAFE)
# ======================
df["order_id"] = df["order_id"].astype("Int64")
df["customer_id"] = df["customer_id"].astype("Int64")
df["amount"] = df["amount"].astype(float)
df["order_date"] = df["order_date"].astype(str)

df = df.reset_index(drop=True)

# ======================
# 🔟 WRITE CLEAN CSV
# ======================
df.to_csv(OUTPUT_FILE, index=False)

# ======================
# 1️⃣1️⃣ WRITE BIGQUERY SCHEMA
# ======================
schema = [
    {"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "order_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "amount", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "payment_method", "type": "STRING", "mode": "NULLABLE"},
    {"name": "store_id", "type": "STRING", "mode": "NULLABLE"},
]

with open(SCHEMA_FILE, "w") as f:
    json.dump(schema, f, indent=2)

print("✅ Cleaning complete")
print(f"Rows loaded: {len(df)}")
print(f"Output CSV: {OUTPUT_FILE}")
print(f"Schema file: {SCHEMA_FILE}")
