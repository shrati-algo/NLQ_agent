import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def _build_mysql_uri_from_env() -> str:
    host = os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT", "3306")
    dbname = os.getenv("MYSQL_DB")
    user = os.getenv("MYSQL_USER")
    pwd = os.getenv("MYSQL_PASSWORD")

    return f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{dbname}"

# Create engine
engine = create_engine(_build_mysql_uri_from_env())

# ==============================
# Load CSV safely (handles encoding issues)
# ==============================
file_path = r"C:\Users\shradha\Documents\NLQ_agent\Current_Inventory.csv"   # change path

try:
    df = pd.read_csv(file_path, encoding="utf-8")
except UnicodeDecodeError:
    df = pd.read_csv(file_path, encoding="latin1")

print("CSV Loaded Successfully!")

# ==============================
# Clean column names (VERY IMPORTANT)
# ==============================
df.columns = (
    df.columns
    .str.strip()
    .str.replace(" ", "_")
    .str.replace(r"[^\w]", "", regex=True)
    .str.lower()
)

print("Columns:", df.columns.tolist())

# ==============================
# Push to MySQL
# ==============================
table_name = "dummy_current_inventory2"   # change this


# Clean column names
df.columns = (
    df.columns
    .str.strip()
    .str.replace(" ", "_")
    .str.replace(r"[^\w]", "", regex=True)
    .str.lower()
)

# 🔥 FIX: Make duplicate columns unique
def make_unique(cols):
    seen = {}
    new_cols = []

    for col in cols:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col)

    return new_cols

df.columns = make_unique(df.columns)

print("Final Columns:", df.columns.tolist())

df.to_sql(
    name=table_name,
    con=engine,
    if_exists="replace",   # 'replace' or 'append'
    index=False,
    chunksize=1000,        # good for large files
    method="multi"
)

print(f"Data uploaded successfully to table: {table_name}")