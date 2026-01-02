import pandas as pd
import os
from pipelines.utils import get_db_engine

def load_demo():
    print("Loading demo (Store Demographics)...")
    data_path = "/app/data/raw/demo(stata).zip"
    if not os.path.exists(data_path):
         data_path = "data/raw/demo(stata).zip"

    if not os.path.exists(data_path):
        # Try unzipped .dta
        if os.path.exists("/app/data/raw/demo.dta"):
             data_path = "/app/data/raw/demo.dta"
        elif os.path.exists("data/raw/demo.dta"):
             data_path = "data/raw/demo.dta"

    if not os.path.exists(data_path):
        print(f"Skipping demo: File not found at {data_path}")
        return

    try:
        df = pd.read_stata(data_path)
    except Exception as e:
        print(f"Error reading Stata file: {e}")
        return

    df.columns = [c.lower() for c in df.columns]
    
    engine = get_db_engine()
    df.to_sql('raw_demo', engine, schema='public', if_exists='replace', index=False)
    print(f"Loaded {len(df)} rows into raw_demo.")

if __name__ == "__main__":
    load_demo()
