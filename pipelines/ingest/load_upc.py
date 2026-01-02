import pandas as pd
import os
from pipelines.utils import get_db_engine

CATEGORIES = ['sdr', 'cer', 'lnd', 'sna']

def load_upc():
    engine = get_db_engine()
    
    for cat in CATEGORIES:
        print(f"Loading UPCs for category: {cat}...")
        filename = f"upc{cat}.csv"
        data_path = f"/app/data/raw/{filename}"
        if not os.path.exists(data_path):
            data_path = f"data/raw/{filename}"
        
        if not os.path.exists(data_path):
            print(f"Skipping {cat}: {filename} not found.")
            continue
            
        try:
            df = pd.read_csv(data_path, encoding='latin1')
        except Exception as e:
            print(f"Error reading CSV {filename}: {e}")
            continue

        df.columns = [c.lower() for c in df.columns]
        df['category_id'] = cat
        
        table_name = f"raw_upc_{cat}"
        df.to_sql(table_name, engine, schema='public', if_exists='replace', index=False)
        print(f"Loaded {len(df)} rows into {table_name}.")

if __name__ == "__main__":
    load_upc()
