import pandas as pd
import os
from pipelines.utils import get_db_engine

CATEGORIES = ['sdr', 'cer', 'lnd', 'sna']

def load_movement():
    engine = get_db_engine()
    
    for cat in CATEGORIES:
        print(f"Loading Movement for category: {cat}...")
        filename = f"w{cat}.csv"
        data_path = f"/app/data/raw/{filename}"
        if not os.path.exists(data_path):
            data_path = f"data/raw/{filename}"
        
        if not os.path.exists(data_path):
            print(f"Skipping {cat}: {filename} not found.")
            continue
            
        # These files can be large, chunking or careful typing recommended for prod
        # For sprint 1, we assume memory is sufficient for one category
        try:
            chunksize = 50000
            for i, chunk in enumerate(pd.read_csv(data_path, encoding='latin1', chunksize=chunksize)):
                chunk.columns = [c.lower() for c in chunk.columns]
                chunk['category_id'] = cat
                
                table_name = f"raw_w{cat}"
                if_exists = 'replace' if i == 0 else 'append'
                chunk.to_sql(table_name, engine, schema='public', if_exists=if_exists, index=False)
                print(f"Loaded chunk {i+1} ({len(chunk)} rows) into {table_name}.")
                
        except Exception as e:
            print(f"Error reading CSV {filename}: {e}")
            continue

if __name__ == "__main__":
    load_movement()
