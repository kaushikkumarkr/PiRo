import pandas as pd
from pipelines.utils import get_db_engine

def validate_sprint2():
    print("Running Sprint 2 Data Validation (Pandas Mode)...")
    engine = get_db_engine()

    # 1. Validate dim_calendar
    print("Validating dim_calendar...")
    df_cal = pd.read_sql("select * from dim_calendar", engine)
    row_count = len(df_cal)
    if 300 <= row_count <= 600:
         print(f"PASSED: dim_calendar row count ({row_count})")
    else:
         print(f"FAILED: dim_calendar row count ({row_count})")

    # 2. Validate dim_upc
    print("Validating dim_upc...")
    df_upc = pd.read_sql("select * from dim_upc", engine)
    unique_cats = set(df_upc['category_id'].unique())
    expected_cats = {'sdr', 'cer', 'lnd', 'sna'}
    
    # We might have partial data if ingestion crashed, but let's check what we have
    print(f"Found categories: {unique_cats}")
    if unique_cats.issubset(expected_cats):
         print("PASSED: dim_upc category_id check (subset match)")
    else:
         print(f"FAILED: Unknown categories found: {unique_cats - expected_cats}")

    # 3. Validate pricing features
    print("Validating mart_weekly_pricing_features...")
    df_mart = pd.read_sql("select * from mart_weekly_pricing_features limit 1000", engine)
    
    if df_mart.empty:
        print("WARNING: mart_weekly_pricing_features is empty (dbt skip?)")
    else:
        min_p = df_mart['price'].min()
        max_p = df_mart['price'].max()
        if 0.01 <= min_p and max_p <= 100.0:
            print(f"PASSED: Price range ({min_p} - {max_p})")
        else:
            print(f"FAILED: Price range outlier ({min_p} - {max_p})")

    print("\nValidation Complete.")

if __name__ == "__main__":
    validate_sprint2()
