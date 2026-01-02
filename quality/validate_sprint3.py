import pandas as pd
from pipelines.utils import get_db_engine

def validate_sprint3():
    print("Running Sprint 3 Validation (Elasticity Check)...")
    engine = get_db_engine()

    # 1. Check Catalog Existence
    try:
        df = pd.read_sql("select * from elasticity_catalog", engine)
    except Exception as e:
        print(f"FAILED: elasticity_catalog table not found. {e}")
        return

    if df.empty:
        print("FAILED: elasticity_catalog is empty.")
        return
    
    print(f"Catalog Size: {len(df)} rows.")

    # 2. Check Coverage
    categories = df['category_id'].unique()
    print(f"Categories found: {categories}")
    
    # 3. Check Elasticity Range (Should be negative usually)
    mean_elast = df['elasticity'].mean()
    print(f"Mean Elasticity across all items: {mean_elast:.3f}")
    
    if mean_elast > 0:
        print("WARNING: Mean elasticity is positive. Check model specification (Log-Log vs Log-Linear).")
    else:
        print("PASSED: Elasticity direction is negative (expected).")

    # 4. Check CI tightness
    df['ci_width'] = df['ci_upper'] - df['ci_lower']
    avg_ci = df['ci_width'].mean()
    print(f"Average CI Width: {avg_ci:.3f}")

if __name__ == "__main__":
    validate_sprint3()
