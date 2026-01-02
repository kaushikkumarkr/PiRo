import pandas as pd
import numpy as np
from sklearn.linear_model import LassoCV, ElasticNetCV
from sqlalchemy import create_engine
from pipelines.utils import get_db_engine
import json

class SyntheticControl:
    def __init__(self, engine=None):
        self.engine = engine or get_db_engine()

    def find_control_stores(self, treatment_store_id, category_id, start_date, end_date):
        """
        Finds synthetic control weights for a treatment store based on pre-period behavior.
        """
        print(f"Finding Synthetic Control for Store {treatment_store_id} ({category_id})")
        
        # 1. Fetch Weekly Sales for All Stores in Category
        query = f"""
            SELECT 
                m.week_id,
                m.store_id,
                SUM(m.sales_units) as total_vol
            FROM fact_movement_weekly m
            JOIN dim_upc u USING(upc_id)
            JOIN dim_calendar c USING(week_id)
            WHERE u.category_id = '{category_id}'
              AND c.start_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1, 2
        """
        df = pd.read_sql(query, self.engine)
        
        # 2. Pivot: Index=Week, Columns=Store, Values=Vol
        pivot_df = df.pivot(index='week_id', columns='store_id', values='total_vol').fillna(0)
        
        if treatment_store_id not in pivot_df.columns:
            raise ValueError(f"Treatment store {treatment_store_id} not found in data.")
            
        # Target (Treatment Store)
        y = pivot_df[treatment_store_id].values
        
        # Features (Potential Control Stores)
        X_df = pivot_df.drop(columns=[treatment_store_id])
        X = X_df.values
        store_ids = X_df.columns.tolist()
        
        # 3. Train Sparse Regression (Lasso) to find best matching stores
        # We constrain weights to be positive (approx with ElasticNet or just accept Lasso)
        # Using LassoCV for auto-alpha
        print("Fitting LassoCV to find matching control stores...")
        model = LassoCV(cv=5, positive=True, random_state=42)
        model.fit(X, y)
        
        # 4. Extract Weights
        weights = model.coef_
        intercept = model.intercept_
        r2 = model.score(X, y)
        
        control_map = {}
        for idx, weight in enumerate(weights):
            if weight > 0.001: # Filter tiny weights
                control_map[int(store_ids[idx])] = round(float(weight), 4)
                
        print(f"Found {len(control_map)} control stores. R2 fit: {r2:.4f}")
        print(f"Control Weights: {control_map}")
        
        return {
            "treatment_store_id": treatment_store_id,
            "control_weights": control_map,
            "intercept": float(intercept),
            "r2_score": float(r2),
            "pre_period_start": start_date,
            "pre_period_end": end_date
        }

if __name__ == "__main__":
    # Test Run
    sc = SyntheticControl()
    # Find a random store to test
    res = sc.find_control_stores(
        treatment_store_id=9, # Example Store
        category_id='sdr', 
        start_date='1990-01-01', 
        end_date='1991-01-01'
    )
    print(json.dumps(res, indent=2))
