import argparse
import json
import pandas as pd
from pipes import quote
from pipelines.utils import get_db_engine
from ml.experimentation.synthetic_control import SyntheticControl
from sqlalchemy import text

def register_experiment(name, hypothesis, treatment_store_id, category_id, start_date, end_date):
    print(f"Registering Experiment: {name}")
    engine = get_db_engine()
    
    # 1. Generate Synthetic Control
    sc = SyntheticControl(engine)
    try:
        control_res = sc.find_control_stores(
            treatment_store_id=treatment_store_id,
            category_id=category_id,
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        print(f"Error generating synthetic control: {e}")
        return

    # 2. Insert into Registry
    # Prepare JSONs
    treatment_json = json.dumps([treatment_store_id])
    control_json = json.dumps(control_res['control_weights'])
    
    insert_query = text("""
        INSERT INTO experiments (
            name, hypothesis, category_id, treatment_stores, control_stores, start_date, end_date, status
        ) VALUES (
            :name, :hypothesis, :category_id, :treatment_stores, :control_stores, :start_date, :end_date, 'planned'
        ) RETURNING experiment_id;
    """)
    
    with engine.begin() as conn:
        res = conn.execute(insert_query, {
            "name": name,
            "hypothesis": hypothesis,
            "category_id": category_id,
            "treatment_stores": treatment_json,
            "control_stores": control_json,
            "start_date": start_date,
            "end_date": end_date
        })
        exp_id = res.fetchone()[0]
        
    print(f"Experiment Registered Successfully! ID: {exp_id}")
    print(f"Control Stores Found: {len(control_res['control_weights'])}")
    print(f"R2 Fit: {control_res['r2_score']:.3f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Register a new Pricing Experiment")
    parser.add_argument("--name", required=True)
    parser.add_argument("--hypothesis", required=True)
    parser.add_argument("--store", type=int, required=True)
    parser.add_argument("--category", default="sdr")
    # Using 1990 as pre-period for demo default
    parser.add_argument("--start_date", default="1990-01-01") 
    parser.add_argument("--end_date", default="1991-01-01")
    
    args = parser.parse_args()
    
    register_experiment(
        name=args.name,
        hypothesis=args.hypothesis,
        treatment_store_id=args.store,
        category_id=args.category,
        start_date=args.start_date,
        end_date=args.end_date
    )
