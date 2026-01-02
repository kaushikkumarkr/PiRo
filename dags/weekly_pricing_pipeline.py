from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
from pipelines.utils import get_db_engine

default_args = {
    'owner': 'piro_data_science',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weekly_pricing_pipeline',
    default_args=default_args,
    description='End-to-End Pricing Engine Pipeline',
    schedule_interval='@weekly',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 1. Transform Data (dbt)
    t1_dbt = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /app/dbt && dbt run --select marts',
    )

    # 2. Check Drift
    # Returns 'trigger_retraining' or 'skip_retraining'
    def check_psi(**kwargs):
        # ... (Import Drift Logic or subprocess) ...
        # For demo, we simulate a drift check
        import random
        drift_score = random.random() * 0.3 # Simulate 0.0 to 0.3
        print(f"PSI Score: {drift_score}")
        if drift_score > 0.2:
            return 'trigger_retraining'
        return 'skip_retraining'

    t2_drift_check = BranchPythonOperator(
        task_id='drift_check',
        python_callable=check_psi,
    )

    # 3a. Retrain Model (Conditional)
    t3_train = BashOperator(
        task_id='trigger_retraining',
        bash_command='python /app/ml/elasticity/train_model.py',
    )

    # 3b. Skip
    t3_skip = BashOperator(
        task_id='skip_retraining',
        bash_command='echo "Stable Distribution. No Training Needed."',
    )

    # 4. Feature Store Update
    t4_fs = BashOperator(
        task_id='update_feature_store',
        bash_command='cd /app/dbt && dbt run --select mart_feature_store',
        trigger_rule='none_failed', # Run even if branch skipped
    )

    # 5. Optimize
    t5_optimize = BashOperator(
        task_id='run_optimizer',
        bash_command='python /app/ml/optimizer/optimize.py --category sdr',
    )

    # Flow
    t1_dbt >> t2_drift_check
    t2_drift_check >> [t3_train, t3_skip]
    [t3_train, t3_skip] >> t4_fs >> t5_optimize
