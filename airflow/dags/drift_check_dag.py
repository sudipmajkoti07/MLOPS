from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import pandas as pd
import redis
import json
from io import StringIO
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

REDIS_KEY_REF = 'preprocess'
REDIS_KEY_CURR = 'prediction_log'
REPORT_PATH = '/opt/airflow/data/drift_report.html'

def check_data_drift():
    print("Connecting to Redis...")
    r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

    # 1. Fetch reference data
    raw_ref = r.get(REDIS_KEY_REF)
    if not raw_ref:
        raise ValueError(f"Reference data '{REDIS_KEY_REF}' not found in Redis.")
    
    print("Loading reference data...")
    ref_df = pd.read_json(StringIO(raw_ref), orient='records')
    print(f"Reference data shape: {ref_df.shape}")

    # 2. Fetch current data
    raw_curr_list = r.lrange(REDIS_KEY_CURR, 0, -1)
    if not raw_curr_list:
        raise AirflowSkipException(f"Current data '{REDIS_KEY_CURR}' not found or empty in Redis. Skipping drift check.")
    
    num_records = len(raw_curr_list)
    if num_records <= 10:
        raise AirflowSkipException(f"Not enough new prediction data. Found {num_records}, need more than 10. Skipping drift check.")
    
    print("Loading current data...")
    curr_data = [json.loads(item) for item in raw_curr_list]
    curr_df = pd.DataFrame(curr_data)
    print(f"Current data shape: {curr_df.shape}")

    # 3. Clean up columns to only compare matching features
    cols_to_drop = ['prediction', 'probability_paid_back', 'probability_not_paid_back']
    curr_df_clean = curr_df.drop(columns=[c for c in cols_to_drop if c in curr_df.columns])
    
    common_cols = list(set(ref_df.columns).intersection(set(curr_df_clean.columns)))
    
    if not common_cols:
        raise ValueError("No common columns found between reference and current data.")
        
    ref_df_eval = ref_df[common_cols]
    curr_df_eval = curr_df_clean[common_cols]

    print(f"Evaluating drift on {len(common_cols)} features: {common_cols}")

    # 4. Generate Drift Report
    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=ref_df_eval, current_data=curr_df_eval)
    
    # 5. Save Report
    report.save_html(REPORT_PATH)
    print(f"Drift report saved successfully to {REPORT_PATH}")

    # 6. Trim the processed items from the Redis list to only check new data next time
    r.ltrim(REDIS_KEY_CURR, num_records, -1)
    print(f"Trimmed {num_records} processed items from '{REDIS_KEY_CURR}'.")

with DAG(
    'drift_check_dag',
    default_args=default_args,
    description='Check data drift using Evidently AI and save HTML report',
    schedule=timedelta(days=7),
    catchup=False,
) as dag:

    drift_task = PythonOperator(
        task_id='run_evidently_drift_check',
        python_callable=check_data_drift,
    )

    drift_task
