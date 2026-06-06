from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import redis
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

IQR_MULTIPLIER = float(os.getenv('IQR_MULTIPLIER', '1.5'))
REDIS_KEY = 'preprocess'


def remove_outliers_iqr(df, column, iqr_multiplier=IQR_MULTIPLIER):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - iqr_multiplier * IQR
    upper_bound = Q3 + iqr_multiplier * IQR
    df_cleaned = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    return df_cleaned


def preprocess_loan_data():
    print("Connecting to MariaDB...")
    engine = create_engine(
        'mysql+pymysql://root:root@mariadb:3306/mlops',
        pool_pre_ping=True,
    )

    print("Reading loan data from MariaDB...")
    df = pd.read_sql('SELECT * FROM loan_data', engine)
    print(f"Loaded {len(df)} rows.")

    print("Dropping id and grade_subgrade columns...")
    df = df.drop(columns=['id', 'grade_subgrade'], errors='raise')

    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    print(f"Removing outliers from numeric columns: {numeric_columns}")

    rows_before = len(df)
    for column in numeric_columns:
        df = remove_outliers_iqr(df, column)
    rows_after = len(df)
    print(f"Outlier removal complete. Rows: {rows_before} -> {rows_after}")

    print("Connecting to Redis...")
    r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

    print(f"Saving preprocessed data to Redis key '{REDIS_KEY}'...")
    r.set(REDIS_KEY, df.to_json(orient='records'))
    print(f"Saved {len(df)} rows to Redis key '{REDIS_KEY}'.")


with DAG(
    'preprocess_loan_data',
    default_args=default_args,
    description='Preprocess loan data from MariaDB and store cleaned data in Redis',
    schedule=None,
    catchup=False,
) as dag:

    preprocess_task = PythonOperator(
        task_id='preprocess_and_save_to_redis',
        python_callable=preprocess_loan_data,
    )

    preprocess_task
