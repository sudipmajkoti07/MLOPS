from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transfer_data():
    csv_path = '/opt/airflow/data/train.csv'
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"File {csv_path} not found.")

    print(f"Reading data from {csv_path}...")
    df = pd.read_csv(csv_path)

    print("Cleaning data...")
    # Basic data cleaning: handle missing values
    df = df.where(pd.notnull(df), None)

    # Convert types if necessary
    # Pandas integers with NaN become floats, so they might be object or float.
    # SQLAlchemy will handle inserting them.

    # Create connection to MariaDB
    # Using pymysql since it is configured in requirements
    print("Connecting to MariaDB...")
    engine = create_engine(
        'mysql+pymysql://root:root@mariadb:3306/mlops',
        pool_pre_ping=True,
    )

    print("Replacing existing data in loan_data table...")
    with engine.begin() as conn:
        try:
            conn.execute(text("TRUNCATE TABLE loan_data"))
        except OperationalError:
            # ColumnStore metadata can be out of sync after a restart
            conn.execute(text("DROP TABLE IF EXISTS loan_data"))
            conn.execute(text("""
                CREATE TABLE loan_data (
                    id INT,
                    annual_income DOUBLE,
                    debt_to_income_ratio DOUBLE,
                    credit_score INT,
                    loan_amount DOUBLE,
                    interest_rate DOUBLE,
                    gender VARCHAR(50),
                    marital_status VARCHAR(50),
                    education_level VARCHAR(50),
                    employment_status VARCHAR(50),
                    loan_purpose VARCHAR(100),
                    grade_subgrade VARCHAR(50),
                    loan_paid_back FLOAT
                ) ENGINE=ColumnStore
            """))

    print(f"Transferring {len(df)} rows to MariaDB ColumnStore...")
    df.to_sql(name='loan_data',
              con=engine,
              if_exists='append',
              index=False,
              method='multi',
              chunksize=5000)
    print("Data transfer complete.")

with DAG(
    'transfer_train_data_to_mariadb',
    default_args=default_args,
    description='A DAG to transfer train.csv data into MariaDB ColumnStore',
    schedule=timedelta(days=7),
    catchup=False,
) as dag:

    transfer_task = PythonOperator(
        task_id='transfer_csv_to_mariadb',
        python_callable=transfer_data,
    )

    trigger_preprocess = TriggerDagRunOperator(
        task_id='trigger_preprocess',
        trigger_dag_id='preprocess_loan_data',
    )

    transfer_task >> trigger_preprocess
