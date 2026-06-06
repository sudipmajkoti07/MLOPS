from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

import mlflow
import mlflow.sklearn
import pandas as pd
import redis
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

REDIS_KEY = 'preprocess'
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
MLFLOW_EXPERIMENT = os.getenv('MLFLOW_EXPERIMENT', 'loan_prediction')
REGISTERED_MODEL_NAME = os.getenv('REGISTERED_MODEL_NAME', 'loan_logistic_regression')

NEW_DF_COLUMNS = [
    'gender',
    'marital_status',
    'education_level',
    'employment_status',
    'loan_purpose',
    'annual_income',
    'loan_amount',
    'credit_score',
    'interest_rate',
    'debt_to_income_ratio',
    'loan_paid_back',
]

CATEGORICAL_FEATURES = [
    'gender',
    'marital_status',
    'education_level',
    'employment_status',
    'loan_purpose',
]

NUMERIC_FEATURES = [
    'annual_income',
    'loan_amount',
    'credit_score',
    'interest_rate',
    'debt_to_income_ratio',
]

TARGET = 'loan_paid_back'


def train_logistic_regression():
    print("Connecting to Redis...")
    r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

    raw_data = r.get(REDIS_KEY)
    if raw_data is None:
        raise ValueError(f"Redis key '{REDIS_KEY}' not found. Run the preprocess DAG first.")

    print(f"Loading preprocessed data from Redis key '{REDIS_KEY}'...")
    from io import StringIO
    df = pd.read_json(StringIO(raw_data), orient='records')
    df = df[NEW_DF_COLUMNS]
    print(f"Loaded {len(df)} rows with columns: {list(df.columns)}")

    X = df.drop(columns=[TARGET])
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', OneHotEncoder(handle_unknown='ignore'), CATEGORICAL_FEATURES),
            ('num', StandardScaler(), NUMERIC_FEATURES),
        ]
    )

    model = Pipeline(
        steps=[
            ('preprocessor', preprocessor),
            ('classifier', LogisticRegression(max_iter=5000, random_state=42)),
        ]
    )

    print(f"Connecting to MLflow at {MLFLOW_TRACKING_URI}...")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name='logistic_regression_training') as run:
        print("Training logistic regression model...")
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]

        accuracy = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)
        roc_auc = roc_auc_score(y_test, y_proba)

        mlflow.log_param('model_type', 'LogisticRegression')
        mlflow.log_param('test_size', 0.2)
        mlflow.log_param('random_state', 42)
        mlflow.log_param('features', ', '.join(CATEGORICAL_FEATURES + NUMERIC_FEATURES))
        mlflow.log_metric('accuracy', accuracy)
        mlflow.log_metric('f1_score', f1)
        mlflow.log_metric('roc_auc', roc_auc)

        print(f"Accuracy: {accuracy:.4f}, F1: {f1:.4f}, ROC-AUC: {roc_auc:.4f}")

        print(f"Logging model to MLflow...")
        model_info = mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path='model',
            input_example=X_train.head(5),
        )

        print(f"Registering model as '{REGISTERED_MODEL_NAME}'...")
        mlflow.register_model(
            model_uri=model_info.model_uri,
            name=REGISTERED_MODEL_NAME,
        )

        print(f"Model logged to MLflow run_id: {run.info.run_id}")
        print(f"Model registered as: {REGISTERED_MODEL_NAME}")


with DAG(
    'train_loan_model',
    default_args=default_args,
    description='Train logistic regression on Redis preprocessed data and register in MLflow',
    schedule=None,
    catchup=False,
) as dag:

    train_task = PythonOperator(
        task_id='train_and_register_model',
        python_callable=train_logistic_regression,
    )

    train_task
