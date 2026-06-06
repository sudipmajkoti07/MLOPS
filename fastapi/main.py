import os
import mlflow
import mlflow.sklearn
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from mlflow.tracking import MlflowClient
from contextlib import asynccontextmanager

# ─── Configuration ────────────────────────────────────────────────────────────
MLFLOW_TRACKING_URI   = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
REGISTERED_MODEL_NAME = os.getenv("REGISTERED_MODEL_NAME", "loan_logistic_regression")

# ─── Global model holder ───────────────────────────────────────────────────────
model_state = {"model": None, "version": None, "run_id": None}


def load_latest_model():
    """Fetch the latest version of the registered model from MLflow."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()

    versions = client.get_latest_versions(REGISTERED_MODEL_NAME)
    if not versions:
        raise RuntimeError(
            f"No versions found for registered model '{REGISTERED_MODEL_NAME}'. "
            "Please train and register the model first via the Airflow DAG."
        )

    # Pick the most recently created version (highest version number)
    latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
    model_uri = f"models:/{REGISTERED_MODEL_NAME}/{latest.version}"

    print(f"[MLflow] Loading model '{REGISTERED_MODEL_NAME}' version {latest.version} …")
    loaded = mlflow.sklearn.load_model(model_uri)
    print(f"[MLflow] Model loaded successfully (run_id={latest.run_id})")

    model_state["model"]   = loaded
    model_state["version"] = latest.version
    model_state["run_id"]  = latest.run_id


# ─── Lifespan (startup / shutdown) ────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        load_latest_model()
    except Exception as e:
        print(f"[WARNING] Could not load model at startup: {e}")
    yield


# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Loan Prediction API",
    description=(
        "Real-time loan approval predictions powered by the latest "
        "MLflow-registered Logistic Regression model."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Request / Response schemas ────────────────────────────────────────────────
class LoanFeatures(BaseModel):
    gender: str = Field(..., example="Male", description="Gender of the applicant")
    marital_status: str = Field(..., example="Married", description="Marital status")
    education_level: str = Field(..., example="Graduate", description="Highest education level")
    employment_status: str = Field(..., example="Employed", description="Employment status")
    loan_purpose: str = Field(..., example="Home", description="Purpose of the loan")
    annual_income: float = Field(..., example=75000.0, description="Annual income in USD")
    loan_amount: float = Field(..., example=20000.0, description="Requested loan amount in USD")
    credit_score: int = Field(..., example=720, description="Credit score (300-850)")
    interest_rate: float = Field(..., example=8.5, description="Interest rate (%)")


class PredictionResponse(BaseModel):
    prediction: int
    prediction_label: str
    probability_paid_back: float
    probability_not_paid_back: float
    model_version: str
    model_run_id: str


class ModelInfoResponse(BaseModel):
    model_name: str
    model_version: str
    run_id: str
    tracking_uri: str


# ─── Endpoints ────────────────────────────────────────────────────────────────
@app.get("/", tags=["Health"])
def root():
    return {
        "message": "Loan Prediction API is running 🚀",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health", tags=["Health"])
def health():
    if model_state["model"] is None:
        raise HTTPException(status_code=503, detail="Model not loaded yet.")
    return {
        "status": "healthy",
        "model_version": model_state["version"],
        "run_id": model_state["run_id"],
    }


@app.get("/model/info", response_model=ModelInfoResponse, tags=["Model"])
def model_info():
    """Return metadata about the currently loaded model."""
    if model_state["model"] is None:
        raise HTTPException(status_code=503, detail="Model not loaded. Call /model/reload first.")
    return ModelInfoResponse(
        model_name=REGISTERED_MODEL_NAME,
        model_version=model_state["version"],
        run_id=model_state["run_id"],
        tracking_uri=MLFLOW_TRACKING_URI,
    )


@app.post("/model/reload", tags=["Model"])
def reload_model():
    """Force reload the latest model version from MLflow."""
    try:
        load_latest_model()
        return {
            "message": "Model reloaded successfully.",
            "model_version": model_state["version"],
            "run_id": model_state["run_id"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict", response_model=PredictionResponse, tags=["Prediction"])
def predict(features: LoanFeatures):
    """
    Predict whether a loan will be paid back.

    - **1** → Loan will be paid back ✅
    - **0** → Loan will NOT be paid back ❌
    """
    if model_state["model"] is None:
        raise HTTPException(
            status_code=503,
            detail="Model is not loaded. Ensure the Airflow training DAG has run successfully.",
        )

    # Build a single-row DataFrame matching training feature order
    input_df = pd.DataFrame([features.model_dump()])

    try:
        prediction  = int(model_state["model"].predict(input_df)[0])
        probability = model_state["model"].predict_proba(input_df)[0]
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Prediction error: {e}")

    prob_paid     = float(probability[1])
    prob_not_paid = float(probability[0])

    return PredictionResponse(
        prediction=prediction,
        prediction_label="Loan will be paid back ✅" if prediction == 1 else "Loan will NOT be paid back ❌",
        probability_paid_back=round(prob_paid, 4),
        probability_not_paid_back=round(prob_not_paid, 4),
        model_version=model_state["version"],
        model_run_id=model_state["run_id"],
    )
