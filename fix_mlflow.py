import mlflow
from mlflow.tracking import MlflowClient

mlflow.set_tracking_uri("http://mlflow:5000")
client = MlflowClient()

# List deleted experiments
from mlflow.entities import ViewType
deleted = client.search_experiments(view_type=ViewType.DELETED_ONLY)
print("Deleted experiments:")
for e in deleted:
    print(f"  name={e.name}  id={e.experiment_id}")

# Restore 'loan_prediction' if found, else permanently delete so it can be recreated
target = None
for e in deleted:
    if e.name == "loan_prediction":
        target = e
        break

if target:
    print(f"\nRestoring experiment '{target.name}' (id={target.experiment_id})...")
    client.restore_experiment(target.experiment_id)
    print("Restored successfully!")
else:
    print("\nExperiment 'loan_prediction' not found in deleted list.")
    # Try creating fresh
    exp_id = client.create_experiment("loan_prediction")
    print(f"Created new experiment 'loan_prediction' with id={exp_id}")
