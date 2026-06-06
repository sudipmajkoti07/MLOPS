import mlflow
from mlflow.tracking import MlflowClient

mlflow.set_tracking_uri("http://mlflow:5000")
client = MlflowClient()

# Get all runs for the experiment
experiment = client.get_experiment_by_name("loan_prediction")
if not experiment:
    print("Experiment 'loan_prediction' not found.")
    exit(1)

runs = client.search_runs(
    experiment_ids=[experiment.experiment_id],
    order_by=["start_time DESC"]
)

print(f"Found {len(runs)} total runs.")

if len(runs) <= 1:
    print("Nothing to clean up.")
    exit(0)

# Keep the latest (first in DESC order), delete the rest
latest = runs[0]
to_delete = runs[1:]

print(f"\nKeeping latest run:")
print(f"  run_id={latest.info.run_id}  started={latest.info.start_time}  status={latest.info.status}")

print(f"\nDeleting {len(to_delete)} old runs...")
for run in to_delete:
    print(f"  Deleting run_id={run.info.run_id}  started={run.info.start_time}")
    client.delete_run(run.info.run_id)

print("\nDone! Only the latest run remains.")
print(f"Latest run_id: {latest.info.run_id}")
print(f"Status: {latest.info.status}")
