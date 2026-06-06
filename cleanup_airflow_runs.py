"""Delete failed DAG runs via raw SQL to avoid ORM FK issues."""
from airflow.utils.session import create_session

dag_id = "train_loan_model"

with create_session() as session:
    # Get failed run IDs first
    rows = session.execute(
        "SELECT run_id FROM dag_run WHERE dag_id = :dag_id AND state = 'failed'",
        {"dag_id": dag_id}
    ).fetchall()

    failed_ids = [r[0] for r in rows]
    print(f"Found {len(failed_ids)} failed run(s): {failed_ids}")

    if not failed_ids:
        print("Nothing to delete.")
    else:
        for run_id in failed_ids:
            # Delete task instances first (FK dependency)
            session.execute(
                "DELETE FROM task_instance WHERE dag_id = :dag_id AND run_id = :run_id",
                {"dag_id": dag_id, "run_id": run_id}
            )
            # Delete any log records
            session.execute(
                "DELETE FROM log WHERE dag_id = :dag_id AND run_id = :run_id",
                {"dag_id": dag_id, "run_id": run_id}
            )
            # Delete dag run notes if table exists
            try:
                session.execute(
                    "DELETE FROM dag_run_note WHERE dag_run_id IN (SELECT id FROM dag_run WHERE run_id = :run_id AND dag_id = :dag_id)",
                    {"dag_id": dag_id, "run_id": run_id}
                )
            except Exception:
                pass
            # Delete the dag run itself
            session.execute(
                "DELETE FROM dag_run WHERE dag_id = :dag_id AND run_id = :run_id",
                {"dag_id": dag_id, "run_id": run_id}
            )
            print(f"  Deleted run_id={run_id}")

        session.commit()
        print("\nDone!")

    # Show remaining
    remaining = session.execute(
        "SELECT run_id, state FROM dag_run WHERE dag_id = :dag_id",
        {"dag_id": dag_id}
    ).fetchall()
    print(f"\nRemaining runs ({len(remaining)}):")
    for r in remaining:
        print(f"  run_id={r[0]}  state={r[1]}")
