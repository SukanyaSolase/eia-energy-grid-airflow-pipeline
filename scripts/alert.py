import json
from datetime import datetime


def send_failure_alert(context):
    """
    on_failure_callback — logs structured failure details when any task fails.
    In production this would send to Slack, PagerDuty, or AWS SNS.
    """
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    execution_date = context["data_interval_start"]
    exception = str(context.get("exception", "No exception details"))

    alert = {
        "timestamp": datetime.utcnow().isoformat(),
        "status": "FAILED",
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "execution_date": str(execution_date),
        "error": exception,
    }

    log_path = f"/opt/airflow/logs/alert_{dag_id}_{task_id}.json"
    with open(log_path, "w") as f:
        json.dump(alert, f, indent=2)

    print(f"[ALERT] Pipeline failure — {dag_id}.{task_id} failed")
    print(f"[ALERT] Error: {exception}")
    print(f"[ALERT] Alert written to {log_path}")