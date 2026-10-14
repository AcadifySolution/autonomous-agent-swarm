import sqlite3

from swarm.core.queue import Task
from swarm.core.state import StateStore


def test_priority_survives_reopen_and_update(tmp_path):
    path = str(tmp_path / "state.db")
    store = StateStore(path)
    task = Task(
        task_id="urgent",
        name="Urgent",
        description="test",
        worker_type="worker",
        priority=2,
    )
    store.save_task(task, status="PENDING")
    store.close()
    store = StateStore(path)
    assert store.get_task(task.task_id)["priority"] == 2
    task.priority = 7
    store.save_task(task, status="RUNNING")
    assert store.get_task(task.task_id)["priority"] == 7
    store.close()


def test_existing_database_gets_priority_column(tmp_path):
    path = str(tmp_path / "legacy.db")
    store = StateStore(path)
    task = Task(
        task_id="legacy", name="Legacy", description="test", worker_type="worker"
    )
    store.save_task(task, status="PENDING")
    store.close()
    with sqlite3.connect(path) as conn:
        conn.execute("ALTER TABLE tasks DROP COLUMN priority")
    store = StateStore(path)
    assert store.get_task(task.task_id)["priority"] == 10
    store.close()
