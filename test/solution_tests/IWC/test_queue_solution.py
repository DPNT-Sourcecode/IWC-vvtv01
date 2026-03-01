from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue
from solutions.IWC.queue_solution_entrypoint import QueueSolutionEntrypoint
from solutions.IWC.task_types import TaskSubmission


def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])


def test_rule_of_3_priority() -> None:
    run_queue([
        call_enqueue("companies_house", 1, "2025-10-20 12:00:00").expect(1),
        call_enqueue("bank_statements", 2, "2025-10-20 12:00:00").expect(2),
        call_enqueue("id_verification", 1, "2025-10-20 12:00:00").expect(3),
        call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(4),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_timestamp_ordering() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, "2025-10-20 12:05:00").expect(1),
        call_enqueue("bank_statements", 2, "2025-10-20 12:00:00").expect(2),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_dependency_resolution() -> None:
    run_queue([
        call_enqueue("credit_check", 1, "2025-10-20 12:00:00").expect(2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
    ])


def test_size_returns_current_count() -> None:
    queue = QueueSolutionEntrypoint()
    assert queue.size() == 0
    queue.enqueue(TaskSubmission(provider="bank_statements", user_id=1, timestamp="2025-10-20 12:00:00"))
    assert queue.size() == 1
    queue.enqueue(TaskSubmission(provider="id_verification", user_id=2, timestamp="2025-10-20 12:00:00"))
    assert queue.size() == 2
    queue.dequeue()
    assert queue.size() == 1


def test_purge_clears_queue() -> None:
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission(provider="bank_statements", user_id=1, timestamp="2025-10-20 12:00:00"))
    queue.enqueue(TaskSubmission(provider="id_verification", user_id=2, timestamp="2025-10-20 12:00:00"))
    assert queue.size() == 2
    result = queue.purge()
    assert result is True
    assert queue.size() == 0


def test_age_returns_integer() -> None:
    queue = QueueSolutionEntrypoint()
    age = queue.age()
    assert isinstance(age, int)


def test_dequeue_empty_returns_none() -> None:
    queue = QueueSolutionEntrypoint()
    result = queue.dequeue()
    assert result is None
