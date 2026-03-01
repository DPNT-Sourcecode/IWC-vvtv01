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
    """Example #1 from challenge: Rule of 3 moves user's tasks to front."""
    run_queue([
        # 1. Enqueue: user_id=1, provider="companies_house", timestamp='2025-10-20 12:00:00' -> 1
        call_enqueue("companies_house", 1, "2025-10-20 12:00:00").expect(1),
        # 2. Enqueue: user_id=2, provider="bank_statements", timestamp='2025-10-20 12:00:00' -> 2
        call_enqueue("bank_statements", 2, "2025-10-20 12:00:00").expect(2),
        # 3. Enqueue: user_id=1, provider="id_verification", timestamp='2025-10-20 12:00:00' -> 3
        call_enqueue("id_verification", 1, "2025-10-20 12:00:00").expect(3),
        # 4. Enqueue: user_id=1, provider="bank_statements", timestamp='2025-10-20 12:00:00' -> 4
        call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(4),
        # User 1 now has 3 tasks, all should come before user 2
        # 5. Dequeue -> {"user_id": 1, "provider": "companies_house"}
        call_dequeue().expect("companies_house", 1),
        # 6. Dequeue -> {"user_id": 1, "provider": "id_verification"}
        call_dequeue().expect("id_verification", 1),
        # 7. Dequeue -> {"user_id": 1, "provider": "bank_statements"}
        call_dequeue().expect("bank_statements", 1),
        # 8. Dequeue -> {"user_id": 2, "provider": "bank_statements"}
        call_dequeue().expect("bank_statements", 2),
    ])


def test_timestamp_ordering() -> None:
    """Example #2 from challenge: Earlier timestamp processed first."""
    run_queue([
        # 1. Enqueue: user_id=1, provider="bank_statements", timestamp='2025-10-20 12:05:00' -> 1
        call_enqueue("bank_statements", 1, "2025-10-20 12:05:00").expect(1),
        # 2. Enqueue: user_id=2, provider="bank_statements", timestamp='2025-10-20 12:00:00' -> 2
        call_enqueue("bank_statements", 2, "2025-10-20 12:00:00").expect(2),
        # 3. Dequeue -> {"user_id": 2, "provider": "bank_statements"} (earlier timestamp)
        call_dequeue().expect("bank_statements", 2),
        # 4. Dequeue -> {"user_id": 1, "provider": "bank_statements"}
        call_dequeue().expect("bank_statements", 1),
    ])


def test_dependency_resolution() -> None:
    """Example #3 from challenge: Dependencies are auto-added."""
    run_queue([
        # 1. Enqueue: user_id=1, provider="credit_check", timestamp='2025-10-20 12:00:00' -> 2
        # credit_check depends on companies_house, so both are added
        call_enqueue("credit_check", 1, "2025-10-20 12:00:00").expect(2),
        # 2. Dequeue -> {"user_id": 1, "provider": "companies_house"} (dependency first)
        call_dequeue().expect("companies_house", 1),
        # 3. Dequeue -> {"user_id": 1, "provider": "credit_check"}
        call_dequeue().expect("credit_check", 1),
    ])


def test_size_returns_current_count() -> None:
    """size() returns current number of pending tasks."""
    queue = QueueSolutionEntrypoint()
    assert queue.size() == 0
    queue.enqueue(TaskSubmission(provider="bank_statements", user_id=1, timestamp="2025-10-20 12:00:00"))
    assert queue.size() == 1
    queue.enqueue(TaskSubmission(provider="id_verification", user_id=2, timestamp="2025-10-20 12:00:00"))
    assert queue.size() == 2
    queue.dequeue()
    assert queue.size() == 1


def test_purge_clears_queue() -> None:
    """purge() clears the queue and returns True."""
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission(provider="bank_statements", user_id=1, timestamp="2025-10-20 12:00:00"))
    queue.enqueue(TaskSubmission(provider="id_verification", user_id=2, timestamp="2025-10-20 12:00:00"))
    assert queue.size() == 2
    result = queue.purge()
    assert result is True
    assert queue.size() == 0


def test_age_returns_integer() -> None:
    """age() returns the internal age of the queue in seconds."""
    queue = QueueSolutionEntrypoint()
    age = queue.age()
    assert isinstance(age, int)


def test_dequeue_empty_returns_none() -> None:
    """dequeue() returns None when queue is empty."""
    queue = QueueSolutionEntrypoint()
    result = queue.dequeue()
    assert result is None

