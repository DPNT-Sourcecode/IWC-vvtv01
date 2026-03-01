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


def test_deduplication() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
        call_enqueue("bank_statements", 1, "2025-10-20 12:05:00").expect(1),
        call_enqueue("id_verification", 1, "2025-10-20 12:05:00").expect(2),
        # id_verification comes first because bank_statements is deprioritized
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_deduplication_keeps_earlier_timestamp() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, "2025-10-20 12:10:00").expect(1),
        call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
        call_enqueue("bank_statements", 2, "2025-10-20 12:05:00").expect(2),
        # Both are LOW priority bank_statements, sorted by timestamp
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_bank_statements_deprioritized() -> None:
    """Round 3 Example: bank_statements goes to end of queue."""
    run_queue([
        # 1. Enqueue: user_id=1, provider="bank_statements", timestamp='2025-10-20 12:00:00' -> 1
        call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
        # 2. Enqueue: user_id=1, provider="id_verification", timestamp='2025-10-20 12:01:00' -> 2
        call_enqueue("id_verification", 1, "2025-10-20 12:01:00").expect(2),
        # 3. Enqueue: user_id=2, provider="companies_house", timestamp='2025-10-20 12:02:00' -> 3
        call_enqueue("companies_house", 2, "2025-10-20 12:02:00").expect(3),
        # 4. Dequeue -> {"user_id": 1, "provider": "id_verification"}
        call_dequeue().expect("id_verification", 1),
        # 5. Dequeue -> {"user_id": 2, "provider": "companies_house"}
        call_dequeue().expect("companies_house", 2),
        # 6. Dequeue -> {"user_id": 1, "provider": "bank_statements"}
        call_dequeue().expect("bank_statements", 1),
    ])


def test_bank_statements_last_with_rule_of_3() -> None:
    """When Rule of 3 applies, bank_statements is still last among user's tasks."""
    run_queue([
        # User 1 has 3 tasks - Rule of 3 applies
        call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
        call_enqueue("id_verification", 1, "2025-10-20 12:01:00").expect(2),
        call_enqueue("companies_house", 1, "2025-10-20 12:02:00").expect(3),
        # User 2 has 1 task
        call_enqueue("id_verification", 2, "2025-10-20 12:00:00").expect(4),
        # User 1's tasks are HIGH priority, but bank_statements is last
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
    ])



