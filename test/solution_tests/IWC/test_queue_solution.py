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


def test_age_returns_zero_when_empty() -> None:
    queue = QueueSolutionEntrypoint()
    assert queue.age() == 0


def test_age_returns_time_gap_in_seconds() -> None:
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission(provider="id_verification", user_id=1, timestamp="2025-10-20 12:00:00"))
    queue.enqueue(TaskSubmission(provider="id_verification", user_id=2, timestamp="2025-10-20 12:05:00"))
    assert queue.age() == 300


def test_age_with_single_task() -> None:
    queue = QueueSolutionEntrypoint()
    queue.enqueue(TaskSubmission(provider="id_verification", user_id=1, timestamp="2025-10-20 12:00:00"))
    assert queue.age() == 0


def test_dequeue_empty_returns_none() -> None:
    queue = QueueSolutionEntrypoint()
    result = queue.dequeue()
    assert result is None


def test_deduplication() -> None:
    """Duplicate (user_id, provider) pairs are deduplicated."""
    run_queue([
        call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
        call_enqueue("bank_statements", 1, "2025-10-20 12:02:00").expect(1),  # Duplicate, keeps earlier
        call_enqueue("id_verification", 1, "2025-10-20 12:04:00").expect(2),  # < 5 min gap, not time-sensitive
        # bank_statements is LOW priority (not time-sensitive), id_verification comes first
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_deduplication_keeps_earlier_timestamp() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, "2025-10-20 12:10:00").expect(1),
        call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
        call_enqueue("bank_statements", 2, "2025-10-20 12:05:00").expect(2),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_bank_statements_deprioritized() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
        call_enqueue("id_verification", 1, "2025-10-20 12:01:00").expect(2),
        call_enqueue("companies_house", 2, "2025-10-20 12:02:00").expect(3),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 2),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_bank_statements_last_with_rule_of_3() -> None:
    run_queue([
        call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
        call_enqueue("id_verification", 1, "2025-10-20 12:01:00").expect(2),
        call_enqueue("companies_house", 1, "2025-10-20 12:02:00").expect(3),
        call_enqueue("id_verification", 2, "2025-10-20 12:00:00").expect(4),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
    ])


def test_time_sensitive_bank_statements_example1() -> None:
    """Round 5 Example #1: Time-sensitive bank_statements gets prioritized."""
    run_queue([
        # 1. Enqueue: user_id=1, provider="id_verification", timestamp='2025-10-20 12:00:00'
        call_enqueue("id_verification", 1, "2025-10-20 12:00:00").expect(1),
        # 2. Enqueue: user_id=2, provider="bank_statements", timestamp='2025-10-20 12:01:00'
        call_enqueue("bank_statements", 2, "2025-10-20 12:01:00").expect(2),
        # 3. Enqueue: user_id=3, provider="companies_house", timestamp='2025-10-20 12:07:00'
        # bank_statements internal age = 12:07 - 12:01 = 6 min >= 5 min -> time-sensitive
        call_enqueue("companies_house", 3, "2025-10-20 12:07:00").expect(3),
        # 4. Dequeue -> id_verification (oldest timestamp)
        call_dequeue().expect("id_verification", 1),
        # 5. Dequeue -> bank_statements (time-sensitive, comes before companies_house)
        call_dequeue().expect("bank_statements", 2),
        # 6. Dequeue -> companies_house
        call_dequeue().expect("companies_house", 3),
    ])


def test_time_sensitive_bank_statements_example2() -> None:
    """Round 5 Example #2: FIFO tie-breaker for time-sensitive bank_statements."""
    run_queue([
        # 1. Enqueue: user_id=1, provider="id_verification", timestamp='2025-10-20 12:00:00'
        call_enqueue("id_verification", 1, "2025-10-20 12:00:00").expect(1),
        # 2. Enqueue: user_id=2, provider="bank_statements", timestamp='2025-10-20 12:02:00'
        call_enqueue("bank_statements", 2, "2025-10-20 12:02:00").expect(2),
        # 3. Enqueue: user_id=1, provider="bank_statements", timestamp='2025-10-20 12:02:00'
        call_enqueue("bank_statements", 1, "2025-10-20 12:02:00").expect(3),
        # 4. Enqueue: user_id=1, provider="companies_house", timestamp='2025-10-20 12:03:00'
        call_enqueue("companies_house", 1, "2025-10-20 12:03:00").expect(4),
        # 5. Enqueue: user_id=3, provider="companies_house", timestamp='2025-10-20 12:10:00'
        # Both bank_statements have internal age = 12:10 - 12:02 = 8 min >= 5 min
        call_enqueue("companies_house", 3, "2025-10-20 12:10:00").expect(5),
        # 6. Dequeue -> id_verification (oldest timestamp)
        call_dequeue().expect("id_verification", 1),
        # 7. Dequeue -> bank_statements user 2 (time-sensitive, FIFO first)
        call_dequeue().expect("bank_statements", 2),
        # 8. Dequeue -> bank_statements user 1 (time-sensitive, FIFO second)
        call_dequeue().expect("bank_statements", 1),
        # 9. Dequeue -> companies_house user 1
        call_dequeue().expect("companies_house", 1),
        # 10. Dequeue -> companies_house user 3
        call_dequeue().expect("companies_house", 3),
    ])


def test_bank_statements_not_time_sensitive() -> None:
    """When bank_statements internal age < 5 min, normal deprioritization applies."""
    run_queue([
        call_enqueue("bank_statements", 1, "2025-10-20 12:00:00").expect(1),
        call_enqueue("id_verification", 2, "2025-10-20 12:01:00").expect(2),
        # Only 1 minute difference, not time-sensitive
        call_enqueue("companies_house", 3, "2025-10-20 12:01:00").expect(3),
        # bank_statements is still LOW priority
        call_dequeue().expect("id_verification", 2),
        call_dequeue().expect("companies_house", 3),
        call_dequeue().expect("bank_statements", 1),
    ])


