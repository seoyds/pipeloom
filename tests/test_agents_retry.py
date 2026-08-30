from pipeloom.agents.retry import RetryPolicy, compute_backoff_delay


def test_retry_policy_has_sensible_defaults():
    policy = RetryPolicy()
    assert policy.max_attempts == 3
    assert policy.backoff_base == 0.5
    assert policy.jitter is True


def test_retry_policy_fields_are_overridable():
    policy = RetryPolicy(max_attempts=5, backoff_base=1.0, jitter=False)
    assert policy.max_attempts == 5
    assert policy.backoff_base == 1.0
    assert policy.jitter is False


def test_compute_backoff_delay_without_jitter_is_exact_exponential():
    policy = RetryPolicy(backoff_base=1.0, jitter=False)
    assert compute_backoff_delay(policy, attempt=1) == 1.0
    assert compute_backoff_delay(policy, attempt=2) == 2.0
    assert compute_backoff_delay(policy, attempt=3) == 4.0


def test_compute_backoff_delay_with_jitter_is_bounded():
    policy = RetryPolicy(backoff_base=1.0, jitter=True)
    for _ in range(20):
        delay = compute_backoff_delay(policy, attempt=3)
        assert 0.0 <= delay <= 4.0
