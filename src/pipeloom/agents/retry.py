import random
from dataclasses import dataclass


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    backoff_base: float = 0.5
    jitter: bool = True


def compute_backoff_delay(policy: RetryPolicy, attempt: int) -> float:
    """Delay before retrying after the `attempt`-th failed attempt (1-indexed).

    Exponential backoff: backoff_base * 2**(attempt-1), optionally randomized
    uniformly in [0, delay) when jitter is enabled.
    """
    delay = policy.backoff_base * (2 ** (attempt - 1))
    if policy.jitter:
        delay = random.uniform(0, delay)
    return delay
