import random

def compute_backoff_seconds(attempts: int, base: float = 2.0, max_delay: float = 300.0, jitter: float = 5.0) -> float:
    """attempts = number of attempts already made (>=1). Exponential growth, capped, plus jitter
    so many simultaneously-failing jobs don't all retry at the exact same instant."""
    delay = min(base * (2 ** (attempts - 1)), max_delay)
    return delay + random.uniform(0, jitter)