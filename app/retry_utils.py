import time
import functools
import logging

# Setup basic logging to track retries
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def retry_with_backoff(retries=3, backoff_in_seconds=1):
    """
    A decorator that retries a function call if it fails.
    Implementing Requirement: 2. Retry Mechanism (3 attempts, Exponential Backoff)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    # Check if we hit a Rate Limit (429) - Requirement 3
                    if hasattr(e, "response") and getattr(e.response, "status_code", None) == 429:
                        wait = 10  # Specific requirement: wait 10s for 429
                        logger.warning("Rate limit (429) hit. Waiting %ds...", wait)
                    else:
                        # Exponential backoff: 1s, 2s, 4s - Requirement 2
                        wait = backoff_in_seconds * (2**attempt)
                    logger.warning("Attempt %d/%d failed: %s. Retrying in %ds...", attempt + 1, retries, e, wait)
                    time.sleep(wait)
            # All 3 attempts failed - re-raise last exception
            raise last_exception
        return wrapper
    return decorator