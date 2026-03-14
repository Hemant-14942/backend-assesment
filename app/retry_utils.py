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
            x = 0
            while x < retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # Check if we hit a Rate Limit (429) - Requirement 3
                    if hasattr(e, 'response') and e.response.status_code == 429:
                        wait = 10  # Specific requirement: wait 10s for 429
                        logger.warning(f"Rate limit hit. Waiting {wait}s...")
                    else:
                        # Exponential backoff: 1s, 2s, 4s - Requirement 2
                        wait = (backoff_in_seconds * (2 ** x))
                    
                    logger.warning(f"Attempt {x+1} failed: {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                    x += 1
            # If all retries fail, raise the final exception
            return func(*args, **kwargs)
        return wrapper
    return decorator