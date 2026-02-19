"""Simple circuit breaker for external service calls."""

import time
import logging

logger = logging.getLogger(__name__)


class CircuitBreaker:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(self, name: str, failure_threshold: int = 5, timeout: int = 60):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.state = self.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0

    @property
    def is_open(self) -> bool:
        if self.state == self.OPEN:
            if time.time() - self.last_failure_time > self.timeout:
                self.state = self.HALF_OPEN
                return False
            return True
        return False

    def record_success(self) -> None:
        if self.state == self.HALF_OPEN:
            logger.info("Circuit breaker %s → CLOSED", self.name)
        self.state = self.CLOSED
        self.failure_count = 0

    def record_failure(self) -> None:
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = self.OPEN
            logger.warning("Circuit breaker %s → OPEN", self.name)

    def reset(self) -> None:
        self.state = self.CLOSED
        self.failure_count = 0
