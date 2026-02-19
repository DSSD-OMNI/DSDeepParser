"""Prometheus metrics for DSDeepParser."""

from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time
import functools

requests_total = Counter("fetcher_requests_total", "Total HTTP requests", ["source", "status"])
request_duration = Histogram("fetcher_request_duration_seconds", "Request duration", ["source"])
errors_total = Counter("fetcher_errors_total", "Total errors", ["source", "type"])
tasks_completed = Counter("scheduler_tasks_completed_total", "Tasks completed", ["source"])
active_tasks = Gauge("scheduler_active_tasks", "Currently running tasks")
db_size_bytes = Gauge("db_size_bytes", "SQLite database file size")


def start_metrics_server(port: int = 8000) -> None:
    try:
        start_http_server(port)
    except OSError:
        pass  # port already in use


def monitor_request(source: str):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = await func(*args, **kwargs)
                requests_total.labels(source=source, status="success").inc()
                return result
            except Exception as exc:
                requests_total.labels(source=source, status="error").inc()
                errors_total.labels(source=source, type=type(exc).__name__).inc()
                raise
            finally:
                request_duration.labels(source=source).observe(time.time() - start)
        return wrapper
    return decorator
