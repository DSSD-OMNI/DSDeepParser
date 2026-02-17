"""Async scheduler wrapper around APScheduler."""

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import logging

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler(job_defaults={"misfire_grace_time": 300})

    def add_job(self, func, trigger, id: str, **kwargs):
        if isinstance(trigger, str):
            trigger = CronTrigger.from_crontab(trigger)
        self.scheduler.add_job(func, trigger, id=id, **kwargs)
        logger.info("Scheduled job %s: %s", id, trigger)

    def start(self):
        self.scheduler.start()

    async def shutdown(self):
        self.scheduler.shutdown(wait=False)
