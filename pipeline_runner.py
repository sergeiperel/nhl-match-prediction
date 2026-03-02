import logging
import time
from collections.abc import Callable


class PipelineRunner:
    def __init__(self, fail_fast: bool = False):
        self.fail_fast = fail_fast
        self.logger = logging.getLogger(__name__)
        self.steps_summary: list[dict] = []
        self.start_time = None

    def run_step(self, name: str, func: Callable, enabled: bool = True, *args, **kwargs):
        if not enabled:
            self.logger.info(f"[SKIPPED] {name}")
            self.steps_summary.append({"name": name, "status": "SKIPPED", "duration": 0})
            return

        self.logger.info(f"[START] {name}")
        start_time = time.perf_counter()

        try:
            func(*args, **kwargs)

            duration = time.perf_counter() - start_time
            self.logger.info(f"✅ [SUCCESS] {name} ({duration:.2f}s)")
            self.steps_summary.append({"name": name, "status": "SUCCESS", "duration": duration})

        except Exception as e:
            duration = time.perf_counter() - start_time
            self.logger.exception(f"❌ [FAILED] {name} ({duration:.2f}s)")

            self.steps_summary.append({"name": name, "status": "FAILED", "duration": duration})

            if self.fail_fast:
                raise e

    def start_pipeline(self):
        self.start_time = time.perf_counter()

    def finish_pipeline(self):
        total_time = time.perf_counter() - self.start_time

        self.logger.info("================================================")
        self.logger.info("📊 Pipeline Summary")

        for step in self.steps_summary:
            self.logger.info(f"{step['name']:<20} | {step['status']:<8} | {step['duration']:.2f}s")

        self.logger.info("================================================")
        self.logger.info(f"Total time: {total_time:.2f}s")
        self.logger.info("================================================")
