import logging
import os
import threading
import time

from registry import ProducerRunner

logger = logging.getLogger(__name__)


def run_pipeline(runner: ProducerRunner) -> None:
    """
    Blocking loop for one producer pipeline: poll the source, publish
    whatever it returns, sleep for the source's own poll interval, repeat.
    This loop never inspects record contents - each Source owns its own
    "what's new" logic (watermark, rate limiting).

    Any exception here is logged and re-raised, deliberately crashing this
    pipeline's thread. run_all() treats a dead thread as fatal for the
    whole process, so the container orchestrator restarts it rather than
    leaving a source silently polling nothing.
    """
    name = runner.name

    logger.info("[%s] starting", name)

    try:
        while True:
            records = runner.source.poll()

            for record in records:
                runner.publisher.publish(record)

            time.sleep(runner.poll_interval_seconds)

    except Exception:
        logger.exception("[%s] pipeline crashed", name)
        raise
    finally:
        runner.source.close()
        runner.publisher.close()
        logger.info("[%s] stopped", name)


def run_all(runners: list[ProducerRunner]) -> None:
    """
    Runs every source pipeline concurrently in its own thread - same
    rationale as the consumer side: each pipeline blocks its own thread
    only, and a dead thread should take the whole process down so the
    orchestrator restarts everything cleanly.
    """
    threads = [
        threading.Thread(target=run_pipeline, args=(runner,), name=runner.name, daemon=True)
        for runner in runners
    ]

    for t in threads:
        t.start()

    while True:
        for t in threads:
            t.join(timeout=1)
            if not t.is_alive():
                logger.critical("Producer pipeline thread '%s' died - exiting process", t.name)
                os._exit(1)
