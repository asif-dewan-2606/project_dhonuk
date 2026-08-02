import logging
import os
import threading

from registry import PipelineRunner

logger = logging.getLogger(__name__)


def run_pipeline(runner: PipelineRunner) -> None:
    """
    Blocking loop for one pipeline: poll Kafka, buffer records, and flush
    to the sink + commit offsets once the batch is ready. Offsets are only
    committed after a successful write - pipeline.flush() raises before
    the buffer is cleared if the sink write fails, so a failed batch is
    never lost and never committed.

    Any exception here is logged and re-raised, deliberately crashing this
    pipeline's thread rather than retrying forever silently. run_all()
    treats a dead thread as fatal for the whole process, so the container
    orchestrator (docker restart / k8s) restarts it and Kafka replays the
    uncommitted batch.
    """
    consumer = runner.consumer
    pipeline = runner.pipeline
    name = runner.name

    logger.info("[%s] starting", name)

    try:
        while True:
            msg = consumer.poll()

            if msg is None:
                if pipeline.is_ready():
                    _flush_and_commit(runner)
                continue

            if msg.error():
                logger.error("[%s] Kafka error: %s", name, msg.error())
                continue

            pipeline.add(msg.value())

            if pipeline.is_ready():
                _flush_and_commit(runner)

    except Exception:
        logger.exception("[%s] pipeline crashed", name)
        raise
    finally:
        consumer.close()
        pipeline.sink.close()
        logger.info("[%s] stopped", name)


def _flush_and_commit(runner: PipelineRunner) -> None:
    runner.pipeline.flush()
    runner.consumer.commit()


def run_all(runners: list[PipelineRunner]) -> None:
    """
    Runs every pipeline concurrently in its own thread. Each Kafka
    consumer's poll() blocks its own thread only, so plain threading is
    enough here - no async runtime needed. If any pipeline thread dies,
    the whole process exits so the orchestrator restarts everything
    cleanly instead of limping along with a silently-dead pipeline.
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
                logger.critical("Pipeline thread '%s' died - exiting process", t.name)
                os._exit(1)
