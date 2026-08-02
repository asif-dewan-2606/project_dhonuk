import logging
import os

from config import load_config
from manager import run_all
from registry import build_pipelines


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
    )


def main():
    config_dir = os.environ.get("CONFIG_DIR", os.path.join(os.path.dirname(__file__), "config"))
    application, pipelines_config = load_config(config_dir)

    setup_logging(application.get("logging", {}).get("level", "INFO"))
    logger = logging.getLogger(__name__)

    runners = build_pipelines(application, pipelines_config)

    if not runners:
        logger.warning("No enabled pipelines found in pipelines.yaml - nothing to do")
        return

    run_all(runners)


if __name__ == "__main__":
    main()
