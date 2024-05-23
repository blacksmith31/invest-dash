import json
import logging.config
import logging.handlers
import pathlib


def setup_logging():
    configfile = pathlib.Path(__file__).resolve().parent / "logging_config.json"
    with open(configfile) as f:
        config = json.load(f)
    logging.config.dictConfig(config)

def main():
    logger = logging.getLogger("jobs")
    setup_logging()
    logger.debug("debug msg")
    logger.info("info msg")

if __name__ == "__main__":
    main()

