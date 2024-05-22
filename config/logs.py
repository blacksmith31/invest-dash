import json
import logging.config
import logging.handlers
import pathlib


logger = logging.getLogger("jobs")

def setup_logging():
    configfile = pathlib.Path(__file__).resolve().parent / "logging_config.json"
    with open(configfile) as f:
        config = json.load(f)
    logging.config.dictConfig(config)

def main():
    setup_logging()
    logger.debug("debug msg")
    logger.info("info msg")

if __name__ == "__main__":
    main()

