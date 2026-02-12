# Sets up clean console logging - INFO level, no print spam.
# Logs tell you exactly what's happening!
import logging
import logging.config

logging.config.dictConfig(
    {
        "version": 1,
        "handlers": {"console": {"class": "logging.StreamHandler"}},
        "root": {"handlers": ["console"], "level": "INFO"},
    }
)

logging.info("Script kicked off - ready to roll!")
