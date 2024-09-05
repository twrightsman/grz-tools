_PACKAGE_ROOT = "grz_upload"

_LOGGING_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_LOGGING_DATEFMT = "%Y-%m-%d %I:%M %p"
_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {
            "format": _LOGGING_FORMAT,
            "datefmt": _LOGGING_DATEFMT,
        },
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["default"],
            "level": "WARNING",
            "propagate": False,
        },
        _PACKAGE_ROOT: {"handlers": ["default"], "level": "INFO", "propagate": False},
        "__main__": {  # if __name__ == '__main__'
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}
