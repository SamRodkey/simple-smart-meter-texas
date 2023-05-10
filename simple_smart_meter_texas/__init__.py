import sys
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def _check_for_pandas() -> bool:
    # TODO: add docstring to _check_for_pandas
    try:
        import pandas as pd

    except ImportError:
        return False

    return True
