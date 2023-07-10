import sys
import logging

logger = logging.getLogger(__package__)
logger.setLevel(logging.DEBUG)


def _check_for_pandas() -> bool:
    """helper method use to check for the presence of optional pandas dependencies

    Returns
    -------
    bool
        True if pandas is installed in currently active Python environment
    """
    try:
        import pandas as pd

    except ImportError:
        return False

    return True
