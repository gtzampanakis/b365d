import datetime
import fractions
import logging

LOGGER = logging.getLogger(__name__)


def utcnow():
    return datetime.datetime.utcnow()


def safe_apply(obj, *fns):
    for fn in fns:
        try:
            obj = fn(obj)
        except Exception as e:
            LOGGER.exception(e)
            return None
    return obj


def apply(obj, *fns):
    """
    This is for symmetry with safe_apply.
    """
    for fn in fns:
        obj = fn(obj)
    return obj


def frac_to_dec(frac_str):
    return float(fractions.Fraction(frac_str) + 1)
