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


def frac_to_dec(frac_str):
    return float(fractions.Fraction(frac_str) + 1)
