import datetime
import threading
import logging

from django.db import transaction, connection
from django.db.models import Q
from django.utils import timezone

from b365dapp.models import (
    EventState, CurrentEventState)

LOGGER = logging.getLogger(__name__)

_LOCK = threading.Lock()

def save_record(record):
    d = record['from_api']

    for col_list in [
        [
            'asian_handicap',
            'asian_handicap_home_odds',
            'asian_handicap_away_odds'
        ],
        [
            'total_line',
            'total_line_over',
            'total_line_under'
        ],
    ]:
        # Find the earliest record that has the values set.
        filter_attrs = dict((col + '__isnull', False) for col in col_list)
        filter_attrs['game_id'] = d['game_id']
        first_record = EventState.objects.filter(
            **filter_attrs).order_by('created_at').first()

        if first_record is None:
            are_all_populated = all(d.get(col) for col in col_list)
            if are_all_populated:
                for col in col_list:
                    d['first_' + col] = d[col]
        else:
            for col in col_list:
                d['first_' + col] = getattr(first_record, col)

    LOGGER.debug('save_record: Saving record: %s', d)

    with _LOCK:
        db_obj = EventState.objects.create(**d)

# It's important not to change id for existing game_id. If it is changed then
# when exporting CurrentEventStates using a list of id values (as it is done by
# the admin action) any events that have been updated in the meantime will be
# omitted by the export.
    existing = CurrentEventState.objects.filter(game_id = db_obj.game_id).first()

    if existing is None:
        id_to_save = None
    else:
        id_to_save = existing.id

    d['id'] = id_to_save

    with _LOCK:
        with transaction.atomic():
            CurrentEventState.objects.filter(game_id = db_obj.game_id).delete()
            CurrentEventState.objects.create(**d)


def expire_current_states(fis):
    with _LOCK:
        info = CurrentEventState.objects.exclude(
# As of 2020-09-09 sometimes two successive calls to the event list URL will
# give different lists. This appears to be a bug on betsapi.com. To work around
# it we will filter out CurrentEventState records that have been updated
# recently.
            Q(game_id__in = fis)
                |
            Q(created_at__gt = timezone.now() - datetime.timedelta(minutes=5))
        ).delete()

def vacuum():
    try:
        LOGGER.info('vacuum: start')
        with _LOCK:
            cursor = connection.cursor()
            cursor.execute('vacuum')
        LOGGER.info('vacuum: end')
    except Exception as e:
        LOGGER.exception(e)

def trim_data():
    try:
        LOGGER.info('trim_data: start')
        with _LOCK:
            info = EventState.objects.filter(
                created_at__lt = timezone.now() - datetime.timedelta(days=7)
            ).delete()
        if info[0]:
            LOGGER.info('Deleted %s old event states', info[0])
        LOGGER.info('trim_data: end')
    except Exception as e:
        LOGGER.exception(e)
