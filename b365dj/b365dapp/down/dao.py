import logging

from django.db import transaction

from b365dapp.models import (
    EventState, CurrentEventState)

LOGGER = logging.getLogger(__name__)

@transaction.atomic
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

    db_obj = EventState.objects.create(**d)

    CurrentEventState.objects.filter(game_id = db_obj.game_id).delete()
    CurrentEventState.objects.create(**d)


def expire_current_states(fis):
    info = CurrentEventState.objects.exclude(game_id__in = fis).delete()
    if info[0]:
        LOGGER.info('Deleted %s expired current event states', info[0])
