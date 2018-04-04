import logging

import b365dapp.models

LOGGER = logging.getLogger(__name__)

def save_record(record):
    LOGGER.debug('save_record: Received record: %s', record)
    db_obj = b365dapp.models.EventState.objects.create(
        **record['from_api']
    )
