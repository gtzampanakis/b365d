from django.core.management.base import BaseCommand, CommandError
import b365dapp.down.down

import b365dapp.throttle as throttle

class Command(BaseCommand):
    help = 'Downloads data from betsapi.com'

    def add_arguments(self, parser):
        parser.add_argument('reqs_per_hour', type=int)
        parser.add_argument('max_concurrent_reqs', type=int)
        parser.add_argument('n_threads', type=int)
        parser.add_argument('update_list_interval', type=int)
        parser.add_argument('update_fi_interval', type=int)

    def handle(self, *args, **options):
        b365dapp.down.down.run(
            options['reqs_per_hour'],
            options['max_concurrent_reqs'],
            options['n_threads'],
            options['update_list_interval'],
            options['update_fi_interval'],
        )
