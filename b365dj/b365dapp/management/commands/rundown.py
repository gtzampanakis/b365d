from django.core.management.base import BaseCommand, CommandError
import b365dapp.down.down

class Command(BaseCommand):
    help = 'Downloads data from betsapi.com'

    def add_arguments(self, parser):
        parser.add_argument('reqs_per_hour', type=int)
        parser.add_argument('mod_keep', type=int)
        parser.add_argument('mod_val', type=int)

    def handle(self, *args, **options):
        updater = b365dapp.down.down.Updater(
            reqs_per_hour = options['reqs_per_hour'],
            mod_keep = options['mod_keep'],
            mod_val = options['mod_val'],
        )
        updater.run()
