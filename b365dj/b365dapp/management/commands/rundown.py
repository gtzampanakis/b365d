from django.core.management.base import BaseCommand, CommandError
import b365dapp.down.down

class Command(BaseCommand):
    help = 'Downloads data from betsapi.com'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        updater = b365dapp.down.down.Updater(199999)
        updater.run()
