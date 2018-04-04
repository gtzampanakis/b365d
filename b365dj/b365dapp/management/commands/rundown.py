from django.core.management.base import BaseCommand, CommandError

class Command(BaseCommand):
    help = 'Downloads data from betsapi.com'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        import b365dapp.down
        updater = b365dapp.down.Updater(3600)
        updater.run()
