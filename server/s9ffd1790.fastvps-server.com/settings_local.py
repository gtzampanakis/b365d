import os

os.environ['WORK_DIR'] = '/usr/b365d/wd'
os.environ['BETS_API_TOKEN'] = 'xxx'

from b365dj.settings import *

SECRET_KEY = (
	'xxx'
	'xxx')
DEBUG = False
ALLOWED_HOSTS.append(
	's9ffd1790.fastvps-server.com')

STATIC_ROOT = os.path.join(WORK_DIR, 'static')
