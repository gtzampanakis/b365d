# List attached to email what i need in columns. I would have one main window
# where is the latest call for every game running and that i can copy that data
# to excel on the go, and then have the  database page. The db can be flushed
# every week so i save that before. I need that running 24/7 and saving
# database, so i get data every 1-3 mins depending how many games there are,
# that the hourly requests are utilized. This api pricing allows for 3600
# requests/hour.

# 1) Date
# 2) Time
# 3) Current home goals
# 4) Current away goals
# 5) League name
# 6) Home team name
# 7) Away team name
# 8) Asian handicap center line market for home team (for example there might be 0.0,+0.5 , i actually want this number as 0.25 , also 0.0,-0.5 as -0.25 , same for -0.5,-1.0 is -0.75 etc. Ask for help if you need) (for some games there is only one option but bigger games have 5 different options, so i want the centermost row if thats possible, if you need a formula so itd be easier, this is the row that has the two prices with the smallest difference)
# 9) Asian handicap odds for the aformentioned option, home team (the yellow number)
# 10) Asian handicap odds for away
# 11) Goal line center option market (for example 2.5 or 3.75 or 1.25 (the market that has numbers/odds closest to each other, i want that option, like 1.85 1.95 is what i want opposed to 1.58 2.25)
# 12) Goal line over price
# 13) Goal line under price
# 14) Attacks for home team (attacks and possession might not exist for some games, in that case display)
# 15) Attacks for away team
# 16) Dangerous attacks for home
# 17) Dangerous attacks for away
# 18) Possession for home (display as number from 1-100 if its percentage)
# 19) Possession for away
# 20) Shots on target for home
# 21) Shots on target for away
# 22) Shots off target for home
# 23) Shots off target for away
# 24) Corners home
# 25) Corners away
# 26) Yellow cards home
# 27) Yellow cards away
# 28) Red cards home
# 29) Red cards away
# 30) Game id
# 31) First asian handicap market for that game id
# 32) ah home odds
# 33) ah away odds
# 34) First totals market for that game id
# 35) over odds
# 36) under odds
# 37) Halftime ah market
# 38) ah home odds
# 39) ah away odds
# 40) Halftime totals market
# 41) over odds
# 42) under odds

import datetime
import json
import logging
import os
import requests

import dao
import throttle
import util

LOGGER = logging.getLogger(__name__)

internal = 'internal'
from_api = 'from_api'

ts = 'timestamp'

es = 'event_start'
chg = 'current_home_goals'
cag = 'current_away_goals'
league = 'league'
home = 'home_team'
away = 'away_team'
ah = 'asian_handicap'
ahho = 'asian_handicap_home_odds'
ahao = 'asian_handicap_away_odds'
tl = 'total_line'
tlo = 'total_line_over'
tlu = 'total_line_under'
atth = 'attacks_home'
atta = 'attacks_away'
datth = 'dangerous_attacks_home'
datta = 'dangerous_attacks_away'
ph = 'possession_home'
pa = 'possession_away'
shnh = 'shots_on_target_home'
shna = 'shots_on_target_away'
shoh = 'shots_off_target_home'
shoa = 'shots_off_target_away'
ch = 'corners_home'
ca = 'corners_away'
ych = 'yellow_cards_home'
yca = 'yellow_cards_away'
rch = 'red_cards_home'
rca = 'red_cards_away'
game_id = 'game_id'
fah = 'first_asian_handicap'
fahh = 'first_asian_handicap_home_odds'
faha = 'first_asian_handicap_away'
ftl = 'first_total_line'
ftlo = 'first_total_line_over'
ftlu = 'first_total_line_under'
hah = 'halftime_asian_handicap'
hahh = 'halftime_asian_handicap_home_odds'
haha = 'halftime_asian_handicap_away_odds'
htl = 'halftime_total_line'
htlo = 'halftime_total_line_over'
htlu = 'halftime_total_line_under'


BETS_API_TOKEN = os.environ['BETS_API_TOKEN']

EVENT_LIST_URL = (
	'https://api.betsapi.com/v1/bet365/inplay?token=%s' % (BETS_API_TOKEN))

EVENT_INFO_URL = 'https://api.betsapi.com/v1/bet365/event?token=%s&FI=%s'


def get_event_info_url(fi):
    return EVENT_INFO_URL % (BETS_API_TOKEN, fi)


class EventInfo:
    
    def __init__(self, evinfolist):
        self.evinfolist = evinfolist

    def get(self, type, field, first_condition_fn = None):
        if '->' in type:
            segments = type.split('->')
            type_prior = segments[0]
            type_posterior = segments[1]
            first_index = -1
            second_index = -1
            for obji, obj in enumerate(self.evinfolist):
                match = False
                if obj.get('type') == type_prior:
                    if first_index == -1:
                        if first_condition_fn(obj):
                            match = True
                    else:
                        match = True
                if match:
                    match = False
                    if first_index == -1:
                        first_index = obji
                    elif second_index == -1:
                        second_index = obji
                        break
            if (first_index, second_index) == (-1, -1):
                return None
            elif second_index == -1:
                second_index = obji
            return EventInfo(self.evinfolist[first_index:second_index]).get(
                type_posterior, field)
        else:
            result = []
            for obj in self.evinfolist:
                if obj.get('type') == type:
                    result.append(obj.get(field)) 
            if len(result) == 1:
                return result[0]
            else:
                return result


class Updater:

    def __init__(self, rph):
        self.rph = rph
        self.throttler = throttle.Throttler(self.rph)
        self.session = requests.Session()

    def get_event_list(self):
        response = self.throttler.call(self.session.get, EVENT_LIST_URL)
        assert response.status_code == 200
        j = response.json()
        assert j['success'] == 1
        return j['results'][0]

    def get_event_info(self, fi):
        url = get_event_info_url(fi)
        response = self.throttler.call(self.session.get, url)
        assert response.status_code == 200
        j = response.json()
        assert j['success'] == 1
        return EventInfo(j['results'][0])
    
    def handle_event_list(self):
        pass

    def handle_event_info(self, event_info):
        d = {from_api: {}, internal: {}}
        di = d[internal]
        da = d[from_api]

        da[es] = util.safe_apply(
            event_info.get('EV', 'TU'),
            lambda o: datetime.datetime(
                int(o[0:4]),
                int(o[4:6]),
                int(o[6:8]),
                int(o[8:10]),
                int(o[10:12]),
                int(o[12:14])))

        for k,i in zip([chg, cag], [0,1]):
            da[k] = util.safe_apply(
                event_info.get('EV', 'SS'),
                lambda o: o.split('-'),
                lambda o: o[i])

        da[league] = event_info.get('EV', 'CT')

        for k, i in zip([home, away], [0, 2]):
            da[k] = util.safe_apply(
                event_info.get('MA->PA', 'NA',
                               lambda o: o['NA'] == 'Fulltime Result'),
                lambda l: l[i])
        
        da[ah] = util.safe_apply(
            event_info.get('MA->PA', 'HA',
                           lambda o: o['NA'].startswith('Asian Handicap')),
            lambda l: l[0])

        for k,i in zip([ahho, ahao], [0, 1]):
            da[k] = util.safe_apply(
                event_info.get('MA->PA', 'OD',
                               lambda o: o['NA'].startswith('Asian Handicap')),
                lambda l: l[i],
                util.frac_to_dec)

        da[tl] = util.safe_apply(
            event_info.get('MA->PA', 'HA',
                           lambda o: o['NA'] == 'Match Goals'),
            lambda l: l[0])

        for k,i in zip([tlo, tlu], [0, 1]):
            da[k] = util.safe_apply(
                event_info.get('MA->PA', 'OD',
                               lambda o: o['NA'] == 'Match Goals'),
                lambda l: l[i],
                util.frac_to_dec)

        dao.save_record(d)

    def run_cycle(self):
        objs = self.get_event_list()
        for obj in objs:
            if obj['type'] == 'EV':
                fi = obj['FI']
                event_info = self.get_event_info(fi)
                self.handle_event_info(event_info)

    def run(self):
        while True:
            try:
                self.run_cycle()
            except Exception as e:
                LOGGER.exception(e)

if __name__ == '__main__':
    logging.basicConfig(level = logging.DEBUG)
    updater = Updater(3600)
    updater.run()

