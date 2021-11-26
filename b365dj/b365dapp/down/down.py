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

import collections
import datetime
import decimal
import json
import logging
import os
import queue
import re
import threading
import time
from pprint import pprint

import pytz
import requests
import schedule

import b365dapp.down.dao as dao
import b365dapp.util as util
import b365dapp.throttle as throttle

LOGGER = logging.getLogger(__name__)

GLOBAL_SLEEP_TIME = .1

MAX_SESSION_AGE = 15 * 60

internal = 'internal'
from_api = 'from_api'

ts = 'timestamp'

# This is the time at which the current period (first/second half) started, in
# London time.
ps = 'period_start'

mm = 'match_minutes'
ms = 'match_seconds'
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
hah = 'halftime_asian_handicap'
hahh = 'halftime_asian_handicap_home_odds'
haha = 'halftime_asian_handicap_away_odds'
htl = 'halftime_total_line'
htlo = 'halftime_total_line_over'
htlu = 'halftime_total_line_under'
game_id = 'game_id'

fah = 'first_asian_handicap'
fahh = 'first_asian_handicap_home_odds'
faha = 'first_asian_handicap_away'
ftl = 'first_total_line'
ftlo = 'first_total_line_over'
ftlu = 'first_total_line_under'


ON_TARGET = "On Target"
OFF_TARGET = "Off Target"
ATTACKS = "Attacks"
DANGEROUS_ATTACKS = "Dangerous Attacks"
POSSESSION = "Possession %"

STATS_DESCS = [
    ON_TARGET,
    OFF_TARGET,
    ATTACKS,
    DANGEROUS_ATTACKS,
    POSSESSION,
]

BETS_API_TOKEN = os.environ['BETS_API_TOKEN']

EVENT_LIST_URL = (
	'https://api.betsapi.com/v1/bet365/inplay?token=%s' % (BETS_API_TOKEN))
EVENT_INFO_URL = 'https://api.betsapi.com/v1/bet365/event?token=%s&FI=%s'
EVENT_STATS_URL = 'https://api.betsapi.com/v1/bet365/event?token=%s&FI=%s&stats=1'

TZ_UTC = pytz.timezone('UTC')
TZ_LONDON = pytz.timezone('Europe/London')



def get_event_info_url(fi):
    return EVENT_INFO_URL % (BETS_API_TOKEN, fi)


def get_event_stats_url(fi):
    return EVENT_STATS_URL % (BETS_API_TOKEN, fi)


def combine(odds_list):
    """
    Input example:

    [{'current_mg_na': 'Match Goals',
      'handicap': None,
      'odds': None,
      'selection': ' '},
     {'current_mg_na': 'Match Goals',
      'handicap': '1.5',
      'odds': 1.6153846153846154,
      'selection': 'Over'},
     {'current_mg_na': 'Match Goals',
      'handicap': '1.5',
      'odds': 2.2,
      'selection': 'Under'},
     {'current_mg_na': 'Asian Handicap (0-0)',
      'handicap': '0',
      'odds': 1.8,
      'selection': 'Barcelona (Jkey) Esports'},
     {'current_mg_na': 'Asian Handicap (0-0)',
      'handicap': '0',
      'odds': 1.9,
      'selection': 'Sevilla (Kodak) Esports'},
     {'current_mg_na': 'Goal Line (0-0)',
      'handicap': None,
      'odds': None,
      'selection': ' '},
     {'current_mg_na': 'Goal Line (0-0)',
      'handicap': '1.75',
      'odds': 1.8,
      'selection': 'Over'},
     {'current_mg_na': 'Goal Line (0-0)',
      'handicap': '1.75',
      'odds': 1.9,
      'selection': 'Under'},

      Output example:
        [('ah',
          ('WestCoastUTD (WCU) Esports',
           '0',
           1.625,
           'Kabush (KAB) Esports',
           '0',
           2.15)),
         ('hah', None),
         ('gl', ('Over', '4.5', 1.775, 'Under', '4.5', 1.925)),
         ('hgl', None)]
    """
    result = []
    for key, category_fn, is_asian in [
        ('ah',
            lambda o: o['current_mg_na'].startswith('Asian Handicap ('), True),
        ('hah',
            lambda o: o['current_mg_na'].startswith('1st Half Asian Handicap ('), True),
        ('gl',
            lambda o: o['current_mg_na'].startswith('Goal Line ('), False),
        ('hgl',
            lambda o: o['current_mg_na'].startswith('1st Half Goal Line ('), False),
    ]:
        min_diff = float('inf')
        min_diff_achiever = None
        for i in range(len(odds_list)):
            obja = odds_list[i]
            if category_fn(obja):
                sela = obja['selection']
                oddsa = obja['odds']
                for j in range(i+1, len(odds_list)):
                    objb = odds_list[j]
                    if (
                        obja['current_mg_na'] == objb['current_mg_na']
                            and
                        obja['selection'] != objb['selection']
                            and
                        (
                                is_asian and (
                                        obja['handicap'] == '0'
                                    and objb['handicap'] == '0'
                                        or
                                        obja['handicap'].startswith('-')
                                    and objb['handicap']
                                            == obja['handicap'].replace('-', '+')
                                        or
                                        obja['handicap'].startswith('+')
                                    and objb['handicap']
                                            == obja['handicap'].replace('+', '-')
                                )
                                or not is_asian and (
                                    obja['handicap'] == objb['handicap']
                                )
                        )
                    ):
                        selb = objb['selection']
                        oddsb = objb['odds']
                        diff = abs(oddsa - oddsb)
                        if diff < min_diff:
                            min_diff = diff
                            min_diff_achiever = (
                                sela, obja['handicap'], oddsa,
                                selb, objb['handicap'], oddsb,
                            )
        result.append((key, min_diff_achiever))
    return result


class HTTPCaller:
    def __init__(self, max_concurrent_reqs, rph):
        self.throttler = throttle.Throttler(rph)
        self.request_semaphore = threading.Semaphore(max_concurrent_reqs)
        self.timeout = (30., 30.)
        self.thrloc = threading.local()

    def get_session(self):
        if getattr(self.thrloc, 'session', None) is None:
            self.thrloc.session = requests.Session()
            self.session_t0 = time.time()
            return self.thrloc.session
        else:
            if time.time() - self.session_t0 > MAX_SESSION_AGE:
                LOGGER.debug('Replacing session because it reached its max age')
                if self.thrloc.session is not None:
                    self.thrloc.session.close()
                self.thrloc.session = None
                return self.get_session()
            else:
                return self.thrloc.session

    def call_via_throttler(self, *args, **kwargs):
        with self.request_semaphore:
            return self.throttler.call(*args, **kwargs)

    def get_event_list(self):
        url = EVENT_LIST_URL
        LOGGER.info('Calling event_list URL: %s', url)
        response = self.call_via_throttler(self.get_session().get, url, timeout=self.timeout)
        assert response.status_code == 200, response.status_code
        j = response.json()
        assert j['success'] == 1
        return j['results'][0]

    def get_event_info(self, fi):
        url = get_event_info_url(fi)
        LOGGER.info('Calling get_event_info URL: %s', url)
        response = self.call_via_throttler(self.get_session().get, url, timeout=self.timeout)
        assert response.status_code == 200, response.status_code
        j = response.json()
        assert j['success'] == 1
        return EventInfo(j['results'][0]), int(j['stats']['update_at'])

    def get_event_stats(self, fi):
        url = get_event_stats_url(fi)
        LOGGER.info('Calling get_event_stats URL: %s', url)
        response = self.call_via_throttler(self.get_session().get, url, timeout=self.timeout)
        assert response.status_code == 200, response.status_code
        j = response.json()
        assert j['success'] == 1
        return EventInfo(j['results'][0])

class EventListUpdater:
    def __init__(self, exit_when, http_caller, update_list_interval):
        self.exit_when = exit_when
        self.http_caller = http_caller
        self.update_list_interval = update_list_interval
        self.t_last_call = 0.
        self.fis_time = (set(), 0)

    @staticmethod
    def event_list_to_fis(event_list):
        fis = []
        current_sport = None
        for obj in event_list:
            if obj['type'] == 'CL':
                current_sport = obj['ID']

            if current_sport == '1': # The code for Soccer is 1
                if obj['type'] == 'EV':
# We used the 'FI' key here prior to 2020-09. Then the 'FI' key was removed and
# an email to betsapi.com support revealed that we should use the 'ID' key from
# then on, even though the 'FI' key is still used in other places and the 'FI'
# key still appears in the query parameters of the URLs (even though we now
# need to pass the value of the 'ID' field in those query parameters and if the
# value of the 'FI' key is passed we get "invalid parameter" error).
                    if 'ID' in obj:
# As of 2021-11: Example value: 111226223C1A_1_9. Trial and error has shown
# that only the digits in the beginning of the string matter. If the full value
# is used (including the characters after the digits) the API calls still
# return the correct data, but in this case the problem is that the final part
# switches between 1_3, 1_9 and 1_11 which causes us to save multiple rows in
# the database for a single event.
                        mo = re.match(r'^\d+', obj['ID'])
                        if mo:
                            fi = mo.group(0)
                            fis.append(fi)
        return fis

    def expire_current_states(self, fis):
        dao.expire_current_states(fis)

    def run_cycle(self):
        event_list = self.http_caller.get_event_list()
        fis = self.event_list_to_fis(event_list)
        self.expire_current_states(fis)
        self.fis_time = (set(fis), time.time())

    def run(self):
        while not self.exit_when.is_set():
            if time.time() - self.t_last_call > self.update_list_interval:
                self.t_last_call = time.time()
                try:
                    self.run_cycle()
                except Exception as e:
                    LOGGER.exception(e)
            else:
                time.sleep(GLOBAL_SLEEP_TIME)
        
class FisDistributor:
    def __init__(
        self, exit_when, http_caller,
        event_list_updater, update_fi_interval,
    ):
        self.exit_when = exit_when
        self.http_caller = http_caller
        self.event_list_updater = event_list_updater
        self.update_fi_interval = update_fi_interval
        self.fi_to_time = {}
        self.last_time_used = -1
        self.message_queue = queue.Queue(10 * 1000)
        self.stale_after = 15 * 60
    
    def send_update_message(self, fi):
        item = {
            'type': 'update',
            'fi': fi,
        }
# Using timeout so we can keep checking the exit_when flag.
        timeout = .5
        while not self.exit_when.is_set():
            try:
                self.message_queue.put(item, timeout=timeout)
                return
            except queue.Full:
                time.sleep(GLOBAL_SLEEP_TIME)

    def run(self):
        while not self.exit_when.is_set():
            fis, time_ = self.event_list_updater.fis_time

            fis_to_delete = [ fi for fi in self.fi_to_time if fi not in fis ]
            for fi in fis_to_delete:
                del self.fi_to_time[fi]

            if time_ > time.time() - self.stale_after:
                if time_ > self.last_time_used:
                    for fi in fis:
                        if fi not in self.fi_to_time:
                            self.fi_to_time[fi] = 0.
                    self.last_time_used = time_
                
                for fi in self.fi_to_time:
                    if self.exit_when.is_set():
                        break
                    if time.time() - self.fi_to_time[fi] > self.update_fi_interval:
                        self.send_update_message(fi)
                        self.fi_to_time[fi] = time.time()
            else:
                LOGGER.warning('Received stale fis, ignored them.')

            time.sleep(GLOBAL_SLEEP_TIME)

class FisUpdater:
    
    def __init__(self, exit_when, http_caller, fis_distributor):
        self.exit_when = exit_when
        self.http_caller = http_caller
        self.fis_distributor = fis_distributor
    
    @staticmethod
    def get_match_time(da_ps, event_info, update_at):
        # if (TT)
        # secs = now - TU(convert into unix epoch) + TM * 60 + TS
        # }else {
        #     secs = TM*60 + TS
        # }
        TT = event_info.find('EV', 'TT')
        TM = event_info.find('EV', 'TM')
        TS = event_info.find('EV', 'TS')

        if not TT or not TM or not TS or not da_ps:
            return None

        TT = int(TT)
        TM = int(TM)
        TS = int(TS)

        if TT == 1:
            update_at_dt = TZ_UTC.localize(datetime.datetime.utcfromtimestamp(update_at))
            seconds = (update_at_dt - da_ps).total_seconds() + TM * 60 + TS
        else:
            seconds = TM * 60 + TS

        seconds = int(seconds)

        return seconds / 60, seconds % 60

    def handle_event_info(self, fi, event_info, event_stats, update_at):
        d = {from_api: {}, internal: {}}
        di = d[internal]
        da = d[from_api]
        
        assert fi is not None
        da[game_id] = fi

        da[ps] = util.safe_apply(
            event_info.find('EV', 'TU'),
            lambda o: datetime.datetime(
                int(o[0:4]),
                int(o[4:6]),
                int(o[6:8]),
                int(o[8:10]),
                int(o[10:12]),
                int(o[12:14])),
            TZ_LONDON.localize)
        
        try:
            match_time = self.get_match_time(da[ps], event_info, update_at)
            da[mm] = match_time[0]
            da[ms] = match_time[1]
        except Exception as e:
            LOGGER.exception(e)

        for k,i in zip([chg, cag], [0,1]):
            da[k] = util.safe_apply(
                event_info.find('EV', 'SS'),
                lambda o: o.split('-'),
                lambda o: o[i])

        da[league] = event_info.find('EV', 'CT')

# This appears to be the best way to get the team names. Initially I found them
# in the Fulltime Result odds but then I found that those odds are not always
# available.
        segments = event_info.find('EV', 'NA').split(' v ')
        da[home] = segments[0]
        da[away] = segments[1]

        all_odds = event_info.get_odds()
        combined = combine(all_odds)

        # Set them all to None in order to play nicely with
        # sanity_check_for_record().
        da[ah] = None
        da[ahho] = None
        da[ahao] = None
        da[hah] = None
        da[hahh] = None
        da[haha] = None
        da[tl] = None
        da[tlo] = None
        da[tlu] = None
        da[htl] = None
        da[htlo] = None
        da[htlu] = None

        for key, odds_tuple in combined:
            if odds_tuple is None:
                continue
            sela, handa, oddsa, selb, handb, oddsb = odds_tuple
            if key == 'ah':
                da[ah] = float(handa)
                da[ahho] = oddsa
                da[ahao] = oddsb
            elif key == 'hah':
                da[hah] = float(handa)
                da[hahh] = oddsa
                da[haha] = oddsb
            elif key == 'gl':
                da[tl] = float(handa)
                da[tlo] = oddsa if sela == 'Over' else oddsb
                da[tlu] = oddsa if sela == 'Under' else oddsb
            elif key == 'hgl':
                da[htl] = float(handa)
                da[htlo] = oddsa if sela == 'Over' else oddsb
                da[htlu] = oddsa if sela == 'Under' else oddsb

        # Start stats.
        if event_stats is not None:
            desc_to_field = {}
            for suffix in range(1, 8):
                field = 'S' + str(suffix)
                value = event_stats.find('EV', field)
                if value in STATS_DESCS:
                    desc_to_field[value] = field
            
            if ATTACKS in desc_to_field:
                for k,i in zip([atth, atta], [0, 1]):
                    da[k] = util.safe_apply(
                        event_stats.find('EV->TE', desc_to_field[ATTACKS]),
                        lambda l: l[i],
                        float)

            if DANGEROUS_ATTACKS in desc_to_field:
                for k,i in zip([datth, datta], [0, 1]):
                    da[k] = util.safe_apply(
                        event_stats.find('EV->TE', desc_to_field[DANGEROUS_ATTACKS]),
                        lambda l: l[i],
                        float)

            if POSSESSION in desc_to_field:
                for k,i in zip([ph, pa], [0, 1]):
                    da[k] = util.safe_apply(
                        event_stats.find('EV->TE', desc_to_field[POSSESSION]),
                        lambda l: l[i],
                        float)

            if ON_TARGET in desc_to_field:
                for k,i in zip([shnh, shna], [0, 1]):
                    da[k] = util.safe_apply(
                        event_stats.find('EV->TE', desc_to_field[ON_TARGET]),
                        lambda l: l[i],
                        float)

            if OFF_TARGET in desc_to_field:
                for k,i in zip([shoh, shoa], [0, 1]):
                    da[k] = util.safe_apply(
                        event_stats.find('EV->TE', desc_to_field[OFF_TARGET]),
                        lambda l: l[i],
                        float)

            for k,i in zip([ch, ca], [0, 1]):
                da[k] = util.safe_apply(
                    event_stats.find('SC->SL', 'D1',
                        lambda o: o.get('NA', '') == 'ICorner'),
                    lambda l: l[i],
                    float)

            for k,i in zip([ych, yca], [0, 1]):
                da[k] = util.safe_apply(
                    event_stats.find('SC->SL', 'D1',
                        lambda o: o.get('NA', '') == 'IYellowCard'),
                    lambda l: l[i],
                    float)

            for k,i in zip([rch, rca], [0, 1]):
                da[k] = util.safe_apply(
                    event_stats.find('SC->SL', 'D1',
                        lambda o: o.get('NA', '') == 'IRedCard'),
                    lambda l: l[i],
                    float)
        # End stats.

        self.sanity_check_for_record(d, event_info)
        dao.save_record(d)

    
    def sanity_check_for_record(self, record, event_info):
        record = record['from_api']

        if (
                record[ah] is not None
            and record[hah] is not None
            and (
                    record[hah] * record[ah] < 0
                or  abs(record[hah]) > abs(record[ah])
            )
        ):
            LOGGER.warning(
                'Found record with hah > ah: %s > %s',
                record[hah], record[ah])

        if (
                record[tl] is not None
            and record[htl] is not None
            and (
                    record[htl] * record[tl] < 0
                or  abs(record[htl]) > abs(record[tl])
            )
        ):
            LOGGER.warning(
                'Found record with htl > tl: %s > %s',
                record[htl], record[tl])
    
    def run_for_fi(self, fi):
        if self.exit_when.is_set():
            return
        event_info, update_at = self.http_caller.get_event_info(fi)
        try:
            if self.exit_when.is_set():
                return
            event_stats = self.http_caller.get_event_stats(fi)
        except Exception as e:
            LOGGER.exception(e)
            event_stats = None
        self.handle_event_info(fi, event_info, event_stats, update_at)

    def run(self):
# Using timeout so we can keep checking the exit_when flag.
        timeout = .5
        while not self.exit_when.is_set():
            try:
                item = self.fis_distributor.message_queue.get(timeout=timeout)
                assert item['type'] == 'update'
                fi = item['fi']
                try:
                    self.run_for_fi(fi)
                except Exception as e:
                    LOGGER.exception(e)
            except queue.Empty:
                time.sleep(GLOBAL_SLEEP_TIME)

class EventInfo:
    
    def __init__(self, evinfolist):
        self.evinfolist = evinfolist
    
    def get_odds(self):
        results = []
        current_mg_na = None
        current_selection = None
        for obji, obj in enumerate(self.evinfolist):
            if obj.get('type') == 'MG':
                current_mg_na = obj.get('NA')
            if obj.get('type') == 'MA':
                current_selection = obj.get('NA') and obj.get('NA').strip()
            if current_mg_na and current_selection and obj.get('type') == 'PA':
                results.append({
                    'current_mg_na': current_mg_na,
                    'selection': current_selection,
                    'handicap': obj.get('HA'),
                    'odds': obj.get('OD') and util.frac_to_dec(obj.get('OD')),
                })
        return results

    def find(self, type, field, first_condition_fn = None):
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
                        if not first_condition_fn or first_condition_fn(obj):
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
            return EventInfo(self.evinfolist[first_index:second_index]).find(
                type_posterior, field)
        else:
            result = []
            for obj in self.evinfolist:
                if type == '*' or obj.get('type') == type:
                    if field is not None:
                        result.append(obj.get(field)) 
                    else:
                        result.append(obj)
            if len(result) == 1:
                return result[0]
            else:
                return result


class Scheduler:
    def __init__(self, exit_when):
        self.exit_when = exit_when

    def run(self):
        schedule.every().day.at('01:00').do(dao.trim_data)
        schedule.every().tuesday.at('03:00').do(dao.vacuum)

        while not self.exit_when.is_set():
            schedule.run_pending()
            time.sleep(GLOBAL_SLEEP_TIME)

def run(
    reqs_per_hour, max_concurrent_reqs, n_threads,
    update_list_interval, update_fi_interval
):
    exit_when = threading.Event()

    http_caller = HTTPCaller(max_concurrent_reqs, reqs_per_hour)

    event_list_updater = EventListUpdater(
        exit_when, http_caller, update_list_interval)
    event_list_updater_thread = threading.Thread(
        target = event_list_updater.run,
        name = 'event_list_updater'
    )

    fis_distributor = FisDistributor(
        exit_when, http_caller, event_list_updater, update_fi_interval)
    fis_distributor_thread = threading.Thread(
        target = fis_distributor.run,
        name = 'fis_distributor'
    )

    all_threads = [
        event_list_updater_thread,
        fis_distributor_thread,
    ]

    for workeri in range(n_threads):
        worker = FisUpdater(exit_when, http_caller, fis_distributor)
        worker_thread = threading.Thread(
            target = worker.run,
            name = 'fis_updater %s/%s' % (workeri+1, n_threads)
        )
        all_threads.append(worker_thread)

    scheduler = Scheduler(exit_when)
    scheduler_thread = threading.Thread(
        target = scheduler.run,
        name = 'scheduler',
    )
    all_threads.append(scheduler_thread)

    try:
        for thread in all_threads:
            thread.start()

        while True:
            non_alive_thread = None
            for t in all_threads:
                if not t.is_alive():
                    non_alive_thread = t
                    break
            if non_alive_thread:
                LOGGER.error('Thread is not alive: %s', non_alive_thread)
                break
            time.sleep(GLOBAL_SLEEP_TIME)

    finally:
        LOGGER.info('Set the exit_when flag.')
        exit_when.set()

        for thread in all_threads:
            thread.join()


