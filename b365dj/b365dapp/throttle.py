import logging
import time

LOGGER = logging.getLogger(__name__)

N_PROJECT_RPH = 30

_BUSY_WAIT_INTERVAL = .01

def get_time():
    return time.time()

class Throttler:
    
    def __init__(self, rph):
        self.interval = 3600. / rph
        self.call_ts = []
        self.last_call_at = -1

    def call(self, fn, *args, **kwargs):
        while True:
            if get_time() - self.last_call_at < self.interval:
                time.sleep(_BUSY_WAIT_INTERVAL)
            else:
                self.last_call_at = get_time()
                self.call_ts.append(self.last_call_at)
                if len(self.call_ts) >= N_PROJECT_RPH:
                    self.call_ts = self.call_ts[-N_PROJECT_RPH:]
                    diff = self.call_ts[-1] - self.call_ts[0]
                    if diff > 0:
                        rph_proj = 3600. / diff * N_PROJECT_RPH
                        LOGGER.info('Projected rph: %s', int(rph_proj))
                return fn(*args, **kwargs)
        
