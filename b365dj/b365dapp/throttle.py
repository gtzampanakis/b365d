import time

_BUSY_WAIT_INTERVAL = .01

def get_time():
    return time.time()

class Throttler:
    
    def __init__(self, rph):
        self.interval = 3600. / rph
        self.last_call_at = -1

    def call(self, fn, *args, **kwargs):
        while True:
            if get_time() - self.last_call_at < self.interval:
                time.sleep(_BUSY_WAIT_INTERVAL)
            else:
                self.last_call_at = get_time()
                return fn(*args, **kwargs)
        
