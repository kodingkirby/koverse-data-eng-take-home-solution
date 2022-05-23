import json
import time
import random
from time import sleep
from threading import Timer

#run timer in seperate thread
class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False

def generate_json():
    hex_values = []
    callsigns = []
    hex_string = '0123456789abcdef'
    letters = 'abcdefghijklmnopqrstuvwxyz'
    numbers = '0123456789'
    for i in range(300):
        string = ""
        hex_values.append((string.join(random.choice(hex_string) for x in range(6))))
    list(set(hex_values)) # remove dupes

    for i in range(len(hex_values)):
        string = ""
        prefix = string.join(random.choice(letters) for x in range(3))
        val = string.join(random.choice(numbers) for x in range(4))
        callsigns.append(prefix+val)

    curr_time = int(time.time()) # get epoch seconds

    #template
    flights = {"now" : curr_time,
        "aircraft" : [
            {"hex": t,"flight":s,"alt_geom":10475,"gs":295.1,"track":91.2,"lat":39.874878,"lon":-104.454186} for t, s in zip(hex_values, callsigns)]
    }

    #write output to file
    with open('flights.json', 'w', encoding='utf-8') as f:
        json.dump(flights, f, ensure_ascii=False)
        print("wrote json file! @ ")
        print(curr_time)

if __name__ == "__main__":
    rt = RepeatedTimer(1, generate_json) # run every 1 second
    try:
        sleep(3600) # run for 1 hr
    except Exception as e:
        print(f"Something went wrong! Here's what: {e}")
    finally:
        rt.stop()