from queue import Queue
from threading import Thread
import csv
from datetime import datetime
from pathlib import Path
import traceback

SENTINEL = object()

_q = Queue()

def _consumer():
    try:
        while True:
            row = _q.get()
            if row is SENTINEL:
                _f.close()
                break

            _w.writerow(row)
            _f.flush()
    except Exception:
        traceback.print_exc()

def start():
    global _f, _w
    _f = open(Path("outputs") / f"{datetime.now():%Y%m%d_%H%M%S}.csv", "w", newline="")
    _w = csv.writer(_f)
    Thread(target=_consumer, daemon=True).start()

def append_csv(row):
    _q.put(row)

def close_csv():
    _q.put(SENTINEL)