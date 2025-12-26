from queue import Queue
from threading import Thread

SENTINEL = object()

q = Queue()

def _consumer():
    while True:
        row = q.get()
        if row is SENTINEL:
            break

        print(row)

def start():
    Thread(target=_consumer, daemon=True).start()

def append_csv(row):
    q.put(row)

def close_csv():
    q.put(SENTINEL)