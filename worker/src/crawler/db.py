import os
import atexit
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

DATABASE_URL = os.environ["DATABASE_URL"]

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    kwargs={"row_factory": dict_row},
    min_size=1,
    max_size=5,
    open=True,
)

atexit.register(pool.close)

def connect():
    return pool.connection()