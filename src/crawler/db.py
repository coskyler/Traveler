from psycopg_pool import ConnectionPool
from dotenv import load_dotenv
load_dotenv()

import os
DATABASE_URL = os.environ["DATABASE_URL"]

pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=10,
    timeout=30,
)