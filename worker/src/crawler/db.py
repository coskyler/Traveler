import os
import psycopg

DATABASE_URL = os.environ["DATABASE_URL"]

def connect(*, row_factory=None):
    return psycopg.connect(DATABASE_URL, row_factory=row_factory)