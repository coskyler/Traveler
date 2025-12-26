from src.db import pool
from src.jobs.handler import classify_operator
import src.jobs.export as export
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import csv

MAX_CONCURRENT_JOBS = 10
JOB_LIMIT = 25
START_ROW = 0

export.start()

def job(t):
    row = list(t[3:])

    operator_url = row[9]
    # print(operator_url)

    classification, status = classify_operator(operator_url)
    row[11] = classification
    row.append(status)

    export.append_csv(row)

with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS) as ex:
    inflight = set()

    for _ in range(JOB_LIMIT):
        with pool.connection() as conn, conn.cursor() as cur:
            conn.execute("BEGIN")
            cur.execute("""
                WITH job AS (
                    SELECT id
                    FROM jobs
                    WHERE status = 'queued'
                      AND id >= %s
                    ORDER BY id
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE jobs
                SET status = 'running'
                FROM job
                WHERE jobs.id = job.id
                RETURNING jobs.*;
            """, (START_ROW,))
            row = cur.fetchone()
            conn.execute("COMMIT")

        if row is None:
            break

        inflight.add(ex.submit(job, row))

        if len(inflight) >= MAX_CONCURRENT_JOBS:
            done, inflight = wait(inflight, return_when=FIRST_COMPLETED)

pool.close()
export.close_csv()