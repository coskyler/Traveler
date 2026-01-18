from crawler.db import pool
from crawler.handler import classify_operator
import crawler.export as export
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, ALL_COMPLETED
import traceback

MAX_CONCURRENT_JOBS = 25
JOB_LIMIT = 1
START_ROW = 50000

export.start()

def job(t):
    row = list(t[3:])

    operator_url = row[9]
    # print(row)

    category, sub_category, status, input_tokens, cached_tokens, output_tokens = classify_operator(operator_url)
    row[11] = category
    row[12] = sub_category
    row += [status, input_tokens - cached_tokens, cached_tokens, output_tokens]

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
        
        f = ex.submit(job, row)

        def log_exception(fut):
            exc = fut.exception()
            if exc:
                traceback.print_exception(type(exc), exc, exc.__traceback__)

        f.add_done_callback(log_exception)
        inflight.add(f)

        if len(inflight) >= MAX_CONCURRENT_JOBS:
            done, inflight = wait(inflight, return_when=FIRST_COMPLETED)

wait(inflight, return_when=ALL_COMPLETED)

pool.close()
export.close_csv()