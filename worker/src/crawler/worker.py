from crawler.db import connect
from crawler.pipeline.types import OperatorInfo, ClassifyResult
from crawler.pipeline import orchestrator
from crawler.pipeline.trace import Trace
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, ALL_COMPLETED
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
import traceback
import random
import json

MAX_CONCURRENT_JOBS = 25
JOB_LIMIT = 5
START_ROW = 0
MAX_JOB_ID = 234371 # not a perfect random sample, but sufficient for tests

def _insert_result(attraction_id, res: ClassifyResult, trace: Trace):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
            SET status = 'finished',
                result = %s,
                trace = %s
            WHERE attraction_id = %s
            """,
            (Jsonb(res.model_dump()), Jsonb(trace.model_dump()), attraction_id),
        )

        conn.commit()


def job(row):
    print(f"Starting {row['operator']}")
    operator = OperatorInfo(
        name=row["operator"] or "",
        country=row["country"] or "",
        city=row["city"] or "",
        url=row["operator_website"] or ""
    )

    result, trace = orchestrator.run(operator)
    print(trace.to_string())
    print(json.dumps(result.model_dump(), indent=2))
    _insert_result(row["attraction_id"], result, trace)

    print(f"Finished {row['operator']}")

with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS) as ex:
    inflight = set()

    for _ in range(JOB_LIMIT):
        with connect() as conn, conn.cursor() as cur:
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
            """, (START_ROW,)) # random.randint(0, MAX_JOB_ID) for random sample
            row = cur.fetchone()
            conn.commit()

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

if inflight:
    wait(inflight, return_when=ALL_COMPLETED)

print("Worker done")