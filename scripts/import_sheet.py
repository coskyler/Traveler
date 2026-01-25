from dotenv import load_dotenv
load_dotenv()

import csv
from pathlib import Path
from io import StringIO
from crawler.db import pool

CSV_PATH = Path(__file__).parents[1] / "dataset.csv"

COPY_SQL = """
COPY jobs_stage (
    attraction_id, destination_id, trip_operator_url, operator,
    country, state, city, email, phone, operator_website,
    bookable, arival_category, arival_sub_category,
    avg_rating, review_count, number_of_products
)
FROM STDIN WITH (FORMAT csv);
"""

with pool.connection() as conn, conn.cursor() as cur:
    with CSV_PATH.open(encoding="utf-8") as f, cur.copy(COPY_SQL) as copy:
        reader = csv.reader(f)
        buf = StringIO()
        writer = csv.writer(buf)

        for cols in reader:
            if len(cols) != 16:
                print("Invalid input")
            writer.writerow(cols)
            copy.write(buf.getvalue())
            buf.seek(0)
            buf.truncate(0)
    conn.commit()

    cur.execute(
        """
        INSERT INTO jobs (
            attraction_id,
            destination_id,
            trip_operator_url,
            operator,
            country,
            state,
            city,
            email,
            phone,
            operator_website,
            bookable,
            arival_category,
            arival_sub_category,
            avg_rating,
            review_count,
            number_of_products
        )
        SELECT
            attraction_id,
            destination_id,
            trip_operator_url,
            operator,
            country,
            state,
            city,
            email,
            phone,
            operator_website,
            bookable,
            arival_category,
            arival_sub_category,
            avg_rating,
            review_count,
            number_of_products
        FROM jobs_stage
        ON CONFLICT (attraction_id) DO NOTHING
        """
    )

    conn.commit()
pool.close()

print("CSV imported.")