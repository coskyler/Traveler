# csv format
# Attraction ID,Destination ID,TRIP Operator URL,Operator,
# Country,State,City,Email,Phone,Operator Website,
# Bookable,Arival Category,Arival Sub-Category,
# Avg. Rating,Review Count,Number of Products

from pathlib import Path
from crawler.db import pool
import csv
from io import StringIO

CSV_PATH = Path(__file__).parents[2] / "dataset.csv"

COPY_SQL = """
COPY jobs (
    attraction_id, destination_id, trip_operator_url, operator,
    country, state, city, email, phone, operator_website,
    bookable, arival_category, arival_sub_category,
    avg_rating, review_count, number_of_products
)
FROM STDIN WITH (FORMAT csv, HEADER true);
"""

with pool.connection() as conn, conn.cursor() as cur:
    with CSV_PATH.open(encoding="utf-8") as f, cur.copy(COPY_SQL) as copy:
        reader = csv.reader(f)
        buf = StringIO()
        writer = csv.writer(buf)

        for cols in reader:
            if len(cols) < 11:
                print(cols)
            cols[11] = ""  # arrival_category
            cols[12] = ""  # arrival_sub_category
            writer.writerow(cols)
            copy.write(buf.getvalue())
            buf.seek(0)
            buf.truncate(0)
    conn.commit()

pool.close()


print("CSV imported.")
