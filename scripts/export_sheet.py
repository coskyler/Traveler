from dotenv import load_dotenv

load_dotenv()

import csv
from datetime import datetime
from pathlib import Path
from crawler.db import pool
from psycopg.rows import dict_row

query = """
    SELECT
        r.*,
        j.*,
        p.profile_type,
        p.role,
        p.profile_name,
        p.email   AS profile_email,
        p.phone   AS profile_phone,
        p.whatsapp
    FROM results r
    JOIN jobs j
        ON j.attraction_id = r.attraction_id
    LEFT JOIN profiles p
        ON p.attraction_id = r.attraction_id
    ORDER BY r.attraction_id
"""

with open(
    Path("outputs") / f"{datetime.now():%Y%m%d_%H%M%S}_Before.csv",
    "w",
    newline="",
    encoding="utf-8-sig",
) as unmodified_file, open(
    Path("outputs") / f"{datetime.now():%Y%m%d_%H%M%S}_After.csv",
    "w",
    newline="",
    encoding="utf-8-sig",
) as modified_file, open(
    Path("outputs") / f"{datetime.now():%Y%m%d_%H%M%S}_Profiles.csv",
    "w",
    newline="",
    encoding="utf-8-sig",
) as profile_file:


    unmodified_writer = csv.writer(unmodified_file)
    modified_writer = csv.writer(modified_file)
    profile_writer = csv.writer(profile_file)

    rows = []

    with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query)

        rows = cur.fetchall()

    unmodified_writer.writerow(
        [
            "Attraction ID",
            "Destination ID",
            "Trip Operator URL",
            "Operator Name",
            "Country",
            "State",
            "City",
            "Email",
            "Phone",
            "Operator Website",
            "Bookable",
            "Arrival Category",
            "Arrival Subcategory",
            "Average Rating",
            "Review Count",
            "Number of Products",
        ]
    )

    modified_writer.writerow(
        [
            "Attraction ID",
            "Destination ID",
            "Trip Operator URL",
            "Operator Name",
            "Country",
            "State",
            "City",
            "Email",
            "Phone",
            "Final URL",
            "Bookable",
            "Operator Type",
            "Business Type",
            "Experience Type",
            "Commercial Operator",
            "Average Rating",
            "Review Count",
            "Number of Products",
            "Booking Method",
            "Operating Scope",
            "Message",
            "Searched",
            "Input Tokens",
            "Cached Input Tokens",
            "Output Tokens",
        ]
    )

    profile_writer.writerow(
        [
            "Attraction ID",
            "Operator Name",
            "Final URL",
            "Profile Type",
            "Role",
            "Profile Name",
            "Email",
            "Phone",
            "WhatsApp",
        ]
    )

    prev_attraction = None
    for r in rows:
        if prev_attraction != r["attraction_id"]:  # skip duplicates
            unmodified_writer.writerow(
                [
                    r["attraction_id"],
                    r["destination_id"],
                    r["trip_operator_url"],
                    r["operator"],
                    r["country"],
                    r["state"],
                    r["city"],
                    r["email"],
                    r["phone"],
                    r["operator_website"],
                    r["bookable"],
                    r["arival_category"],
                    r["arival_sub_category"],
                    r["avg_rating"],
                    r["review_count"],
                    r["number_of_products"],
                ]
            )

            modified_writer.writerow(
                [
                    r["attraction_id"],
                    r["destination_id"],
                    r["trip_operator_url"],
                    r["operator"],
                    r["country"],
                    r["state"],
                    r["city"],
                    r["email"],
                    r["phone"],
                    r["final_url"],
                    r["bookable"],
                    r["operator_type"],
                    r["business_type"],
                    r["experience_type"],
                    r["is_commercial"],
                    r["avg_rating"],
                    r["review_count"],
                    r["number_of_products"],
                    r["booking_method"],
                    r["operating_scope"],
                    r["message"],
                    r["searched"],
                    r["input_tokens"],
                    r["cached_input_tokens"],
                    r["output_tokens"],
                ]
            )

        profile_writer.writerow(
            [
                r["attraction_id"],
                r["operator"],
                r["final_url"],
                r["profile_type"],
                r["role"],
                r["profile_name"],
                r["profile_email"],
                r["profile_phone"],
                r["whatsapp"],
            ]
        )

        prev_attraction = r["attraction_id"]

print("DB exported")
