CREATE TABLE jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    status TEXT NOT NULL DEFAULT 'queued', -- queued | running | succeeded | failed | discarded
    attempts INTEGER NOT NULL DEFAULT 0,

    attraction_id        BIGINT,
    destination_id       BIGINT,
    trip_operator_url    TEXT,
    operator             TEXT,
    country              TEXT,
    state                TEXT,
    city                 TEXT,
    email                TEXT,
    phone                TEXT,
    operator_website     TEXT,
    bookable             TEXT,
    arival_category      TEXT,
    arival_sub_category  TEXT,
    avg_rating           NUMERIC(4,2),
    review_count         INTEGER,
    number_of_products   INTEGER
);

CREATE INDEX jobs_queued_idx ON jobs(id) WHERE status='queued';