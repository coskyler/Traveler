CREATE TABLE jobs_stage (
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

CREATE TABLE jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- from the dataset
    attraction_id        BIGINT UNIQUE NOT NULL,
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
    number_of_products   INTEGER,

    -- queue state
    status TEXT NOT NULL DEFAULT 'queued'
);

CREATE INDEX jobs_queued_idx ON jobs(attraction_id) WHERE status='queued';
CREATE INDEX url_idx ON jobs(operator_website) WHERE status!='queued';

CREATE TABLE results (
    attraction_id BIGINT PRIMARY KEY REFERENCES jobs(attraction_id),

    final_url            TEXT,
    operator_type        TEXT,
    business_type        TEXT,
    experience_type      TEXT,
    is_commercial        BOOLEAN,
    booking_method       TEXT,
    operating_scope      TEXT,
    message              TEXT,
    input_tokens         INTEGER,
    cached_input_tokens  INTEGER,
    output_tokens        INTEGER,
    searched             BOOLEAN
);

CREATE TABLE profiles (
    attraction_id BIGINT NOT NULL REFERENCES jobs(attraction_id),

    profile_type    TEXT,
    role            TEXT,
    profile_name    TEXT,
    email           TEXT,
    phone           TEXT,
    whatsapp        TEXT
);