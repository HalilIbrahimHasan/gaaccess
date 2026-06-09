-- 834 Issuer ETL staging schema

CREATE TABLE IF NOT EXISTS raw_file_inventory (
    file_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer          TEXT NOT NULL,
    year            TEXT NOT NULL,
    month           TEXT NOT NULL,
    file_name       TEXT NOT NULL,
    file_path       TEXT NOT NULL,
    file_hash       TEXT,
    file_size       INTEGER,
    file_type       TEXT,
    source_type     TEXT DEFAULT 'local',
    processed_status TEXT DEFAULT 'pending',
    error_message   TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_file_hash ON raw_file_inventory(file_hash);

CREATE TABLE IF NOT EXISTS stg_834_records (
    record_id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id                         INTEGER REFERENCES raw_file_inventory(file_id),
    issuer                          TEXT NOT NULL,
    year                            TEXT NOT NULL,
    month                           TEXT NOT NULL,
    policy_id                       TEXT,
    member_id                       TEXT,
    subscriber_id                   TEXT,
    member_first_name               TEXT,
    member_last_name                TEXT,
    relationship                    TEXT,
    subscriber_flag                 TEXT,
    action_code                     TEXT,
    action_code_description         TEXT,
    maintenance_type_code           TEXT,
    additional_maint_reason_code    TEXT,
    coverage_status                 TEXT,
    benefit_effective_date          TEXT,
    benefit_end_date                TEXT,
    member_maint_effective_date     TEXT,
    total_premium_amount            REAL,
    individual_responsibility_amount REAL,
    aptc_amount                     REAL,
    user_fee_amount                 REAL,
    insurance_type_code             TEXT,
    health_coverage_policy_no       TEXT,
    household_or_employee_case_id   TEXT,
    rating_area                     TEXT,
    source_exchg_id                 TEXT,
    premium_validation_status       TEXT,
    expected_user_fee               REAL,
    days_between_effective_and_cancel INTEGER,
    months_between_effective_and_cancel INTEGER,
    cancellation_window_status      TEXT,
    refund_eligibility              TEXT,
    raw_xml_path                    TEXT,
    raw_payload                     TEXT,
    created_at                      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_stg_issuer ON stg_834_records(issuer);
CREATE INDEX IF NOT EXISTS idx_stg_period ON stg_834_records(issuer, year, month);
CREATE INDEX IF NOT EXISTS idx_stg_policy ON stg_834_records(policy_id, member_id);
CREATE INDEX IF NOT EXISTS idx_stg_action ON stg_834_records(action_code_description);

CREATE TABLE IF NOT EXISTS parse_errors (
    error_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer      TEXT,
    year        TEXT,
    month       TEXT,
    file_name   TEXT,
    file_path   TEXT,
    error_message TEXT,
    created_at  TEXT DEFAULT (datetime('now'))
);
