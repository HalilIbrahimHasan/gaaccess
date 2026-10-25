-- =============================================================================
-- RCNI SCHEMA DISCOVERY ONLY — dbo.rcni_raw
-- =============================================================================
-- Purpose: Inspect physical columns/types/sample data BEFORE any reconciliation
--          join logic is written.
--
-- Safety: READ ONLY. No INSERT/UPDATE/DELETE/TRUNCATE/DROP/ALTER.
--         No permanent indexes. No joins to Swathi / inbound_automation.
--
-- Run this in SSMS / Azure Data Studio against the Azure SQL database that
-- hosts dbo.rcni_raw. Paste ALL result sets back for mapping approval.
-- =============================================================================

SET NOCOUNT ON;

-- -----------------------------------------------------------------------------
-- 1) Does the table exist?
-- -----------------------------------------------------------------------------
SELECT
    OBJECT_SCHEMA_NAME(object_id) AS table_schema,
    name AS table_name,
    create_date,
    modify_date
FROM sys.tables
WHERE name = N'rcni_raw'
  AND schema_id = SCHEMA_ID(N'dbo');

-- -----------------------------------------------------------------------------
-- 2) INFORMATION_SCHEMA.COLUMNS
-- -----------------------------------------------------------------------------
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    ORDINAL_POSITION,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = N'dbo'
  AND TABLE_NAME = N'rcni_raw'
ORDER BY ORDINAL_POSITION;

-- -----------------------------------------------------------------------------
-- 3) sys.columns + types (exact SQL types)
-- -----------------------------------------------------------------------------
SELECT
    c.column_id,
    c.name AS column_name,
    t.name AS type_name,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity,
    c.is_computed
FROM sys.columns AS c
INNER JOIN sys.types AS t
    ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID(N'dbo.rcni_raw')
ORDER BY c.column_id;

-- -----------------------------------------------------------------------------
-- 4) Existing indexes on dbo.rcni_raw (for performance planning only)
-- -----------------------------------------------------------------------------
SELECT
    i.name AS index_name,
    i.type_desc,
    i.is_unique,
    i.is_primary_key,
    STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns
FROM sys.indexes AS i
INNER JOIN sys.index_columns AS ic
    ON i.object_id = ic.object_id
   AND i.index_id = ic.index_id
INNER JOIN sys.columns AS c
    ON ic.object_id = c.object_id
   AND ic.column_id = c.column_id
WHERE i.object_id = OBJECT_ID(N'dbo.rcni_raw')
  AND i.type > 0
GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key
ORDER BY i.name;

-- -----------------------------------------------------------------------------
-- 5) Approximate row count (metadata — inexpensive)
-- -----------------------------------------------------------------------------
SELECT
    OBJECT_SCHEMA_NAME(p.object_id) AS table_schema,
    OBJECT_NAME(p.object_id) AS table_name,
    SUM(p.rows) AS approx_row_count_metadata
FROM sys.partitions AS p
WHERE p.object_id = OBJECT_ID(N'dbo.rcni_raw')
  AND p.index_id IN (0, 1)
GROUP BY p.object_id;

-- Optional exact count — can be expensive on ~48M rows; leave commented.
-- SELECT COUNT_BIG(*) AS exact_row_count FROM dbo.rcni_raw;

-- -----------------------------------------------------------------------------
-- 6) TOP 20 sample rows (all columns as stored)
-- -----------------------------------------------------------------------------
SELECT TOP (20) *
FROM dbo.rcni_raw;

-- -----------------------------------------------------------------------------
-- 7) Column-name candidates for the 19 known RCNI business fields
--     (pattern assist only — final mapping requires human confirmation)
-- -----------------------------------------------------------------------------
SELECT
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    CASE
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%exchangeassignedpolicy%'
          OR REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%exchgassignedpolicy%'
            THEN 'CANDIDATE: Exchange Assigned Policy ID'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%exchangeassignedmember%'
          OR REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%exchgassignedmember%'
            THEN 'CANDIDATE: Exchange Assigned Member ID'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%issuerassignedmember%'
            THEN 'CANDIDATE: Issuer Assigned Member ID'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%exchangeassignedsubscriber%'
          OR REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%exchgassignedsubscriber%'
            THEN 'CANDIDATE: Exchange Assigned Subscriber ID'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%issuerassignedsubscriber%'
            THEN 'CANDIDATE: Issuer Assigned Subscriber ID'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%discrepancyreasoncode%'
          OR REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%reasoncode%'
            THEN 'CANDIDATE: Discrepancy Reason Code'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%discrepancyreasontext%'
          OR REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%reasontext%'
          OR REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%reasondescription%'
            THEN 'CANDIDATE: Discrepancy Reason Text'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%dateofdiscrepancy%'
          OR REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%discrepancydate%'
            THEN 'CANDIDATE: Date of Discrepancy'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%enrollmentstatus%'
            THEN 'CANDIDATE: Enrollment Status'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%reconfilename%'
          OR REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%filename%'
            THEN 'CANDIDATE: Recon File Name'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             IN ('issuer', 'hiosisuerid', 'hiosissuerid', 'issuerid')
          OR REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%issuer%'
            THEN 'CANDIDATE: Issuer (review carefully)'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%coverageyear%'
          OR REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%planyear%'
          OR REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%benefityear%'
            THEN 'CANDIDATE: Plan/Coverage Year'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%planid%'
            THEN 'CANDIDATE: Plan ID'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%hixvalue%'
            THEN 'CANDIDATE: HIX Value'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%issuervalue%'
            THEN 'CANDIDATE: Issuer Value'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%autofixed%'
            THEN 'CANDIDATE: Autofixed by HIX'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%assignee%'
            THEN 'CANDIDATE: Assignee'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%lastname%'
            THEN 'CANDIDATE: name field (member/subscriber last)'
        WHEN REPLACE(REPLACE(LOWER(c.COLUMN_NAME), ' ', ''), '_', '')
             LIKE '%firstname%'
            THEN 'CANDIDATE: name field (member/subscriber first)'
        ELSE 'UNMAPPED — review manually'
    END AS business_field_candidate
FROM INFORMATION_SCHEMA.COLUMNS AS c
WHERE c.TABLE_SCHEMA = N'dbo'
  AND c.TABLE_NAME = N'rcni_raw'
ORDER BY c.ORDINAL_POSITION;

-- -----------------------------------------------------------------------------
-- 8) OPTIONAL profile queries — EDIT column names AFTER reviewing result sets
--     1–3 and 7. Keep commented until physical names are confirmed.
-- -----------------------------------------------------------------------------
/*
-- Distinct issuer values (replace <issuer_column>)
SELECT TOP (100)
    <issuer_column> AS issuer_value,
    COUNT_BIG(*) AS row_count
FROM dbo.rcni_raw
GROUP BY <issuer_column>
ORDER BY row_count DESC;

-- Coverage / plan / processing year fields (replace placeholders)
SELECT TOP (100)
    <year_column> AS year_value,
    COUNT_BIG(*) AS row_count
FROM dbo.rcni_raw
GROUP BY <year_column>
ORDER BY year_value;

-- Enrollment status values
SELECT
    <enrollment_status_column> AS enrollment_status,
    COUNT_BIG(*) AS row_count
FROM dbo.rcni_raw
GROUP BY <enrollment_status_column>
ORDER BY row_count DESC;

-- Discrepancy reason code / text
SELECT TOP (100)
    <discrepancy_reason_code_column> AS reason_code,
    <discrepancy_reason_text_column> AS reason_text,
    COUNT_BIG(*) AS row_count
FROM dbo.rcni_raw
GROUP BY <discrepancy_reason_code_column>, <discrepancy_reason_text_column>
ORDER BY row_count DESC;
*/
