-- =============================================================================
-- REPRODUCE SWATHI'S MATCH NOT FOUND ANALYSIS
-- =============================================================================
-- READ ONLY. Temp tables only. No RCNI. No permanent writes.
--
-- REQUIRED INPUT:
--   Swathi's exact Match Not Found source containing the 47,945 Enrollee IDs.
--
-- The exact source is not present in this project. Before execution, import or
-- expose that source in Azure SQL as a table/view, then set the three source
-- variables below. Do not point them at the 109,776 NO_INBOUND export.
--
-- Premium field:
--   net_premium_amt is the field present in the existing Swathi enrollment
--   workbook. This script verifies that dbo.Enrollments_TEST actually contains
--   the column and stops if it does not.
-- =============================================================================

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @SourceSchema SYSNAME = NULL;          -- Example: N'dbo'
DECLARE @SourceObject SYSNAME = NULL;          -- Exact imported Match Not Found table/view
DECLARE @SourceEnrolleeIDColumn SYSNAME = NULL;-- Exact Enrollee ID column in that source

DECLARE @ExpectedTotal BIGINT = 47945;
DECLARE @ExpectedEnrolled BIGINT = 29999;
DECLARE @ExpectedZeroDollarTotal BIGINT = 7952;
DECLARE @ExpectedEnrolledZero BIGINT = 6806;
DECLARE @ExpectedEnrolledNonZero BIGINT = 23193;


-- =============================================================================
-- REQUIRED INPUT GUARDS
-- =============================================================================
IF @SourceSchema IS NULL
   OR @SourceObject IS NULL
   OR @SourceEnrolleeIDColumn IS NULL
BEGIN
    THROW 50001,
        'STOP: Supply Swathi''s exact 47,945-row Match Not Found source in @SourceSchema, @SourceObject, and @SourceEnrolleeIDColumn.',
        1;
END;

DECLARE @QualifiedSource NVARCHAR(517) =
    QUOTENAME(@SourceSchema) + N'.' + QUOTENAME(@SourceObject);

IF OBJECT_ID(@QualifiedSource) IS NULL
BEGIN
    THROW 50002,
        'STOP: The configured Swathi Match Not Found source table/view does not exist.',
        1;
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(@QualifiedSource)
      AND c.name = @SourceEnrolleeIDColumn
)
BEGIN
    THROW 50003,
        'STOP: The configured Enrollee ID column does not exist in the Swathi source.',
        1;
END;

IF COL_LENGTH('dbo.Enrollments_TEST', 'enrollee_id') IS NULL
   OR COL_LENGTH('dbo.Enrollments_TEST', 'enrollment_status_description') IS NULL
BEGIN
    THROW 50004,
        'STOP: Required dbo.Enrollments_TEST enrollee/status columns are unavailable.',
        1;
END;

IF COL_LENGTH('dbo.Enrollments_TEST', 'net_premium_amt') IS NULL
BEGIN
    THROW 50005,
        'STOP: dbo.Enrollments_TEST.net_premium_amt was not found. Confirm Swathi''s actual Net Premium Amount column before proceeding.',
        1;
END;


-- =============================================================================
-- STAGE SWATHI'S EXACT MATCH NOT FOUND ENROLLEE IDS
-- =============================================================================
IF OBJECT_ID('tempdb..#swathi_match_not_found_ids') IS NOT NULL
    DROP TABLE #swathi_match_not_found_ids;

CREATE TABLE #swathi_match_not_found_ids (
    Enrollee_ID VARCHAR(100) NOT NULL
);

DECLARE @LoadSourceSQL NVARCHAR(MAX) =
    N'
    INSERT INTO #swathi_match_not_found_ids (Enrollee_ID)
    SELECT DISTINCT
        LTRIM(RTRIM(CAST(' + QUOTENAME(@SourceEnrolleeIDColumn) + N' AS VARCHAR(100))))
    FROM ' + @QualifiedSource + N'
    WHERE NULLIF(
        LTRIM(RTRIM(CAST(' + QUOTENAME(@SourceEnrolleeIDColumn) + N' AS VARCHAR(100)))),
        ''''
    ) IS NOT NULL;';

EXEC sys.sp_executesql @LoadSourceSQL;

CREATE UNIQUE CLUSTERED INDEX CX_swathi_match_not_found_ids
    ON #swathi_match_not_found_ids (Enrollee_ID);


-- =============================================================================
-- QUERY dbo.Enrollments_TEST USING ONLY SWATHI'S ENROLLEE IDS
-- =============================================================================
IF OBJECT_ID('tempdb..#swathi_enrollment_rows') IS NOT NULL
    DROP TABLE #swathi_enrollment_rows;

CREATE TABLE #swathi_enrollment_rows (
    Enrollee_ID VARCHAR(100) NOT NULL,
    Enrollment_Status NVARCHAR(255) NULL,
    Net_Premium_Amount DECIMAL(38, 10) NULL
);

DECLARE @LoadEnrollmentSQL NVARCHAR(MAX) =
    N'
    INSERT INTO #swathi_enrollment_rows (
        Enrollee_ID,
        Enrollment_Status,
        Net_Premium_Amount
    )
    SELECT DISTINCT
        LTRIM(RTRIM(CAST(e.enrollee_id AS VARCHAR(100)))) AS Enrollee_ID,
        LTRIM(RTRIM(CAST(e.enrollment_status_description AS NVARCHAR(255))))
            AS Enrollment_Status,
        TRY_CONVERT(DECIMAL(38, 10), e.net_premium_amt)
            AS Net_Premium_Amount
    FROM dbo.Enrollments_TEST AS e
    INNER JOIN #swathi_match_not_found_ids AS s
        ON s.Enrollee_ID = LTRIM(RTRIM(CAST(e.enrollee_id AS VARCHAR(100))));';

EXEC sys.sp_executesql @LoadEnrollmentSQL;

CREATE CLUSTERED INDEX CX_swathi_enrollment_rows
    ON #swathi_enrollment_rows (Enrollee_ID);

CREATE NONCLUSTERED INDEX IX_swathi_status_premium
    ON #swathi_enrollment_rows (Enrollment_Status, Net_Premium_Amount)
    INCLUDE (Enrollee_ID);


-- =============================================================================
-- ACTUAL METRICS
-- =============================================================================
DECLARE @ActualTotal BIGINT = (
    SELECT COUNT(DISTINCT Enrollee_ID)
    FROM #swathi_enrollment_rows
);

DECLARE @ActualEnrolled BIGINT = (
    SELECT COUNT(DISTINCT Enrollee_ID)
    FROM #swathi_enrollment_rows
    WHERE Enrollment_Status = 'Enrolled'
);

DECLARE @ActualZeroDollarTotal BIGINT = (
    SELECT COUNT(DISTINCT Enrollee_ID)
    FROM #swathi_enrollment_rows
    WHERE Net_Premium_Amount = 0
);

DECLARE @ActualEnrolledZero BIGINT = (
    SELECT COUNT(DISTINCT Enrollee_ID)
    FROM #swathi_enrollment_rows
    WHERE Enrollment_Status = 'Enrolled'
      AND Net_Premium_Amount = 0
);

DECLARE @ActualEnrolledNonZero BIGINT =
    @ActualEnrolled - @ActualEnrolledZero;


-- =============================================================================
-- RESULT 1 — Enrollment Status distribution
-- =============================================================================
SELECT
    Enrollment_Status,
    COUNT(DISTINCT Enrollee_ID) AS Enrollee_Count
FROM #swathi_enrollment_rows
GROUP BY Enrollment_Status
ORDER BY Enrollment_Status;


-- =============================================================================
-- RESULT 2 — Zero-dollar Enrollment Status distribution
-- =============================================================================
SELECT
    Enrollment_Status AS [Zero-dollar Enrollment Status],
    COUNT(DISTINCT Enrollee_ID) AS Enrollee_Count
FROM #swathi_enrollment_rows
WHERE Net_Premium_Amount = 0
GROUP BY Enrollment_Status
ORDER BY Enrollment_Status;


-- =============================================================================
-- RESULT 3 — Enrolled premium split
-- =============================================================================
SELECT
    @ActualEnrolled AS Enrolled_Total,
    @ActualEnrolledZero AS Enrolled_Zero_Dollar,
    @ActualEnrolledNonZero AS Enrolled_Non_Zero;


-- =============================================================================
-- RESULT 4 — Validation
-- =============================================================================
SELECT
    v.Metric,
    v.Actual_Count,
    v.Expected_Count,
    v.Actual_Count - v.Expected_Count AS Difference,
    CASE
        WHEN v.Actual_Count = v.Expected_Count THEN 'PASS'
        ELSE 'FAIL'
    END AS Control_Status
FROM (
    VALUES
        (
            CAST('TOTAL_MATCH_NOT_FOUND' AS VARCHAR(40)),
            @ActualTotal,
            @ExpectedTotal
        ),
        (
            CAST('ENROLLED' AS VARCHAR(40)),
            @ActualEnrolled,
            @ExpectedEnrolled
        ),
        (
            CAST('ZERO_DOLLAR_TOTAL' AS VARCHAR(40)),
            @ActualZeroDollarTotal,
            @ExpectedZeroDollarTotal
        ),
        (
            CAST('ENROLLED_ZERO_DOLLAR' AS VARCHAR(40)),
            @ActualEnrolledZero,
            @ExpectedEnrolledZero
        ),
        (
            CAST('ENROLLED_NON_ZERO' AS VARCHAR(40)),
            @ActualEnrolledNonZero,
            @ExpectedEnrolledNonZero
        )
) AS v (Metric, Actual_Count, Expected_Count)
ORDER BY
    CASE v.Metric
        WHEN 'TOTAL_MATCH_NOT_FOUND' THEN 1
        WHEN 'ENROLLED' THEN 2
        WHEN 'ZERO_DOLLAR_TOTAL' THEN 3
        WHEN 'ENROLLED_ZERO_DOLLAR' THEN 4
        WHEN 'ENROLLED_NON_ZERO' THEN 5
        ELSE 99
    END;
