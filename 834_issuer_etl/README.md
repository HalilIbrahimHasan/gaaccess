# 834 Issuer ETL Framework

A scalable Python ETL framework for processing **834 XML issuer enrollment files**. The pipeline extracts XML from per-issuer folders, parses enrollee records into normalized DataFrames, validates data quality, computes KPIs, and exports results to Excel, cleaned XML, SQLite, and an interactive Plotly HTML dashboard.

Designed to process **any number of issuers dynamically** — no hardcoded issuer logic.

---

## Requirements

- Python 3.10+
- Dependencies listed in `requirements.txt`

```bash
cd 834_issuer_etl
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Project Structure

```
834_issuer_etl/
├── source_data/                  # Input XML files (local; SFTP-ready design)
│   └── 64357/
│       └── *.xml
├── assets/                       # All generated outputs
│   └── 64357/
│       ├── cleaned_xml/
│       ├── excel/
│       ├── sqlite/
│       ├── dashboards/
│       └── validation_reports/
├── src/
│   ├── config.py                 # Paths, schema, PII policy, validation rules
│   ├── main.py                   # Pipeline orchestrator
│   ├── extract/
│   │   └── xml_reader.py         # DataSource abstraction (local + future SFTP)
│   ├── transform/
│   │   ├── xml_parser.py         # 834 XML → flat enrollee rows
│   │   ├── cleaner.py            # Type conversion, PII masking, metadata
│   │   └── kpi_builder.py        # Issuer-level KPIs and breakdowns
│   ├── validate/
│   │   ├── schema_validator.py   # Required column checks
│   │   └── data_quality_validator.py  # Business rules + profiling
│   ├── load/
│   │   ├── excel_exporter.py
│   │   ├── xml_exporter.py
│   │   └── sqlite_loader.py
│   ├── dashboard/
│   │   └── plotly_dashboard.py
│   └── utils/
│       ├── logger.py
│       └── file_utils.py
├── requirements.txt
└── README.md
```

---

## How to Run

### Process all issuers

```bash
python src/main.py
```

### Process a single issuer

```bash
python src/main.py --issuer 64357
```

The pipeline will:

1. Discover issuer folders under `source_data/`
2. Parse all `*.xml` files per issuer (continues if one file fails)
3. Clean and standardize enrollee records
4. Run schema and data-quality validation
5. Compute issuer KPIs
6. Export outputs to `assets/{issuer_id}/`

---

## Output Locations

| Output | Path |
|--------|------|
| Cleaned enrollees (Excel) | `assets/{issuer_id}/excel/cleaned_enrollees_{issuer_id}.xlsx` |
| KPI summary (Excel) | `assets/{issuer_id}/excel/kpi_summary_{issuer_id}.xlsx` |
| Validation report (Excel) | `assets/{issuer_id}/excel/validation_report_{issuer_id}.xlsx` |
| Cleaned enrollees (XML) | `assets/{issuer_id}/cleaned_xml/cleaned_enrollees_{issuer_id}.xml` |
| SQLite database | `assets/{issuer_id}/sqlite/issuer_{issuer_id}.db` |
| Interactive dashboard | `assets/{issuer_id}/dashboards/issuer_{issuer_id}_dashboard.html` |
| Validation CSV | `assets/{issuer_id}/validation_reports/validation_report_{issuer_id}.csv` |

---

## Adding a New Issuer

1. Create a folder under `source_data/` named with the numeric issuer ID:

   ```
   source_data/68806/
   ```

2. Place 834 XML files in that folder:

   ```
   source_data/68806/from_68806_GA_834_INDV_20260301080000.xml
   ```

3. Run the pipeline:

   ```bash
   python src/main.py
   ```

   Outputs are automatically created under `assets/68806/`.

No code changes are required.

---

## PII Handling

By default, sensitive fields are **masked** in all outputs:

- SSN, phone, email, first/last name, full address

To export raw PII for debugging only, set in `src/config.py`:

```python
EXPORT_PII = True
```

**Never enable this in production exports.**

---

## SFTP-Ready Design

The extract layer uses a `DataSource` abstract interface (`extract/xml_reader.py`):

- **`LocalFileSource`** — reads from `source_data/{issuer_id}/` (current default)
- **`SFTPFileSource`** — stub documented for future implementation

Downstream transform, validate, load, and dashboard stages depend only on parsed data — **not** on how files are retrieved. Adding SFTP later requires only a new `DataSource` implementation and a one-line change in `main.py`.

---

## SQLite Tables

Each issuer database (`issuer_{issuer_id}.db`) contains:

| Table | Description |
|-------|-------------|
| `issuer_enrollees` | Cleaned enrollee records (one row per member) |
| `issuer_kpis` | Scalar KPI metrics with load timestamp |
| `validation_results` | Schema and data-quality check outcomes |

### Example SQL Queries

Open the database:

```bash
sqlite3 assets/64357/sqlite/issuer_64357.db
```

**Count total enrollees:**

```sql
SELECT COUNT(*) AS total_enrollees
FROM issuer_enrollees;
```

**Count subscribers and dependents:**

```sql
SELECT
    subscriber_flag,
    COUNT(*) AS member_count
FROM issuer_enrollees
GROUP BY subscriber_flag;
```

**Premium by rating area:**

```sql
SELECT
    rating_area,
    SUM(total_premium_amt) AS total_premium,
    COUNT(*) AS member_count
FROM issuer_enrollees
GROUP BY rating_area
ORDER BY total_premium DESC;
```

**Duplicate member check:**

```sql
SELECT
    issuer_id,
    exchg_indiv_identifier,
    COUNT(*) AS occurrence_count
FROM issuer_enrollees
GROUP BY issuer_id, exchg_indiv_identifier
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;
```

**Unique policies by file:**

```sql
SELECT
    source_file,
    COUNT(DISTINCT exchg_assigned_policy_id) AS unique_policies,
    COUNT(*) AS total_rows
FROM issuer_enrollees
GROUP BY source_file
ORDER BY source_file;
```

**Monthly premium trend:**

```sql
SELECT
    SUBSTR(benefit_effective_begin_date, 1, 7) AS effective_month,
    SUM(total_premium_amt) AS monthly_premium,
    COUNT(*) AS member_count
FROM issuer_enrollees
WHERE benefit_effective_begin_date IS NOT NULL
  AND benefit_effective_begin_date != ''
GROUP BY effective_month
ORDER BY effective_month;
```

---

## Validation Checks

Per-issuer validation includes:

- Required columns exist
- Required ID fields not null (`issuer_id`, `exchg_indiv_identifier`, `exchg_assigned_policy_id`)
- Duplicate checks (within file and across files)
- QTYt consistency vs enrollee counts per enrollment segment
- `subscriber_flag` values are `Y` or `N`
- Insurance type codes tracked dynamically from data
- Premium fields numeric; `total_premium_amt` non-negative
- `benefit_effective_begin_date` not null
- `source_exchg_id` presence check
- Missingness percentage by column
- Row counts, unique policy count, and unique member count by file

Results appear in Excel validation reports, CSV exports, SQLite `validation_results`, and the dashboard validation summary chart.

---

## KPIs Generated

- Total files, enrollments, enrollees, subscribers, dependents
- Unique policies, members, households
- Duplicate member and policy-member counts
- Total/average premium and individual responsibility amounts
- Breakdowns by subscriber flag, relationship, event type/reason, maintenance type, insurance type, rating area, effective month
- Premium by rating area and effective month
- File count trend and enrollee count by file

---

## Dashboard

Open the generated HTML file in any browser:

```
assets/64357/dashboards/issuer_64357_dashboard.html
```

Includes:

- KPI summary table
- Enrollees by source file
- Subscribers vs dependents (pie)
- Premium by rating area
- Members by effective month
- Validation issue summary
- Missingness by column (top 15)
- Duplicate count indicators

---

## Error Handling

- Malformed XML files are logged and **skipped**; remaining files continue processing
- Issuer-level failures are logged with stack traces; other issuers still process
- All stages emit structured log messages to stdout

---

## License

Internal use — adjust as needed for your organization.
