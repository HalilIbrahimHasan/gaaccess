# 834 Issuer ETL Framework

A scalable Python ETL framework for processing **834 XML issuer enrollment files** at **issuer / year / month** partition granularity. The pipeline extracts XML from partitioned folders, parses enrollee records into normalized DataFrames, validates data quality, computes KPIs, and exports results to Excel, cleaned XML, SQLite, JSON validation reports, and interactive Plotly HTML dashboards.

Designed to process **any number of issuers and time partitions dynamically** — no hardcoded issuer logic.

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

## Input Folder Structure

XML files must be organized by issuer, year, and month:

```
source_data/
└── 64357/
    └── 2026/
        └── 02/
            ├── from_64357_GA_834_INDV_20260204071545.xml
            ├── from_64357_GA_834_INDV_20260211071730.xml
            ├── from_64357_GA_834_INDV_20260218064929.xml
            └── from_64357_GA_834_INDV_20260225064930.xml
```

Additional issuers follow the same pattern:

```
source_data/
├── 64357/
│   └── 2026/
│       └── 02/
├── 68806/
│   └── 2026/
│       ├── 01/
│       └── 02/
└── 49046/
    └── 2025/
        └── 12/
```

---

## Project Structure

```
834_issuer_etl/
├── source_data/                  # Input XML (local; SFTP-ready design)
│   └── {issuer_id}/{year}/{month}/*.xml
├── assets/                       # Generated outputs
│   └── {issuer_id}/
│       ├── {year}/{month}/       # Monthly partition outputs
│       │   ├── cleaned_xml/
│       │   ├── excel/
│       │   ├── sqlite/
│       │   ├── dashboards/
│       │   └── validation_reports/
│       └── rollups/              # Issuer-level combined outputs
│           ├── excel/
│           ├── sqlite/
│           └── dashboards/
├── src/
│   ├── config.py
│   ├── main.py
│   ├── extract/xml_reader.py     # DataSource abstraction
│   ├── transform/
│   │   ├── xml_parser.py
│   │   ├── cleaner.py
│   │   └── kpi_builder.py
│   ├── validate/
│   │   ├── schema_validator.py
│   │   └── data_quality_validator.py
│   ├── load/
│   │   ├── excel_exporter.py
│   │   ├── xml_exporter.py
│   │   └── sqlite_loader.py
│   ├── dashboard/plotly_dashboard.py
│   └── utils/
│       ├── partition.py          # Partition discovery
│       ├── file_utils.py
│       └── logger.py
├── requirements.txt
└── README.md
```

---

## How to Run

### Process all issuers, years, and months

```bash
python src/main.py
```

### Process all partitions for one issuer

```bash
python src/main.py --issuer 64357
```

### Process all months in a year for one issuer

```bash
python src/main.py --issuer 64357 --year 2026
```

### Process a single month

```bash
python src/main.py --issuer 64357 --year 2026 --month 02
```

After monthly processing, the pipeline automatically builds **issuer rollup** outputs combining all processed months for each issuer.

---

## Adding a New Issuer / Year / Month

1. Create the folder path:

   ```
   source_data/68806/2026/03/
   ```

2. Place 834 XML files in that month folder.

3. Run the pipeline:

   ```bash
   python src/main.py
   ```

   Monthly outputs appear under `assets/68806/2026/03/`.
   Rollup outputs appear under `assets/68806/rollups/` after all months for that issuer are processed in the same run.

No code changes are required.

---

## Monthly Output Locations

For partition `64357 / 2026 / 02`:

| Output | Path |
|--------|------|
| Cleaned enrollees (Excel) | `assets/64357/2026/02/excel/cleaned_enrollees_64357_2026_02.xlsx` |
| KPI summary (Excel) | `assets/64357/2026/02/excel/kpi_summary_64357_2026_02.xlsx` |
| Validation report (Excel) | `assets/64357/2026/02/excel/validation_report_64357_2026_02.xlsx` |
| Validation report (JSON) | `assets/64357/2026/02/validation_reports/validation_report_64357_2026_02.json` |
| Cleaned enrollees (XML) | `assets/64357/2026/02/cleaned_xml/cleaned_enrollees_64357_2026_02.xml` |
| SQLite database | `assets/64357/2026/02/sqlite/issuer_64357_2026_02.db` |
| Dashboard | `assets/64357/2026/02/dashboards/issuer_64357_2026_02_dashboard.html` |

---

## Rollup Output Locations

For issuer `64357` (all processed months combined):

| Output | Path |
|--------|------|
| Cleaned enrollees (Excel) | `assets/64357/rollups/excel/cleaned_enrollees_64357_all_periods.xlsx` |
| KPI summary (Excel) | `assets/64357/rollups/excel/kpi_summary_64357_all_periods.xlsx` |
| SQLite database | `assets/64357/rollups/sqlite/issuer_64357_all_periods.db` |
| Dashboard | `assets/64357/rollups/dashboards/issuer_64357_all_periods_dashboard.html` |

---

## Partition Columns

Every cleaned enrollee row includes:

| Column | Description |
|--------|-------------|
| `issuer_id` | From folder name |
| `source_year` | From year folder (e.g. `2026`) |
| `source_month` | From month folder (e.g. `02`) |
| `source_period` | `YYYY-MM` format (e.g. `2026-02`) |
| `source_file` | Original XML filename |
| `load_timestamp` | UTC timestamp when row was processed |

---

## PII Handling

By default, sensitive fields are **masked** in all outputs (SSN, phone, email, names, address).

To export raw PII for debugging only:

```python
# src/config.py
EXPORT_PII = True
```

Never enable in production.

---

## SFTP-Ready Design

The extract layer uses a `DataSource` abstract interface (`extract/xml_reader.py`):

- **`LocalFileSource`** — reads from `source_data/{issuer}/{year}/{month}/` (current default)
- **`SFTPFileSource`** — documented extension point for future remote ingestion

Downstream stages depend only on `Partition` objects and parsed data — not on how files are retrieved.

---

## SQLite Tables

### Monthly partition DB (`issuer_{issuer}_{year}_{month}.db`)

| Table | Description |
|-------|-------------|
| `issuer_enrollees` | Cleaned enrollee records for that month |
| `issuer_kpis` | Scalar KPI metrics |
| `validation_results` | Schema and data-quality check outcomes |

### Rollup DB (`issuer_{issuer}_all_periods.db`)

| Table | Description |
|-------|-------------|
| `issuer_enrollees_all_periods` | All months combined |
| `issuer_kpis_all_periods` | Rollup KPI metrics |
| `validation_results_all_periods` | Rollup validation outcomes |

### Example SQL Queries

```bash
sqlite3 assets/64357/2026/02/sqlite/issuer_64357_2026_02.db
```

**Count total enrollees (monthly):**

```sql
SELECT COUNT(*) AS total_enrollees
FROM issuer_enrollees;
```

**Count subscribers and dependents:**

```sql
SELECT subscriber_flag, COUNT(*) AS member_count
FROM issuer_enrollees
GROUP BY subscriber_flag;
```

**Premium by rating area:**

```sql
SELECT rating_area, SUM(total_premium_amt) AS total_premium
FROM issuer_enrollees
GROUP BY rating_area
ORDER BY total_premium DESC;
```

**Duplicate member check (monthly):**

```sql
SELECT exchg_indiv_identifier, COUNT(*) AS occurrence_count
FROM issuer_enrollees
GROUP BY exchg_indiv_identifier
HAVING COUNT(*) > 1;
```

**Monthly premium trend (rollup DB):**

```sql
SELECT source_period, SUM(total_premium_amt) AS monthly_premium, COUNT(*) AS members
FROM issuer_enrollees_all_periods
GROUP BY source_period
ORDER BY source_period;
```

**Unique policies by file:**

```sql
SELECT source_file,
       COUNT(DISTINCT exchg_assigned_policy_id) AS unique_policies,
       COUNT(*) AS total_rows
FROM issuer_enrollees
GROUP BY source_file;
```

---

## Validation Checks (per partition)

- Required columns exist (including partition columns)
- Required ID fields not null
- Duplicate checks within file and within month
- QTYt consistency vs enrollee counts
- Subscriber flag values (`Y` / `N`)
- Insurance type codes tracked dynamically
- Premium fields numeric and non-negative
- Benefit effective date not null
- Source exchange ID presence
- Missingness percentage by column
- Row counts, unique policies, and unique members by file

Results are exported to Excel, JSON, SQLite, and the dashboard.

---

## KPIs (per partition)

- `total_files_processed`, `total_enrollment_records`, `total_enrollees`
- `total_subscribers`, `total_dependents`
- `unique_policies`, `unique_members`, `unique_households`
- `duplicate_member_count`, `duplicate_policy_member_count`
- `total_premium_amount`, `average_premium_amount`
- Breakdowns by file, rating area, effective month, insurance type, etc.

Rollup KPIs add cross-period breakdowns (`member_count_by_source_period`, `premium_by_source_period`).

---

## Dashboards

**Monthly:** `assets/{issuer}/{year}/{month}/dashboards/issuer_{issuer}_{year}_{month}_dashboard.html`

Title shows issuer, year, and month clearly.

**Rollup:** `assets/{issuer}/rollups/dashboards/issuer_{issuer}_all_periods_dashboard.html`

Shows trends across months (enrollees and premium by `source_period`).

Open in any browser:

```bash
open assets/64357/2026/02/dashboards/issuer_64357_2026_02_dashboard.html
```

---

## Error Handling

- Malformed XML files are logged and skipped; remaining files continue
- Partition-level failures do not stop other partitions
- Issuer rollup runs after all monthly partitions for that issuer complete successfully

---

## License

Internal use — adjust as needed for your organization.
