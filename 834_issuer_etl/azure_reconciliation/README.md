# Azure vs XML Enrollment Reconciliation

Extension module — does **not** modify the existing pipeline, SFTP downloader, parser, or `main.py`.

## Run

```bash
# Full reconciliation (requires Azure credentials in .env)
python run_azure_reconciliation.py

# Filtered scope (matches source_data partitions)
python run_azure_reconciliation.py --issuer 13535 --year 2026 --month 02

# XML + column mapping only (no Azure connection)
python run_azure_reconciliation.py --skip-azure

# Force parse source_data instead of reading staging SQLite
python run_azure_reconciliation.py --parse-source
```

## Azure credentials (.env)

```
AZURE_SQL_SERVER=your-server.database.windows.net
AZURE_SQL_DATABASE=your-db
AZURE_SQL_USERNAME=your-user
AZURE_SQL_PASSWORD=your-password
AZURE_SQL_DRIVER=ODBC Driver 18 for SQL Server
AZURE_ENROLLMENTS_TABLE=Enrollments_PY2026
```

Legacy aliases `SERVER`, `DATABASE`, `USERNAME`, `PASSWORD`, `DRIVER` are also supported.

## Data sources

| Source | Path | Notes |
|--------|------|-------|
| XML | `source_data/{issuer}/{year}/{month}/` | **Only** XML input; never reads `assets/` |
| Staging (optional) | `data/issuer_834.db` | Used when populated by existing pipeline |
| Azure | `dbo.Enrollments_PY2026` (configurable) | SELECT only via SQLAlchemy |

## Outputs

All under `outputs/reconciliation/`:

| Output | Location |
|--------|----------|
| Column mapping report | `excel/column_mapping_report.xlsx` |
| Comparison workbook | `excel/azure_xml_comparison.xlsx` |
| Dashboard | `dashboards/reconciliation_dashboard.html` |
| SQLite | `sqlite/reconciliation.db` |

SQLite tables: `xml_snapshot`, `azure_snapshot`, `comparison_detail`, `comparison_summary`, `issuer_month_summary`

## Architecture

```
source_data discovery → XML load/parse → lifecycle replay (chronological)
                                              ↓
Azure SQL (dynamic) ──────────────→ column mapping → compare → reports
```

## Join key (canonical)

`issuer + enrollment_id + enrollee_id + insurance_type`

Mapped dynamically from available columns — see `column_mapping_report.xlsx`.

## Lifecycle engine

Replays **all** XML maintenance events chronologically up to each target month.
Final state per member = comparison state (not latest row alone).
