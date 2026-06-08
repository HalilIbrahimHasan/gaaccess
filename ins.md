Create a new professional Python ETL framework for 834 XML issuer enrollment files.

Context:
We currently have one issuer: 64357.
Input files are XML files located under:

source_data/
64357/
*.xml

Later, more issuers will be added like:

source_data/
64357/
68806/
49046/

The framework must dynamically process every issuer folder without hardcoding one issuer only.

Goal:
Build an ETL-style Python project that reads 834 XML enrollment files, parses them into clean pandas DataFrames, validates the data, generates KPIs, exports cleaned outputs, loads the data into SQLite, and creates an interactive Plotly HTML dashboard.

Important:
Add clear docstrings and comments for every class, function, and method explaining:

* what it does
* why it is needed
* how it supports the ETL/data validation process

Project structure:

834_issuer_etl/
source_data/
64357/
from_64357_GA_834_INDV_20260204071545.xml
from_64357_GA_834_INDV_20260211071730.xml
from_64357_GA_834_INDV_20260218064929.xml
from_64357_GA_834_INDV_20260225064930.xml
assets/
64357/
cleaned_xml/
excel/
sqlite/
dashboards/
validation_reports/
src/
config.py
main.py
extract/
xml_reader.py
transform/
xml_parser.py
cleaner.py
kpi_builder.py
validate/
schema_validator.py
data_quality_validator.py
load/
excel_exporter.py
xml_exporter.py
sqlite_loader.py
dashboard/
plotly_dashboard.py
utils/
logger.py
file_utils.py
requirements.txt
README.md

Technical requirements:
Use Python 3.10+.
Use pandas, lxml or xml.etree.ElementTree, openpyxl, sqlite3 or SQLAlchemy, plotly.
Do not require FTP/SFTP yet. For now, read from local source_data folder.
Design the source layer so FTP/SFTP can be added later without changing the downstream ETL logic.

XML parsing requirements:
Parse each XML file under each issuer folder.
Each XML root contains enrollments.
Each enrollment may contain one or more enrollee records.
Create one normalized row per enrollee.

Capture file-level/header fields:

* source_file
* issuer_id parsed from folder name or filename
* ISA09
* ISA10
* ISA13
* GS04
* GS05
* GS06
* ST02
* actionCode
* insurerTaxIdNumber
* QTYn
* QTYy
* QTYt

Capture enrollee/member fields when available:

* subscriberFlag
* relationshipCode from relationshipLkp.lookupValueCode
* eventTypeCode from enrollmentEvents.eventTypeLkp.lookupValueCode
* eventReasonCode from enrollmentEvents.eventReasonLookUp.lookupValueCode
* exchgSubscriberIdentifier
* exchgAssignedPolicyID
* exchgIndivIdentifier
* issuerSubscriberIdentifier
* issuerIndivIdentifier
* memberMaintEffectiveDate
* memberEntityIdentifierCode
* memberGenderCode
* memberMaritalStatusCode
* memberCitizenshipStatusCode
* memberTobaccoUsageCode
* city
* state
* zip
* memberBirthDate

Capture coverage/reporting fields:

* maintenanceTypeCode
* insuranceTypeCode
* benefitEffectiveBeginDate
* lastPremiumPaidDate
* householdOrEmployeeCaseID
* classOfContractCode
* healthCoveragePolicyNo
* aptcAmt
* healthCoveragePremiumAmt
* ratingArea
* totalIndivResponsibilityAmt
* totalPremiumAmt
* sourceExchgID
* additionalMaintReasonCode

PII handling:
Because XML contains sensitive member data, do not export SSN, phone, email, first name, last name, or full address by default.
If needed for debugging, make this configurable with EXPORT_PII=False in config.py.
Default should be safe and masked.

Cleaning requirements:

* Strip whitespace from all string fields.
* Convert date fields from YYYYMMDD to real date format.
* Convert numeric amount fields to decimal/float.
* Standardize column names.
* Add load_timestamp.
* Add file_date derived from GS04 or filename.
* Add issuer_id.
* Add source_file.

Validation requirements:
Create validation reports per issuer.
Validate:

* required columns exist
* required ID fields are not null
* duplicate checks on issuer_id + source_file + exchgIndivIdentifier
* duplicate checks on issuer_id + exchgIndivIdentifier across all files
* QTYt consistency where possible against enrollee counts per enrollment
* subscriberFlag values should be Y or N
* insuranceTypeCode should be tracked dynamically, not hardcoded
* premium fields should be numeric
* totalPremiumAmt should not be negative
* benefitEffectiveBeginDate should not be null
* sourceExchgID should be present when available
* missingness percentage by column
* row counts by file
* unique policy count by file
* unique member count by file

KPIs:
Create issuer-level KPIs:

* total_files_processed
* total_enrollment_records
* total_enrollees
* total_subscribers
* total_dependents
* unique_policies
* unique_members
* unique_households
* duplicate_member_count
* duplicate_policy_member_count
* total_premium_amount
* total_individual_responsibility_amount
* average_premium_amount
* average_individual_responsibility_amount
* member_count_by_subscriber_flag
* member_count_by_relationship_code
* member_count_by_event_type
* member_count_by_event_reason
* member_count_by_maintenance_type
* member_count_by_insurance_type
* member_count_by_rating_area
* member_count_by_effective_month
* premium_by_rating_area
* premium_by_effective_month
* file_count_trend
* enrollee_count_by_file

Exports:
For each issuer, export to:
assets/{issuer_id}/excel/
cleaned_enrollees_{issuer_id}.xlsx
kpi_summary_{issuer_id}.xlsx
validation_report_{issuer_id}.xlsx

assets/{issuer_id}/cleaned_xml/
cleaned_enrollees_{issuer_id}.xml

assets/{issuer_id}/sqlite/
issuer_{issuer_id}.db

SQLite:
Create tables:

* issuer_enrollees
* issuer_kpis
* validation_results

Also create example SQL queries in README.md:

* count total enrollees
* count subscribers/dependents
* premium by rating area
* duplicate member check
* unique policies by file
* monthly premium trend

Dashboard:
Generate one interactive Plotly HTML dashboard per issuer:
assets/{issuer_id}/dashboards/issuer_{issuer_id}_dashboard.html

Dashboard should include:

* KPI cards or summary table
* enrollees by source file
* subscribers vs dependents
* premium by rating area
* members by effective month
* validation issue summary
* missingness by column
* duplicate count summary

Main runner:
Running this command should process all issuers:

python src/main.py

Optional:
Allow one issuer only:

python src/main.py --issuer 64357

Code quality:

* Modular and dynamic
* No hardcoded issuer logic except example default
* Clear logging
* Error handling if XML file is malformed
* Continue processing other files if one file fails
* README should explain setup, folder structure, how to run, where outputs are created, and how to add a new issuer folder.

Please generate the complete framework files with production-quality Python code, comments, docstrings, and a clean README.
