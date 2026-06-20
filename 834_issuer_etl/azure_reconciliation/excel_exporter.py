"""Excel exports for Azure vs XML reconciliation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


def write_excel_report(
    output_path: Path,
    sheets: dict[str, pd.DataFrame],
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            safe = name[:31]
            (df if not df.empty else pd.DataFrame()).to_excel(writer, sheet_name=safe, index=False)
    logger.info("Wrote Excel report: %s (%d sheets)", output_path, len(sheets))
    return output_path


def build_comparison_workbook(
    *,
    final_summary: pd.DataFrame,
    status_summary: pd.DataFrame,
    issuer_month: pd.DataFrame,
    xml_summary: pd.DataFrame,
    azure_summary: pd.DataFrame,
    match_sample: pd.DataFrame,
    status_diff: pd.DataFrame,
    xml_not_in_azure: pd.DataFrame,
    azure_not_in_xml: pd.DataFrame,
    detailed: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    return {
        "Final_Result_Summary": final_summary,
        "Status_Result_Summary": status_summary,
        "Issuer_Month_Result": issuer_month,
        "XML_Summary": xml_summary,
        "Azure_Summary": azure_summary,
        "Match_Sample": match_sample,
        "Status_Diff": status_diff,
        "XML_Not_In_Azure": xml_not_in_azure,
        "Azure_Not_In_XML": azure_not_in_xml,
        "Detailed_Comparison": detailed,
    }
