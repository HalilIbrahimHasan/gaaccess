"""
Cleaned XML exporter — serializes enrollee DataFrame back to XML.

Provides a human-readable consolidated XML output per issuer for systems
that prefer XML over flat-file or database consumption.
"""

import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


class XmlExporter:
    """
    Export cleaned enrollee records to a consolidated XML file.

    Reconstructs a simplified ``<enrollments>`` document from the cleaned
    DataFrame without re-emitting masked PII values.
    """

    def export_enrollees(
        self, df: pd.DataFrame, issuer_id: str, output_dir: Path
    ) -> Path:
        """
        Write cleaned enrollees to ``cleaned_enrollees_{issuer_id}.xml``.

        Args:
            df: Cleaned enrollee DataFrame.
            issuer_id: Issuer identifier.
            output_dir: Target ``assets/{issuer_id}/cleaned_xml`` directory.

        Returns:
            Path to the written XML file.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"cleaned_enrollees_{issuer_id}.xml"

        root = ET.Element("enrollments")
        root.set("issuer_id", issuer_id)
        root.set("record_count", str(len(df)))

        for _, row in df.iterrows():
            enrollee_el = ET.SubElement(root, "enrollee")
            for col, val in row.items():
                if pd.isna(val) or val == "":
                    continue
                child = ET.SubElement(enrollee_el, col)
                child.text = str(val)

        tree = ET.ElementTree(root)
        ET.indent(tree, space="  ")
        tree.write(path, encoding="UTF-8", xml_declaration=True)

        logger.info("Exported cleaned XML to %s (%d records)", path, len(df))
        return path
