"""
XML file reader with pluggable data-source abstraction.

The ``DataSource`` interface isolates *where* files live (local disk today,
SFTP/FTP tomorrow) from *how* they are parsed downstream. Partitions are
discovered at issuer/year/month granularity for scalable processing.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from config import SOURCE_DATA_DIR
from utils.logger import get_logger
from utils.partition import Partition, discover_partitions

logger = get_logger(__name__)


class DataSource(ABC):
    """
    Abstract contract for locating and reading partitioned issuer XML files.

    Downstream ETL stages depend on this interface—not on filesystem or
    network details—so new ingestion channels can be swapped in cleanly.
    """

    @abstractmethod
    def discover_partitions(
        self,
        issuer_id: str | None = None,
        year: str | None = None,
        month: str | None = None,
    ) -> list[Partition]:
        """Return all partitions matching optional issuer/year/month filters."""

    @abstractmethod
    def read_bytes(self, file_id: str) -> bytes:
        """Read raw XML bytes for a given file identifier."""


class LocalFileSource(DataSource):
    """
    Read 834 XML files from ``source_data/{issuer}/{year}/{month}/`` on disk.

    This is the default production source until SFTP ingestion is wired in.
    """

    def __init__(self, source_dir: Path | None = None) -> None:
        """
        Initialize the local file source.

        Args:
            source_dir: Root directory containing partitioned issuer folders.
        """
        self.source_dir = source_dir or SOURCE_DATA_DIR

    def discover_partitions(
        self,
        issuer_id: str | None = None,
        year: str | None = None,
        month: str | None = None,
    ) -> list[Partition]:
        """Discover local issuer/year/month partitions under ``source_dir``."""
        return discover_partitions(
            source_root=self.source_dir,
            issuer_id=issuer_id,
            year=year,
            month=month,
        )

    def read_bytes(self, file_id: str) -> bytes:
        """Read a local XML file and return its raw bytes."""
        path = Path(file_id)
        logger.debug("Reading local file: %s", path)
        return path.read_bytes()


# Future SFTP implementation sketch (not active — documents extension point):
#
# class SFTPFileSource(DataSource):
#     """Stage remote SFTP partitions locally, then expose via DataSource API."""
#     def discover_partitions(...) -> list[Partition]: ...
#     def read_bytes(self, file_id: str) -> bytes: ...


@dataclass
class XmlFileRecord:
    """
    Metadata wrapper for a single XML input file within a partition.

    Carries issuer and period context so transform/validate stages can
    attribute rows back to the originating file and time partition.
    """

    issuer_id: str
    source_year: str
    source_month: str
    source_period: str
    source_file: str
    file_path: str


class XmlReader:
    """
    High-level reader that enumerates partition XML files via a ``DataSource``.

    Orchestrates discovery and per-file access without embedding transport
    logic, keeping the extract stage thin and testable.
    """

    def __init__(self, source: DataSource | None = None) -> None:
        """
        Args:
            source: Data source implementation; defaults to ``LocalFileSource``.
        """
        self.source = source or LocalFileSource()

    def discover_partitions(
        self,
        issuer_id: str | None = None,
        year: str | None = None,
        month: str | None = None,
    ) -> list[Partition]:
        """Return partitions available from the configured source."""
        return self.source.discover_partitions(
            issuer_id=issuer_id, year=year, month=month
        )

    def get_file_records(self, partition: Partition) -> list[XmlFileRecord]:
        """
        Build ``XmlFileRecord`` entries for all XML files in a partition.

        Args:
            partition: Issuer/year/month partition to read.

        Returns:
            List of file records ready for parsing.
        """
        records = [
            XmlFileRecord(
                issuer_id=partition.issuer_id,
                source_year=partition.year,
                source_month=partition.month,
                source_period=partition.source_period,
                source_file=xml_path.name,
                file_path=str(xml_path),
            )
            for xml_path in partition.xml_files
        ]
        logger.info(
            "Prepared %d XML file record(s) for partition %s",
            len(records),
            partition.period_key,
        )
        return records

    def read_xml_content(self, record: XmlFileRecord) -> bytes:
        """
        Read raw XML bytes for a file record.

        Args:
            record: File metadata from ``get_file_records``.

        Returns:
            Raw file bytes for the XML parser.
        """
        return self.source.read_bytes(record.file_path)
