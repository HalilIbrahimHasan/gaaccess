"""
XML file reader with pluggable data-source abstraction.

The ``DataSource`` interface isolates *where* files live (local disk today,
SFTP/FTP tomorrow) from *how* they are parsed downstream. Adding remote
ingestion later only requires a new ``DataSource`` implementation.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from config import SOURCE_DATA_DIR
from utils.file_utils import discover_issuer_ids, list_xml_files
from utils.logger import get_logger

logger = get_logger(__name__)


class DataSource(ABC):
    """
    Abstract contract for locating and reading issuer XML files.

    Downstream ETL stages depend on this interface—not on filesystem or
    network details—so new ingestion channels can be swapped in cleanly.
    """

    @abstractmethod
    def discover_issuers(self) -> list[str]:
        """Return all issuer IDs that have available input data."""

    @abstractmethod
    def list_files(self, issuer_id: str) -> list[str]:
        """
        Return logical file identifiers for an issuer.

        For local sources these are absolute paths; for SFTP they might be
        remote paths that get staged locally before parsing.
        """

    @abstractmethod
    def read_bytes(self, file_id: str) -> bytes:
        """Read raw XML bytes for a given file identifier."""


class LocalFileSource(DataSource):
    """
    Read 834 XML files from ``source_data/{issuer_id}/*.xml`` on local disk.

    This is the default production source until SFTP ingestion is wired in.
    """

    def __init__(self, source_dir: Path | None = None) -> None:
        """
        Initialize the local file source.

        Args:
            source_dir: Root directory containing per-issuer subfolders.
        """
        self.source_dir = source_dir or SOURCE_DATA_DIR

    def discover_issuers(self) -> list[str]:
        """Discover issuer folders under the configured source root."""
        return discover_issuer_ids(self.source_dir)

    def list_files(self, issuer_id: str) -> list[str]:
        """List XML paths for an issuer as string identifiers."""
        return [str(p) for p in list_xml_files(issuer_id, self.source_dir)]

    def read_bytes(self, file_id: str) -> bytes:
        """Read a local XML file and return its raw bytes."""
        path = Path(file_id)
        logger.debug("Reading local file: %s", path)
        return path.read_bytes()


# Future SFTP implementation sketch (not active — documents extension point):
#
# class SFTPFileSource(DataSource):
#     """Stage remote SFTP files locally, then expose via DataSource API."""
#     def discover_issuers(self) -> list[str]: ...
#     def list_files(self, issuer_id: str) -> list[str]: ...
#     def read_bytes(self, file_id: str) -> bytes: ...


@dataclass
class XmlFileRecord:
    """
    Metadata wrapper for a single XML input file.

    Carries issuer context and source path so transform/validate stages can
    attribute rows back to the originating file.
    """

    issuer_id: str
    source_file: str
    file_path: str


class XmlReader:
    """
    High-level reader that enumerates issuer XML files via a ``DataSource``.

    Orchestrates discovery and per-file access without embedding transport
    logic, keeping the extract stage thin and testable.
    """

    def __init__(self, source: DataSource | None = None) -> None:
        """
        Args:
            source: Data source implementation; defaults to ``LocalFileSource``.
        """
        self.source = source or LocalFileSource()

    def discover_issuers(self) -> list[str]:
        """Return issuer IDs available from the configured source."""
        return self.source.discover_issuers()

    def get_file_records(
        self, issuer_id: str | None = None
    ) -> list[XmlFileRecord]:
        """
        Build ``XmlFileRecord`` entries for one or all issuers.

        Args:
            issuer_id: Process only this issuer; ``None`` processes all.

        Returns:
            List of file records ready for parsing.
        """
        issuer_ids = [issuer_id] if issuer_id else self.source.discover_issuers()
        records: list[XmlFileRecord] = []

        for iid in issuer_ids:
            for file_path in self.source.list_files(iid):
                records.append(
                    XmlFileRecord(
                        issuer_id=iid,
                        source_file=Path(file_path).name,
                        file_path=file_path,
                    )
                )

        logger.info(
            "Prepared %d XML file record(s) for issuer(s): %s",
            len(records),
            issuer_ids,
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
