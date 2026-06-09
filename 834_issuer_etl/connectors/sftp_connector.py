"""SFTP connector placeholder — does not connect yet."""

from __future__ import annotations

from connectors.base_connector import SourceConnector, SourceFile
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class SFTPSourceConnector(SourceConnector):
    """Future SFTP download → source_data, then same local pipeline."""

    def mode(self) -> str:
        return "sftp"

    def sync(self) -> list[SourceFile]:
        logger.warning(
            "SFTP mode is not implemented yet. Host=%s user=%s path=%s",
            settings.sftp_host,
            settings.sftp_user,
            settings.sftp_remote_path,
        )
        logger.info("Falling back to local source_data scan.")
        from connectors.local_connector import LocalSourceConnector

        return LocalSourceConnector().sync()
