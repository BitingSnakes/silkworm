from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

try:
    from google.oauth2.service_account import (  # pyright: ignore[reportMissingImports]
        Credentials,
    )
    from googleapiclient.discovery import (  # pyright: ignore[reportMissingImports]
        build,
    )

    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    Credentials = None
    build = None
    GOOGLE_SHEETS_AVAILABLE = False

from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class GoogleSheetsPipeline:
    """
    Pipeline that appends items to a Google Sheet.

    Requires Google Sheets API credentials (service account JSON file).

    Args:
        spreadsheet_id: Spreadsheet identifier from its URL.
        credentials_file: Service-account credentials JSON file.
        sheet_name: Worksheet receiving rows.
        batch_size: Items flattened and appended per API batch.

    Example:
        from silkworm.pipelines import GoogleSheetsPipeline

        pipeline = GoogleSheetsPipeline(
            spreadsheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            credentials_file="path/to/credentials.json",
            sheet_name="Sheet1",
        )
    """

    def __init__(
        self,
        spreadsheet_id: str,
        credentials_file: str,
        *,
        sheet_name: str = "Sheet1",
        batch_size: int = 100,
    ) -> None:
        """
        Initialize GoogleSheetsPipeline.

        Args:
            spreadsheet_id: Google Sheets spreadsheet ID (from the URL)
            credentials_file: Path to service account credentials JSON file
            sheet_name: Name of the sheet to append to (default: "Sheet1")
            batch_size: Number of items to batch before writing (default: 100)
        """
        if not GOOGLE_SHEETS_AVAILABLE:
            raise ImportError(
                "google-api-python-client and google-auth are required for GoogleSheetsPipeline. "
                "Install them with: pip install silkworm-rs[gsheets]",
            )

        self.spreadsheet_id = spreadsheet_id
        self.credentials_file = credentials_file
        self.sheet_name = sheet_name
        self.batch_size = batch_size
        self._service = None
        self._batch: list[JSONValue] = []
        self._fieldnames: list[str] | None = None
        self._header_written = False
        self.logger: Logger = get_logger(component="GoogleSheetsPipeline")

    async def open(self, spider: Spider) -> None:
        """Authenticate the Sheets service and reset batching state."""
        # Initialize Google Sheets API client
        creds = Credentials.from_service_account_file(  # type: ignore[union-attr]
            self.credentials_file,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        self._service = build("sheets", "v4", credentials=creds)  # type: ignore[misc]
        self._batch = []
        self._fieldnames = None
        self._header_written = False
        self.logger.info(
            "Opened Google Sheets pipeline",
            spreadsheet_id=self.spreadsheet_id,
            sheet_name=self.sheet_name,
        )

    async def close(self, spider: Spider) -> None:
        """Write any partial batch and release the Sheets service reference."""
        # Write any remaining batched items
        if self._batch:
            await self._write_batch()

        self._service = None
        self.logger.info(
            "Closed Google Sheets pipeline",
            spreadsheet_id=self.spreadsheet_id,
            sheet_name=self.sheet_name,
        )

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Buffer one item and append a batch when ``batch_size`` is reached."""
        if not self._service:
            raise RuntimeError("GoogleSheetsPipeline not opened")

        self._batch.append(item)

        if len(self._batch) >= self.batch_size:
            await self._write_batch()

        return item

    async def _write_batch(self) -> None:
        """Write the current batch of items to Google Sheets."""
        if not self._batch or not self._service:
            return

        try:
            # Flatten items if they are dicts
            rows: list[list[str | int | float | bool | None]] = []

            for item in self._batch:
                if isinstance(item, Mapping):
                    flat_item = self._flatten_dict(item)
                    # Initialize fieldnames from first item
                    if self._fieldnames is None:
                        self._fieldnames = list(flat_item.keys())
                    row = [flat_item.get(field) for field in self._fieldnames]
                    rows.append(row)  # type: ignore
                else:
                    # Simple value
                    if self._fieldnames is None:
                        self._fieldnames = ["value"]
                    rows.append([str(item)])

            # Write header if first batch
            if not self._header_written and self._fieldnames:
                header_range = f"{self.sheet_name}!A1"
                header_body = {"values": [self._fieldnames]}
                self._service.spreadsheets().values().append(
                    spreadsheetId=self.spreadsheet_id,
                    range=header_range,
                    valueInputOption="RAW",
                    body=header_body,
                ).execute()
                self._header_written = True

            # Append data rows
            if rows:
                data_range = f"{self.sheet_name}!A2"
                body = {"values": rows}
                self._service.spreadsheets().values().append(
                    spreadsheetId=self.spreadsheet_id,
                    range=data_range,
                    valueInputOption="RAW",
                    body=body,
                ).execute()

            log_pipeline_item(
                self,
                "Wrote items to Google Sheets",
                spreadsheet_id=self.spreadsheet_id,
                sheet_name=self.sheet_name,
                count=len(self._batch),
            )
        except Exception as exc:
            self.logger.error(
                "Failed to write items to Google Sheets",
                spreadsheet_id=self.spreadsheet_id,
                sheet_name=self.sheet_name,
                count=len(self._batch),
                error=str(exc),
            )
            raise

        # Clear the batch after writing
        self._batch = []

    def _flatten_dict(
        self,
        data: Mapping[str, JSONValue],
        parent_key: str = "",
        sep: str = "_",
    ) -> dict[str, JSONValue | str]:
        """Flatten a nested dictionary structure."""
        items: list[tuple[str, JSONValue | str]] = []
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, Mapping):
                items.extend(self._flatten_dict(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                # Convert list to comma-separated string
                items.append((new_key, ", ".join(str(v) for v in value)))
            else:
                items.append((new_key, value))
        return dict(items)
