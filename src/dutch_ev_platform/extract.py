"""HTTP extraction from the RDW Socrata APIs."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
import logging
import time
from typing import Any

import requests

from .config import Settings


LOGGER = logging.getLogger(__name__)


class ExtractionError(RuntimeError):
    """Raised when an RDW API request cannot be completed or validated."""


class RDWClient:
    def __init__(
        self,
        settings: Settings,
        session: requests.Session | None = None,
        sleep: Any = time.sleep,
    ) -> None:
        self.settings = settings
        self.session = session or requests.Session()
        self.sleep = sleep
        self.request_count = 0
        self.session.headers.update(
            {"User-Agent": "dutch-ev-data-platform/0.2 (portfolio project)"}
        )

    def _get(self, url: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt in range(1, self.settings.max_retries + 1):
            try:
                self.request_count += 1
                response = self.session.get(
                    url, params=params, timeout=self.settings.request_timeout_seconds
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list) or not all(
                    isinstance(row, dict) for row in payload
                ):
                    raise ExtractionError("RDW response is not a JSON array of objects")
                return payload
            except (requests.RequestException, ValueError, ExtractionError) as exc:
                last_error = exc
                LOGGER.warning(
                    "RDW request failed",
                    extra={"event": "request_retry", "dataset": url, "row_count": attempt},
                )
                if attempt < self.settings.max_retries:
                    self.sleep(2 ** (attempt - 1))
        # Request exception strings can contain the fully rendered query URL,
        # including licence plates used by matching detail requests. Suppress
        # both that text and the chained exception before structured logging.
        raise ExtractionError(
            "RDW request failed after retries; request details suppressed for privacy"
        ) from None

    @staticmethod
    def _quote(value: str) -> str:
        return f"'{value.replace(chr(39), chr(39) * 2)}'"

    @staticmethod
    def batches(values: Iterable[str], size: int) -> Iterator[list[str]]:
        batch: list[str] = []
        for value in values:
            if value:
                batch.append(value)
            if len(batch) == size:
                yield batch
                batch = []
        if batch:
            yield batch

    def fetch_ev_identifier_page(
        self, limit: int, after_identifier: str | None = None
    ) -> list[dict[str, Any]]:
        fuel_filter = "brandstof_omschrijving in ('Elektriciteit','Waterstof')"
        where = fuel_filter
        if after_identifier is not None:
            where = f"({fuel_filter}) AND kenteken > {self._quote(after_identifier)}"
        return self._get(
            self.settings.fuel_url,
            {
                "$select": "kenteken",
                "$where": where,
                "$group": "kenteken",
                "$limit": limit,
                "$order": "kenteken",
            },
        )

    def fetch_vehicle_pages(
        self, vehicle_ids: Iterable[str]
    ) -> Iterator[list[dict[str, Any]]]:
        identifiers = sorted({value for value in vehicle_ids if value})
        fields = (
            "kenteken,merk,handelsbenaming,datum_eerste_toelating,"
            "eerste_kleur,tweede_kleur,voertuigsoort"
        )
        for chunk in self.batches(identifiers, self.settings.detail_batch_size):
            quoted = ",".join(self._quote(value) for value in chunk)
            offset = 0
            while True:
                rows = self._get(
                    self.settings.vehicle_url,
                    {
                        "$select": fields,
                        "$where": f"kenteken in ({quoted})",
                        "$limit": self.settings.page_size,
                        "$offset": offset,
                        "$order": "kenteken",
                    },
                )
                yield rows
                if len(rows) < self.settings.page_size:
                    break
                offset += self.settings.page_size

    def fetch_fuel_pages(
        self, vehicle_ids: Iterable[str]
    ) -> Iterator[list[dict[str, Any]]]:
        identifiers = sorted({value for value in vehicle_ids if value})
        for chunk in self.batches(identifiers, self.settings.detail_batch_size):
            quoted = ",".join(self._quote(value) for value in chunk)
            offset = 0
            while True:
                rows = self._get(
                    self.settings.fuel_url,
                    {
                        "$select": (
                            "kenteken,brandstof_volgnummer,brandstof_omschrijving,"
                            "emissiecode_omschrijving,co2_uitstoot_gecombineerd,"
                            "nettomaximumvermogen,klasse_hybride_elektrisch_voertuig"
                        ),
                        "$where": f"kenteken in ({quoted})",
                        "$limit": self.settings.page_size,
                        "$offset": offset,
                        "$order": "kenteken,brandstof_volgnummer",
                    },
                )
                yield rows
                if len(rows) < self.settings.page_size:
                    break
                offset += self.settings.page_size
