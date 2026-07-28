"""HTTP extraction from the RDW Socrata APIs."""

from __future__ import annotations

from collections.abc import Iterable
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
        self.session.headers.update(
            {"User-Agent": "dutch-ev-data-platform/0.1 (portfolio project)"}
        )

    def _get(self, url: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        last_error: Exception | None = None
        for attempt in range(1, self.settings.max_retries + 1):
            try:
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
        raise ExtractionError(f"RDW request failed after retries: {last_error}") from last_error

    def fetch_vehicle_sample(self, limit: int) -> list[dict[str, Any]]:
        fields = (
            "kenteken,merk,handelsbenaming,datum_eerste_toelating,"
            "eerste_kleur,tweede_kleur,voertuigsoort"
        )
        return self._get(
            self.settings.vehicle_url,
            {"$select": fields, "$limit": limit, "$order": "kenteken"},
        )

    def fetch_ev_identifier_sample(self, limit: int) -> list[str]:
        rows = self._get(
            self.settings.fuel_url,
            {
                "$select": "kenteken",
                "$where": "brandstof_omschrijving in ('Elektriciteit','Waterstof')",
                "$limit": limit,
                "$order": "kenteken",
            },
        )
        return [
            str(row.get("kenteken", "")).strip()
            for row in rows
            if row.get("kenteken")
        ]

    def fetch_vehicles_by_ids(
        self, vehicle_ids: Iterable[str], chunk_size: int = 50
    ) -> list[dict[str, Any]]:
        identifiers = sorted({value for value in vehicle_ids if value})
        fields = (
            "kenteken,merk,handelsbenaming,datum_eerste_toelating,"
            "eerste_kleur,tweede_kleur,voertuigsoort"
        )
        rows: list[dict[str, Any]] = []
        for start in range(0, len(identifiers), chunk_size):
            chunk = identifiers[start : start + chunk_size]
            quoted = ",".join(
                f"'{value.replace(chr(39), chr(39) * 2)}'" for value in chunk
            )
            rows.extend(
                self._get(
                    self.settings.vehicle_url,
                    {
                        "$select": fields,
                        "$where": f"kenteken in ({quoted})",
                        "$limit": self.settings.page_size,
                        "$order": "kenteken",
                    },
                )
            )
        return rows

    def fetch_fuels_for_vehicles(
        self, vehicle_ids: Iterable[str], chunk_size: int = 50
    ) -> list[dict[str, Any]]:
        identifiers = sorted({value for value in vehicle_ids if value})
        rows: list[dict[str, Any]] = []
        for start in range(0, len(identifiers), chunk_size):
            chunk = identifiers[start : start + chunk_size]
            quoted = ",".join(f"'{value.replace(chr(39), chr(39) * 2)}'" for value in chunk)
            rows.extend(
                self._get(
                    self.settings.fuel_url,
                    {
                        "$select": (
                            "kenteken,brandstof_volgnummer,brandstof_omschrijving,"
                            "emissiecode_omschrijving,co2_uitstoot_gecombineerd,"
                            "nettomaximumvermogen,klasse_hybride_elektrisch_voertuig"
                        ),
                        "$where": f"kenteken in ({quoted})",
                        "$limit": self.settings.page_size,
                        "$order": "kenteken,brandstof_volgnummer",
                    },
                )
            )
        return rows
