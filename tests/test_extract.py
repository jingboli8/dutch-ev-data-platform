from __future__ import annotations

from unittest.mock import Mock

import pytest
import requests

from dutch_ev_platform.extract import ExtractionError, RDWClient


def test_fetch_vehicle_sample_uses_limit_and_select(settings):
    response = Mock()
    response.json.return_value = [{"kenteken": "TEST_VEHICLE_001"}]
    response.raise_for_status.return_value = None
    session = Mock()
    session.headers = {}
    session.get.return_value = response

    rows = RDWClient(settings, session=session).fetch_vehicle_sample(2)

    assert rows == [{"kenteken": "TEST_VEHICLE_001"}]
    params = session.get.call_args.kwargs["params"]
    assert params["$limit"] == 2
    assert "kenteken" in params["$select"]


def test_fetch_ev_identifier_sample_filters_and_limits(settings):
    response = Mock()
    response.json.return_value = [{"kenteken": "TEST_VEHICLE_001"}]
    response.raise_for_status.return_value = None
    session = Mock()
    session.headers = {}
    session.get.return_value = response

    identifiers = RDWClient(settings, session=session).fetch_ev_identifier_sample(5)

    assert identifiers == ["TEST_VEHICLE_001"]
    params = session.get.call_args.kwargs["params"]
    assert params["$limit"] == 5
    assert "Elektriciteit" in params["$where"]


def test_extraction_wraps_request_errors(settings):
    session = Mock()
    session.headers = {}
    session.get.side_effect = requests.Timeout("slow")

    with pytest.raises(ExtractionError, match="after retries"):
        RDWClient(settings, session=session, sleep=lambda _: None).fetch_vehicle_sample(1)
