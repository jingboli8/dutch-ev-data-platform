from __future__ import annotations

from unittest.mock import Mock

import pytest
import requests

from dutch_ev_platform.extract import ExtractionError, RDWClient


def _response(rows):
    response = Mock()
    response.json.return_value = rows
    response.raise_for_status.return_value = None
    return response


def test_keyset_identifier_pages_are_grouped_ordered_and_advanced(settings):
    session = Mock()
    session.headers = {}
    session.get.side_effect = [
        _response([{"kenteken": "TEST_VEHICLE_001"}]),
        _response([{"kenteken": "TEST_VEHICLE_002"}]),
    ]
    client = RDWClient(settings, session=session)

    first = client.fetch_ev_identifier_page(1)
    second = client.fetch_ev_identifier_page(1, first[-1]["kenteken"])

    assert len(first) == len(second) == 1
    first_params = session.get.call_args_list[0].kwargs["params"]
    second_params = session.get.call_args_list[1].kwargs["params"]
    assert first_params["$group"] == "kenteken"
    assert first_params["$order"] == "kenteken"
    assert "kenteken >" in second_params["$where"]
    assert client.request_count == 2


def test_empty_identifier_page_is_returned_unchanged(settings):
    session = Mock()
    session.headers = {}
    session.get.return_value = _response([])

    assert RDWClient(settings, session=session).fetch_ev_identifier_page(2) == []


def test_detail_query_paginates_until_short_page(settings):
    session = Mock()
    session.headers = {}
    session.get.side_effect = [
        _response(
            [
                {"kenteken": "TEST_VEHICLE_001"},
                {"kenteken": "TEST_VEHICLE_002"},
            ]
        ),
        _response([]),
    ]
    client = RDWClient(settings, session=session)

    pages = list(
        client.fetch_vehicle_pages(
            ["TEST_VEHICLE_001", "TEST_VEHICLE_002"]
        )
    )

    assert [len(page) for page in pages] == [2, 0]
    assert session.get.call_args_list[1].kwargs["params"]["$offset"] == 2


def test_extraction_wraps_retry_failure_without_identifier_leak(settings, caplog):
    session = Mock()
    session.headers = {}
    session.get.side_effect = requests.Timeout(
        "failed URL contained TEST_VEHICLE_001"
    )

    with pytest.raises(ExtractionError, match="after retries") as error:
        RDWClient(
            settings, session=session, sleep=lambda _: None
        ).fetch_ev_identifier_page(1)

    assert "TEST_VEHICLE_001" not in str(error.value)
    assert "TEST_VEHICLE_001" not in caplog.text
