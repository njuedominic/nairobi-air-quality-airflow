from unittest.mock import Mock

from nairobi_air_quality_airflow.api.openaq_client import OpenAQClient


def test_get_locations_builds_expected_request_and_returns_results():
    client = OpenAQClient(api_key="abc123", host="api.openaq.org")
    mock_response = Mock()
    mock_response.json.return_value = {"results": [{"id": 1, "name": "Nairobi"}]}

    client.session.get = Mock(return_value=mock_response)

    results = client.get_locations(latitude=-1.28, longitude=36.82, radius=5000, limit=50)

    client.session.get.assert_called_once_with(
        "https://api.openaq.org/v3/locations",
        params={
            "coordinates": "-1.28,36.82",
            "radius": 5000,
            "limit": 50,
        },
        timeout=30,
    )
    assert results == [{"id": 1, "name": "Nairobi"}]


def test_get_measurements_returns_results_from_api_payload():
    client = OpenAQClient(api_key="abc123", host="api.openaq.org")
    mock_response = Mock()
    mock_response.json.return_value = {"results": [{"value": 7.5}]}

    client.session.get = Mock(return_value=mock_response)

    results = client.get_measurements(sensor_id=42, limit=25)

    client.session.get.assert_called_once_with(
        "https://api.openaq.org/v3/sensors/42/measurements",
        params={"limit": 25},
        timeout=30,
    )
    assert results == [{"value": 7.5}]
