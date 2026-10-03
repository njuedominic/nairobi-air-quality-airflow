from fastapi.testclient import TestClient

from nairobi_air_quality_airflow.api.service import app


def test_runtime_config_returns_mapbox_access_token(monkeypatch):
    monkeypatch.setenv("MAPBOX_ACCESS_TOKEN", "pk.test-token")
    client = TestClient(app)

    response = client.get("/config")

    assert response.status_code == 200
    assert response.json() == {"mapbox_access_token": "pk.test-token"}