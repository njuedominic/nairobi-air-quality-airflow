import requests
from urllib3 import Retry
from requests.adapters import HTTPAdapter


class OpenAQClient:
    def __init__(self, api_key: str, host: str):
        self.base_url = f"https://{host}/v3"

        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("https://", adapter)

        self.session.headers.update(
            {
                "X-API-Key": api_key,
                "Accept": "application/json",
            }
        )

    def get_locations(
        self,
        latitude: float = -1.278560,
        longitude: float = 36.821249,
        radius: int = 25000,
        limit: int = 100,
    ) -> list[dict]:
        response = self.session.get(
            f"{self.base_url}/locations",
            params={
                "coordinates": f"{latitude},{longitude}",
                "radius": radius,
                "limit": limit,
            },
            timeout=30,
        )

        response.raise_for_status()

        payload = response.json()

        return payload.get("results", [])

    def get_sensors(
            self,
            location_id: int,
    )-> list[dict]:
        response = self.session.get(
            f"{self.base_url}/locations/{location_id}/sensors",
            params={
                "location_id": location_id,
            },
            timeout=30,
        )

        response.raise_for_status()

        payload = response.json()

        return payload.get("results", [])

    #----------------------------------------------------------------------------

    def get_measurements(
            self,
            sensor_id: int,
            limit: int = 100,
    )-> list[dict]:

        response = self.session.get(
            f"{self.base_url}/sensors/{sensor_id}/measurements",
            params={
                "limit": limit,
            },
            timeout=30,
        )

        response.raise_for_status()

        payload = response.json()

        return payload.get("results", [])