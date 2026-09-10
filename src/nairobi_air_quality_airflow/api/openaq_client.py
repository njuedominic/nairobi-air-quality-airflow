import requests


class OpenAQClient:
    def __init__(self, api_key: str, host: str):
        self.base_url = f"https://{host}/v3"

        self.session = requests.Session()

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
        