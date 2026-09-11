from datetime import datetime, timedelta
from airflow import sensors
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from nairobi_air_quality_airflow.api.openaq_client import OpenAQClient


@dag(
    dag_id="nairobi_air_quality",
    description="A DAG to fetch and process Nairobi air quality data",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["air_quality", "nairobi"],
)
def nairobi_air_quality_pipeline():
    @task
    def extract_locations():
        """
        Extracts the list of locations in Nairobi for which air quality data is available.
        """
        connection = BaseHook.get_connection("openaq_api")
        api_key = connection.extra_dejson.get("api_key")

        client = OpenAQClient(
            api_key=api_key,
            host=connection.host,
        )
        locations = client.get_locations()

        simplified_locations = [
            {
                "location_id": loc.get("id"),
                "name": loc.get("name"),
            }
            for loc in locations
        ]

        print(f"Extracted {len(simplified_locations)} locations.")

        return simplified_locations

    # ---------Extract Sensors Task---------

    @task
    def extract_sensors(location: dict) -> list[dict]:
        """
        Extract the sensors available.
        """

        connection = BaseHook.get_connection("openaq_api")

        client = OpenAQClient(
            api_key=connection.extra_dejson["api_key"],
            host=connection.host,
        )

        sensors = client.get_sensors(location_id=location["location_id"])

        normalized_sensors = [
            {
                "sensor_id": sensor["id"],
                "location_id": location["location_id"],
                "location_name": location["name"],
                "parameter": sensor["parameter"]["name"],
                "unit": sensor["parameter"]["units"],
            }
            for sensor in sensors
        ]

        print(f"Location: {location['name']} " f"has {len(sensors)} sensors")

        return normalized_sensors

    @task
    def flatten_sensors(sensors_groups: list[list[dict]]) -> list[dict]:
        """
        Flattens a list of lists of sensors into a single list of sensors.
        """
        flattened = [sensor for group in sensors_groups for sensor in group]
        print(f"Flattened to {len(flattened)} sensors.")
        return flattened

    @task(
        max_active_tis_per_dag=5,
    )
    def extract_measurements(sensor: dict) -> list[dict]:
        """
        Extracts the air quality measurements from each sensor.
        """
        connection = BaseHook.get_connection("openaq_api")
        api_key = connection.extra_dejson.get("api_key")

        client = OpenAQClient(
            api_key=api_key,
            host=connection.host,
        )
        measurements = client.get_measurements(
            sensor_id=sensor["sensor_id"],
            limit=100,
        )

        normalized_measurements = [
            {
                "location_id": sensor["location_id"],
                "location_name": sensor["location_name"],
                "sensor_id": sensor["sensor_id"],
                "parameter": sensor["parameter"],
                "unit": sensor["unit"],
                "value": measurement["value"],
                "measurement_timestamp": measurement["period"]["datetimeFrom"]["utc"],
            }
            for measurement in measurements
        ]

        print(
            f"Sensor {sensor['sensor_id']} "
            f"({sensor['parameter']}) returned "
            f"{len(measurements)} measurements."
        )

        return normalized_measurements

    @task
    def flatten_measurements(
        measurements_groups: list[list[dict]],
    ) -> list[dict]:
        """
        Flattens a list of lists of measurements into a single list of measurements.

        """
        flattened = [
            measurement for group in measurements_groups for measurement in group
        ]
        print(f"Flattened to {len(flattened)} measurements.")
        return flattened

    @task
    def validate_measurements(
        measurements: list[dict],
    ) -> list[dict]:
        valid_measurements = []
        invalid_reasons = {
            "missing_location_id": 0,
            "missing_sensor_id": 0,
            "missing_parameter": 0,
            "missing_unit": 0,
            "missing_timestamp": 0,
            "invalid_value": 0,
            "negative_value": 0,
        }

        for measurement in measurements:
            value = measurement.get("value")
            if measurement.get("location_id") is None:
                invalid_reasons["missing_location_id"] += 1
                continue

            if measurement.get("sensor_id") is None:
                invalid_reasons["missing_sensor_id"] += 1
                continue

            if not measurement.get("parameter"):
                invalid_reasons["missing_parameter"] += 1
                continue

            if not measurement.get("unit"):
                invalid_reasons["missing_unit"] += 1
                continue

            if not measurement.get("measurement_timestamp"):
                invalid_reasons["missing_timestamp"] += 1
                continue

            if not isinstance(value, (int, float)):
                invalid_reasons["invalid_value"] += 1
                continue

            if value < 0:
                invalid_reasons["negative_value"] += 1
                continue

            valid_measurements.append(measurement)

        invalid_count = sum(invalid_reasons.values())

        print(
            f"Validated measurements: "
            f"{len(valid_measurements)} valid, "
            f"{invalid_count} invalid"
        )

        print(f"Invalid reasons: {invalid_reasons}")

        return valid_measurements

    locations = extract_locations()
    sensor_group = extract_sensors.expand(location=locations)
    sensors = flatten_sensors(sensor_group)
    measurement_groups = extract_measurements.expand(sensor=sensors)
    measurements = flatten_measurements(measurement_groups)

    validated_measurements = validate_measurements(measurements)


nairobi_air_quality_pipeline()
