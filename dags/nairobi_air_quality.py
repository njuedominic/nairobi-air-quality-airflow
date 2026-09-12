from datetime import datetime, timedelta, timezone
from airflow import logging
from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from nairobi_air_quality_airflow.api.openaq_client import OpenAQClient
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import get_current_context


def _coerce_utc_datetime(value):
    """Normalize Airflow and stdlib datetimes to UTC-aware datetimes."""
    if hasattr(value, "in_timezone"):
        return value.in_timezone(timezone.utc)

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def _get_measurement_datetime_window(
    data_interval_start,
    data_interval_end,
):
    """Return a daily OpenAQ datetime window."""

    start = _coerce_utc_datetime(data_interval_start)
    end = _coerce_utc_datetime(data_interval_end)

    if end <= start:
        # Manual Airflow run: use previous completed UTC day.
        end = start.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        start = end - timedelta(days=1)

    return (
        start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


@dag(
    dag_id="nairobi_air_quality",
    description="A DAG to fetch and process Nairobi air quality data",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 5,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["air_quality", "nairobi"],
)
def nairobi_air_quality_pipeline():

    # ===============================================================================
    # TASK  -- extract_locations
    # ===============================================================================

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
                "latitude": (
                    loc.get("coordinates", {}).get("latitude")
                    if loc.get("coordinates")
                    else None
                ),
                "longitude": (
                    loc.get("coordinates", {}).get("longitude")
                    if loc.get("coordinates")
                    else None
                ),
            }
            for loc in locations
        ]

        print(f"Extracted {len(simplified_locations)} locations.")

        return simplified_locations

    # ===============================================================================
    # TASK -- extract_sensors  (DYNAMICALLY MAPPED, one instance per location)
    # ===============================================================================

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
                "latitude": location["latitude"],
                "longitude": location["longitude"],
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

    # ===============================================================================
    # TASK -- extract_measurements  (DYNAMICALLY MAPPED, one instance per sensor)
    # ===============================================================================
    @task(
        max_active_tis_per_dag=5,
    )
    def extract_measurements(sensor: dict) -> list[dict]:
        """
        Extracts the air quality measurements from each sensor.
        """

        context = get_current_context()
        datetime_from, datetime_to = _get_measurement_datetime_window(
            context["data_interval_start"],
            context["data_interval_end"],
        )

        print(f"Interval start: {datetime_from}")
        print(f"Interval end:   {datetime_to}")

        connection = BaseHook.get_connection("openaq_api")
        api_key = connection.extra_dejson.get("api_key")

        client = OpenAQClient(
            api_key=api_key,
            host=connection.host,
        )
        

        measurements = client.get_measurements(
            sensor_id=sensor["sensor_id"],
            datetime_from=datetime_from,
            datetime_to=datetime_to,
            limit=1000,
        )

        normalized_measurements = [
            {
                "location_id": sensor["location_id"],
                "location_name": sensor["location_name"],
                "latitude": sensor["latitude"],
                "longitude": sensor["longitude"],
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

    # ===============================================================================
    # TASK  -- validate_measurements
    # ===============================================================================

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
            "missing_latitude": 0,
            "missing_longitude": 0,
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
            if measurement.get("latitude") is None:
                invalid_reasons["missing_latitude"] += 1
                continue
            if measurement.get("longitude") is None:
                invalid_reasons["missing_longitude"] += 1
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

    # ===============================================================================
    # TASK 6 -- load_to_postgres
    # ===============================================================================
    @task
    def load_measurements(measurements: list[dict]) -> None:
        """
        Idempotent bulk upsert into the warehouse via a PostgresHook."""

        if not measurements:
            print("No measurements to load.")
            return


        postgres_hook = PostgresHook(
            postgres_conn_id="postgres_default"
        )

        sql = """
        INSERT INTO air_quality_measurements (
        location_id,
        location_name,
        latitude,
        longitude,
        geom,
        sensor_id,
        parameter,
        unit,
        value,
        measurement_timestamp
        )
        VALUES (
        %(location_id)s,
        %(location_name)s,
        %(latitude)s,
        %(longitude)s,
        ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326),
        %(sensor_id)s,
        %(parameter)s,
        %(unit)s,
        %(value)s,
        %(measurement_timestamp)s
        )
        ON CONFLICT (
        sensor_id,
        parameter,
        measurement_timestamp
        )
        
        DO UPDATE SET

        location_id = EXCLUDED.location_id,
        location_name = EXCLUDED.location_name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        geom = EXCLUDED.geom,
        unit = EXCLUDED.unit,
        value = EXCLUDED.value,
        updated_at = NOW();
        
        """

        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.executemany(
                sql,
                measurements
            )

            conn.commit()

            print(
                f"Loaded/upserted "
                f"{len(measurements)} measurements."
                )

        except Exception:
            conn.rollback()
            logging.exception(
                "Failed to load measurements."
                )
            raise

        finally:
            cursor.close()
            conn.close()

        print(f"Loading {len(measurements)} measurements into the database.")

    # ------------------------------------

    @task
    def upsert_dimensions() -> None:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        upsert_dim_location_sql = """
        INSERT INTO dim_location (
        location_id,
        location_name,
        latitude,
        longitude,
        geom
        )

        SELECT DISTINCT ON (location_id)
        location_id,
        location_name,
        latitude,
        longitude,
        geom

        FROM air_quality_measurements
        WHERE location_id IS NOT NULL
        ORDER BY location_id, updated_at DESC
        ON CONFLICT (location_id)
        DO UPDATE SET
        location_name = EXCLUDED.location_name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        geom = EXCLUDED.geom,
        updated_at = NOW();
        """

        upsert_dim_sensor_sql = """
        INSERT INTO dim_sensor (
        sensor_id,
        location_id,
        parameter,
        unit
        )

        SELECT DISTINCT ON (sensor_id)
        sensor_id,
        location_id,
        parameter,
        unit
        FROM air_quality_measurements
        WHERE sensor_id IS NOT NULL
        ORDER BY sensor_id, updated_at DESC

        ON CONFLICT (sensor_id)
        DO UPDATE SET
        location_id = EXCLUDED.location_id,
        parameter = EXCLUDED.parameter,
        unit = EXCLUDED.unit,
        updated_at = NOW();
        """

        print("Upserting dim_location...")
        postgres_hook.run(upsert_dim_location_sql, autocommit=True)
        print("dim_location upsert complete.")

        print("Upserting dim_sensor...")
        postgres_hook.run(upsert_dim_sensor_sql, autocommit=True)
        print("dim_sensor upsert complete.")

        print("All dimensions upserted successfully.")

    # -----------------------------------

    @task
    def refresh_marts() -> None:
        """Refresh analytical materialized views after new measurements are loaded."""

        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        marts = [
            "mart_latest_air_quality",
            "mart_daily_air_quality",
            "mart_sensor_health",
            "mart_spatial_air_quality",
        ]
        for mart in marts:
            print(f"Refreshing {mart}...")
            postgres_hook.run(
                f"REFRESH MATERIALIZED VIEW {mart};",
                autocommit=True,
            )
            print(f"Refreshed {mart}.")

        print("All materialized views refreshed successfully.")

    # -----------------------------------
    # -----------------------------------

    locations = extract_locations()
    sensor_group = extract_sensors.expand(location=locations)
    sensors = flatten_sensors(sensor_group)
    measurement_groups = extract_measurements.expand(sensor=sensors)
    measurements = flatten_measurements(measurement_groups)
    validated_measurements = validate_measurements(measurements)
    load_task = load_measurements(validated_measurements)
    dimension_task = upsert_dimensions()
    refresh_task = refresh_marts()
    load_task >> dimension_task >> refresh_task


nairobi_air_quality_pipeline()
