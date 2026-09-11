from types import SimpleNamespace
from unittest.mock import Mock

from nairobi_air_quality_airflow.database.postgres import (
    UPSERT_MEASUREMENTS_SQL,
    build_measurement_rows,
    upsert_measurements,
)


def test_build_measurement_rows_converts_dicts_to_database_tuples():
    measurements = [
        {
            "location_id": 1,
            "location_name": "Nairobi Central",
            "sensor_id": 101,
            "parameter": "pm25",
            "unit": "ug/m3",
            "value": 12.5,
            "measurement_timestamp": "2026-09-11T12:00:00Z",
        }
    ]

    rows = build_measurement_rows(measurements)

    assert rows == [
        (
            1,
            "Nairobi Central",
            101,
            "pm25",
            "ug/m3",
            12.5,
            "2026-09-11T12:00:00Z",
        )
    ]


def test_upsert_measurements_uses_bulk_sql_and_returns_row_count():
    hook = Mock()
    conn = Mock()
    cursor = conn.cursor.return_value
    hook.get_conn.return_value = conn

    measurements = [
        {
            "location_id": 1,
            "location_name": "Nairobi Central",
            "sensor_id": 101,
            "parameter": "pm25",
            "unit": "ug/m3",
            "value": 12.5,
            "measurement_timestamp": "2026-09-11T12:00:00Z",
        },
        {
            "location_id": 2,
            "location_name": "Westlands",
            "sensor_id": 102,
            "parameter": "pm10",
            "unit": "ug/m3",
            "value": 8.0,
            "measurement_timestamp": "2026-09-11T12:05:00Z",
        },
    ]

    row_count = upsert_measurements(hook, measurements)

    assert row_count == 2
    cursor.executemany.assert_called_once_with(UPSERT_MEASUREMENTS_SQL, [
        (
            1,
            "Nairobi Central",
            101,
            "pm25",
            "ug/m3",
            12.5,
            "2026-09-11T12:00:00Z",
        ),
        (
            2,
            "Westlands",
            102,
            "pm10",
            "ug/m3",
            8.0,
            "2026-09-11T12:05:00Z",
        ),
    ])
    conn.commit.assert_called_once()
    cursor.close.assert_called_once()
    conn.close.assert_called_once()


def test_upsert_measurements_returns_zero_for_empty_input():
    hook = Mock()

    row_count = upsert_measurements(hook, [])

    assert row_count == 0
    hook.get_conn.assert_not_called()
