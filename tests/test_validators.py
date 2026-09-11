from nairobi_air_quality_airflow.validation.validators import validate_measurements


def test_validate_measurements_keeps_valid_and_skips_invalid():
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
            "value": -3,
            "measurement_timestamp": "2026-09-11T12:05:00Z",
        },
        {
            "location_id": 3,
            "location_name": "Kilimani",
            "sensor_id": 103,
            "parameter": "",
            "unit": "ug/m3",
            "value": 20,
            "measurement_timestamp": "2026-09-11T12:10:00Z",
        },
    ]

    valid_measurements = validate_measurements(measurements)

    assert len(valid_measurements) == 1
    assert valid_measurements[0]["sensor_id"] == 101


def test_validate_measurements_accepts_empty_input():
    assert validate_measurements([]) == []
