def validate_measurements(measurements: list[dict]) -> list[dict]:
    """Return only valid measurements and ignore malformed records."""
    valid_measurements: list[dict] = []
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

    return valid_measurements
