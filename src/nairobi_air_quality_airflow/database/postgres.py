def build_measurement_rows(measurements: list[dict]) -> list[tuple]:
    """Convert measurement dicts into the database row format used by the upsert."""
    return [
        (
            measurement["location_id"],
            measurement["location_name"],
            measurement["sensor_id"],
            measurement["parameter"],
            measurement["unit"],
            measurement["value"],
            measurement["measurement_timestamp"],
        )
        for measurement in measurements
    ]


UPSERT_MEASUREMENTS_SQL = """
INSERT INTO air_quality_measurements (
    location_id,
    location_name,
    sensor_id,
    parameter,
    unit,
    value,
    measurement_timestamp
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (
    sensor_id,
    parameter,
    measurement_timestamp
)
DO UPDATE SET
    location_id = EXCLUDED.location_id,
    location_name = EXCLUDED.location_name,
    unit = EXCLUDED.unit,
    value = EXCLUDED.value,
    updated_at = NOW();
"""


def upsert_measurements(postgres_hook, measurements: list[dict]) -> int:
    """Bulk upsert provided measurements into Postgres."""
    rows = build_measurement_rows(measurements)

    if not rows:
        return 0

    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.executemany(UPSERT_MEASUREMENTS_SQL, rows)
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
