import os

import psycopg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

app = FastAPI(
    title="Nairobi Air Quality API",
    version="0.1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:3000",
        "http://localhost:3000",
        "https://nairobi-air-quality-airflow.onrender.com",
        "https://nairobi-air-quality-map.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

def get_connection():
    return psycopg.connect(os.getenv("DATABASE_URL"))


@app.get("/config")
def runtime_config():
    return {
        "mapbox_access_token": os.getenv("MAPBOX_ACCESS_TOKEN", ""),
    }


@app.get("/health")
def health():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()

        return {
            "status": "ok",
            "database": "reachable",
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Database connection failed: {exc}",
        ) from exc


@app.get("/air-quality")
def air_quality(
    parameter: str | None = Query(
        default=None,
        description="Optional parameter, e.g. pm25, pm1, temperature",
    ),
    status: str | None = Query(
        default=None,
        description="Optional data status: healthy, stale, inactive",
    ),
):
    conditions = []
    params = {}

    if parameter is not None:
        conditions.append("parameter = %(parameter)s")
        params["parameter"] = parameter

    if status is not None:
        conditions.append("data_status = %(status)s")
        params["status"] = status

    where_clause = ""
    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)

    sql = f"""
    SELECT jsonb_build_object(
        'type', 'FeatureCollection',
        'features', COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'type', 'Feature',
                    'geometry',
                    ST_AsGeoJSON(geom::geometry)::jsonb,
                    'properties',
                    jsonb_build_object(
                        'location_id', location_id,
                        'location_name', location_name,
                        'parameter', parameter,
                        'unit', unit,
                        'latest_value', latest_value,
                        'avg_24h', avg_24h,
                        'measurements_24h', measurements_24h,
                        'sensor_count', sensor_count,
                        'data_status', data_status,
                        'latest_measurement_at',
                        latest_measurement_at
                    )
                )
            ),
            '[]'::jsonb
        )
    )
    FROM mart_spatial_air_quality
    {where_clause}
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()

        return row[0] if row else {
            "type": "FeatureCollection",
            "features": [],
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch air quality data: {exc}",
        ) from exc
    