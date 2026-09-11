CREATE TABLE IF NOT EXISTS raw_air_quality (
    -- ---- natural key -----------------------------------------------------------------
    sensor_id              BIGINT        NOT NULL,
    measurement_timestamp  TIMESTAMPTZ   NOT NULL,

    -- ---- descriptive attributes ------------------------------------------------------
    location_id            BIGINT        NOT NULL,
    location_name          TEXT          NOT NULL,
    parameter              TEXT          NOT NULL,   -- pm25, pm10, no2, co, o3, so2 ...
    unit                   TEXT,                     -- µg/m³, ppm, ...
    value                  DOUBLE PRECISION NOT NULL,

    -- ---- warehouse bookkeeping -------------------------------------------------------
    -- When this row was (re)written by the pipeline. Distinct from measurement_timestamp,
    -- which is when the *air* was sampled. Lets you audit late-arriving/corrected data.
    loaded_timestamp       TIMESTAMPTZ   NOT NULL DEFAULT now(),

    CONSTRAINT pk_raw_air_quality PRIMARY KEY (sensor_id, measurement_timestamp)
);