# Nairobi Air Quality 

<!-- 
CREATE TABLE IF NOT EXISTS raw_air_quality (
    location_id INT,
    location_name VARCHAR(255),
    sensor_id INT,
    parameter VARCHAR(50),
    unit VARCHAR(50),
    value NUMERIC,
    measurement_timestamp TIMESTAMP WITH TIME ZONE,
    loaded_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- The composite primary key prevents duplicate entries on pipeline reruns
    PRIMARY KEY (sensor_id, measurement_timestamp)
); -->


