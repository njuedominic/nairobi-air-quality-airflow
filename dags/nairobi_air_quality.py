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


    
    @task
    def extract_sensors(location: dict) -> list[dict]:
        """
        Extract the sensors available for one Nairobi location.
        """

        connection = BaseHook.get_connection("openaq_api")

        client = OpenAQClient(
        api_key = connection.extra_dejson["api_key"],
        host=connection.host,
        )

        sensors = client.get_sensors(
        location_id=location["location_id"]
        )

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


        print(
            f"Location: {location['name']} "f"has {len(sensors)} sensors"
            )
        
        return normalized_sensors

    @task
    def flatten_sensors(sensors_groups: list[list[dict]]) -> list[dict]:
        """
        Flattens a list of lists of sensors into a single list of sensors.
        """
        flattened = [
            sensor
            for group in sensors_groups
            for sensor in group]
        print(f"Flattened to {len(flattened)} sensors.")
        return flattened


    @task
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


    locations = extract_locations()
    sensor_group = extract_sensors.expand(
    location=locations
    )
    sensors = flatten_sensors(sensor_group)
    measurement_groups = extract_measurements.expand(
    sensor=sensors
    )




    
#-------------------------------------------------------------------------
        # def validate_measurements(measurements):
        #     '''
        #     Validates the extracted air quality measurements.'''
        #     # Placeholder for actual validation logic
        #     return[]
        # @task
        # def load_to_database(validated_measurements):
        #     '''
        #     Loads the validated air quality measurements into the database.'''
        #     print(f"Loading {len(validated_measurements)} records into the database.")
        #     # Placeholder for actual loading logic
        #     return "Data loaded successfully"
    

    # measurements = extract_measurements(sensors)
    # validated_measurements = validate_measurements(measurements)
    # load_to_database(validated_measurements)



nairobi_air_quality_pipeline()