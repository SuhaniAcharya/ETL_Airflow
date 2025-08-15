from airflow import DAG # DAG is a Directed Acyclic Graph
from airflow.providers.http.hooks.http import HttpHook # HttpHook is used to make HTTP requests in Airflow
from airflow.providers.postgres.hooks.postgres import PostgresHook # PostgresHook is used to interact with PostgreSQL databases
from airflow.decorators import task # task decorator is used to define tasks in Airflow
from airflow.utils.dates import days_ago # days_ago is a utility function to get dates relative to the current date

# Lat and Lon for desired location, LONDON in this case
Latitude = '51.5074'
Longitude = '-0.1278'

# Postgres Connection ID
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

# Creating DAG
with DAG(dag_id = 'weather_etl_pipeline', 
        default_args = default_args,
        schedule_interval = '@daily',
        catchup = False) as dag:

    @task()
    def extract_weather_data():
        """Extract weather data from Open Meteo API using Airflow Connection"""
        # Use HTTP hook to get weather data
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method = 'GET')
        
        # Build API endpoint
        # url = https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f"/v1/forecast?latitude={Latitude}&longitude={Longitude}&current_weather=true"
        
        # Make request via HTTP hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            return {"error": "Failed to fetch weather data"}

    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted weather data into a format suitable for loading into PostgreSQL"""
        current_weather = weather_data['current_weather']
        
        transformed_data = {
            'latitude': Latitude,
            'longitude': Longitude,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
        }
        
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        """Load the transformed weather data into PostgreSQL"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesnt exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT
            );
        """)
        
        # Insert transformed data into the table
        cursor.execute("""
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (transformed_data['latitude'], transformed_data['longitude'], 
              transformed_data['temperature'], transformed_data['windspeed'], 
              transformed_data['winddirection'], transformed_data['weathercode']
              ))
        
        conn.commit()
        cursor.close()
        
    # DAG Worflow
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)

# Before running we need to make another file for the Airflow Docker Compose setup, docker-compose.yml

# To run we write following in terminal:
# astro dev start # Will run airflow in package container

# after that command we will see new container in docker desktop