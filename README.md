This project runs Apache Airflow inside Docker, with PostgreSQL as the metadata database. It mounts your dags/ folder so your ETL script (e.g., ETLWeather.py) runs inside the Airflow environment.

Prerequisites:
  Install Docker Desktop 
  Ensure Docker Daemon is running

Initialize an Airflow project (inside your new folder in VS Code)
  Open VS Code’s terminal in your folder and run one of this for blank project: astro dev init
  (Creates the standard DAGs/plugins/dependencies structure.) 

Write your ETL code
You will find my code in AirflowETL -> dags -> __pychache__ -> ETLWeather.py

Make docker-compose.yml file before running project

Run Airflow locally:
  From your project directory: astro dev start
