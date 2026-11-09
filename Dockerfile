FROM apache/airflow:3.4.0-python3.12

# The pipeline code (extraction + site) and the dbt adapter run inside the Airflow workers.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
