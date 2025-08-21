FROM apache/airflow:2.7.0

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy scripts and DAGs
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/
COPY --chown=airflow:root dags/ /opt/airflow/dags/