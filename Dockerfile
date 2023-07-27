# Start from the Airflow image
FROM apache/airflow:2.5.1

# Change to the root user to install packages
USER root

# Install necessary packages and PostgreSQL client (libpq)
RUN apt-get update && \
    apt-get install -y wget build-essential libssl-dev libreadline-dev zlib1g-dev && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" >> /etc/apt/sources.list.d/pgdg.list' && \
    apt-get update && \
    apt-get -y -q install postgresql-client-13 libpq-dev

# Copy your requirements file into the Docker container
COPY requirements.txt /requirements.txt

# Set environment variable for additional requirements
ARG _PIP_ADDITIONAL_REQUIREMENTS

# Change back to the airflow user
USER airflow

# Install the Python packages
RUN pip install -r /requirements.txt
# RUN if [ -n "$_PIP_ADDITIONAL_REQUIREMENTS" ]; then pip install $_PIP_ADDITIONAL_REQUIREMENTS; fi

