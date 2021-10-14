# Base Image
# pulling a python image from the dockehub
FROM python:3.8-slim
LABEL maintainer="MarcLamberti"

# Arguments that can be set with docker build
ARG AIRFLOW_VERSION=2.0.1
# This is a container path. All the ariflow related artifacts will be stored here
ARG AIRFLOW_HOME=/opt/airflow

# Export the environment variable AIRFLOW_HOME where airflow will be installed
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Install dependencies and tools
# Each RUN command in a Dockerfile creates a new layer to the Docker image. 
# The '&& 's in the RUN command exist so that all the shell commands will take place in a single layer.
RUN apt-get update -y && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \
    python3-dev \
    wget \
    libczmq-dev \
    curl \
    libssl-dev \
    git \
    inetutils-telnet \
    bind9utils freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libffi-dev libpq-dev \
    freetds-bin build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    rsync \
    zip \
    unzip \
    gcc \
    vim \
    netcat \
    && apt-get autoremove -yqq --purge && apt-get clean

# It will copy from this path (local) into the container
COPY ./requirements-python3.8.txt /requirements-python3.8.txt

# Upgrade pip
# Create airflow user 
# Install apache airflow with subpackages
RUN pip install --upgrade "pip==20.2.4" && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    pip install apache-airflow==${AIRFLOW_VERSION} --constraint ./requirements-python3.8.txt && \
    pip install yfinance
    # pip install apache-airflow-providers-apache-hdfs==1.0.1 && \
    # pip install apache-airflow-providers-apache-hive==1.0.2 && \
    # pip install apache-airflow-providers-apache-spark==1.0.2 && \
    # pip install apache-airflow-providers-slack==3.0.0 && \
    # pip install apache-airflow-providers-http==1.1.1 && 

# Copy the airflow.cfg file (config)
# COPY ./config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# Set the owner of the files in AIRFLOW_HOME to the user airflow
RUN chown -R airflow: ${AIRFLOW_HOME}

# Copy the entrypoint.sh from host to container (at path AIRFLOW_HOME)
COPY ./start-airflow.sh ./start-airflow.sh

# Set the entrypoint.sh file to be executable
RUN chmod +x ./start-airflow.sh

# Set the username to use
USER airflow

# Create the folder dags inside $AIRFLOW_HOME
# This directory is created in the container
RUN mkdir -p ${AIRFLOW_HOME}/dags

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080

# Execute start-airflow.sh
# This command will get the container up and running
CMD [ "./start-airflow.sh" ]

    