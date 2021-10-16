FROM puckel/docker-airflow:1.10.1 as builder

ARG SPARK_VERSION=2.4.7
ARG HADOOP_VERSION=2.7
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ARG SPARK_HOME=/usr/spark-${SPARK_VERSION}
RUN curl -sL --retry 3 \
  "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
  | tar xz -C /tmp/ \
 && mv /tmp/$SPARK_PACKAGE $SPARK_HOME \
 && chown -R root:root $SPARK_HOME


COPY ./airflow/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
RUN pip install -U pip
RUN pip install poetry

RUN mkdir /home/spark
WORKDIR /home/spark

# Get the dependencies for the project locally
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.in-project true && poetry install --no-root

FROM openjdk:8

ARG SPARK_VERSION=2.4.7
ARG HADOOP_VERSION=2.7
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ARG SPARK_HOME=/usr/spark-${SPARK_VERSION}

ENV PYTHON_DEPENDENCY=python3.7
RUN apt-get update && \
    apt-get install -y --no-install-recommends ${PYTHON_DEPENDENCY} && \
    apt-get clean

ENV SPARK_HOME=/usr/spark-${SPARK_VERSION}
ENV PYTHONPATH /home/spark/.venv

COPY --from=builder /usr/spark-${SPARK_VERSION}/ /usr/spark-${SPARK_VERSION}

RUN adduser --home /home/spark --disabled-password spark

USER spark
WORKDIR /home/spark

COPY --from=builder --chown=spark:spark /home/spark/.venv /home/spark/.venv

# This deals with the fact that `apt-get install` up there installs
# python to /usr/bin/python3.7, which is different than the location
# of python3 in python:3.7-buster
RUN rm /home/spark/.venv/bin/python
RUN ln -s /usr/bin/python3.7 /home/spark/.venv/bin/python

COPY adding.py test_add.py ./
ENV PATH=${PYTHONPATH}/bin:${SPARK_HOME}/bin:${PATH}
CMD ["pytest"]