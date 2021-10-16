FROM puckel/docker-airflow:1.10.1

COPY ./mnt/airflow/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

COPY ./mnt/requirements.txt /requirements.txt
RUN pip install -r /requirements.txt