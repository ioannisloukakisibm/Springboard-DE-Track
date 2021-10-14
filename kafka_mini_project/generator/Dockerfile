# Dockerfile
FROM python:3.6
WORKDIR /usr/app
# ADD ./requirements.txt ./
ADD ./kafka_mini_project/generator/requirements.txt ./
RUN pip install -r requirements.txt
ADD ./ ./
CMD ["python", "app.py"]
