FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY dags/ /app/dags/
ENV AIRFLOW_HOME="/app"\
    MODEL_VERSION=v1.0.0
EXPOSE 8080
CMD ["airflow", "standalone"]