FROM apache/airflow:2.4.3-python3.9
USER root
RUN apt-get update                             \
 && apt-get install -y --no-install-recommends \
 libpq-dev default-libmysqlclient-dev pkg-config build-essential           \
 && rm -fr /var/lib/apt/lists/*                \
 &&  update-ca-certificates

WORKDIR ${AIRFLOW_HOME}
COPY plugins/ plugins/
COPY requirements.txt .
USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
