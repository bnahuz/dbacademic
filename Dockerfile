FROM apache/airflow:2.5.1
COPY requirements.txt .
RUN cat requirements.txt
RUN pip install -r requirements.txt
RUN mkdir -p download