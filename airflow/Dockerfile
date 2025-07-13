FROM apache/airflow:3.0.0

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools wheel

COPY requirements.txt .

RUN pip install -r requirements.txt