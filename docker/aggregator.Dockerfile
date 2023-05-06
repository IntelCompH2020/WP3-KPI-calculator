FROM python:3.8-slim-buster

ENV PYTHONPATH="/src"

WORKDIR /src
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /src

ENTRYPOINT ["python", "aggregator/Climate_EU.py"]