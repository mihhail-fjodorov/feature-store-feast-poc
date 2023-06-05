FROM python:3.10-slim

RUN apt-get update -qq \
    && apt-get dist-upgrade -y \
    && apt-get install libpq-dev python-dev -y \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
COPY src/transfer_job/transfer.py .

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3", "transfer.py"]
