FROM python:3.10-slim

RUN apt-get update -qq \
    && apt-get dist-upgrade -y \
    && apt-get install libpq-dev python-dev -y \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install psycopg2-binary

COPY src/customer/customer.py .

ENTRYPOINT ["python3", "customer.py"]
