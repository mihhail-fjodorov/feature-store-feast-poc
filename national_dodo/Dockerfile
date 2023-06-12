FROM python:3.10-slim

RUN apt-get update -qq \
    && apt-get dist-upgrade -y \
    && apt-get install libpq-dev python-dev -y \
    && rm -rf /var/lib/apt/lists/*

RUN pip install "feast[redis]"
RUN pip3 install psycopg2-binary

COPY feature_repo/feature_store.yaml feature_store.yaml

# Needed to reach online store within Docker network.
RUN sed -i 's/localhost:6379/feast-online-store-1:6379/g' feature_store.yaml
RUN sed -i 's/0.0.0.0:5433/feast-registry-1:5432/g' feature_store.yaml
ENV FEAST_USAGE=False

CMD ["feast", "serve", "-h", "0.0.0.0", "-p", "6566"]
