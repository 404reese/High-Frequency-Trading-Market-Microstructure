FROM apache/spark:3.5.0
USER root

RUN pip install --no-cache-dir pandas numpy pyarrow

USER spark

WORKDIR /app