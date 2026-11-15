FROM quay.io/jupyter/pyspark-notebook@sha256:7e102d041c42cca6e0d32269cded10fad2c377aeb09fad1ec546840660d691c4
RUN pip install --no-cache-dir confluent-kafka==2.16.1
COPY src/wiki_producer.py /work/wiki_producer.py
ENTRYPOINT ["python", "-u", "/work/wiki_producer.py"]
