FROM apache/spark:3.5.5

USER root

COPY streaming_job.py /opt/spark/work-dir/streaming_job.py

USER 185

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5", "/opt/spark/work-dir/streaming_job.py"]
