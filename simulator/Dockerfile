FROM python:3.11-slim-bookworm
RUN pip install --no-cache-dir --default-timeout=600 --retries 10 \
    confluent-kafka==2.16.0 fastavro==1.13.1 faker==40.43.0 pyyaml==6.0.3
WORKDIR /opt/tracewell/simulator
ENV PYTHONUNBUFFERED=1
CMD ["python", "simulator.py"]
