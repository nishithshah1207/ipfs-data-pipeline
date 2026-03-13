FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
COPY pipeline_client/ pipeline_client/

RUN pip install --no-cache-dir -e ".[benchmark]"

ENTRYPOINT ["pipeline"]
