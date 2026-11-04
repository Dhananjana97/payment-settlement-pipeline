FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY services/requirements.txt /app/services/requirements.txt
RUN pip install -r /app/services/requirements.txt

COPY services /app/services

# Default command is overridden per service in docker-compose.yml
CMD ["python", "-m", "services.api.main"]
