FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY backend/pyproject.toml /app/pyproject.toml
COPY backend/app /app/app
COPY backend/alembic.ini /app/alembic.ini
COPY backend/alembic /app/alembic
COPY backend/scripts /app/scripts
COPY data /app/data

RUN pip install --no-cache-dir .
RUN chmod +x /app/scripts/*.sh

EXPOSE 8000

CMD ["/app/scripts/start_api.sh"]
