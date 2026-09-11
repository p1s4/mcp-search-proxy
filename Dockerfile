FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY server.py /app/server.py

ENV UPSTREAM_URLS=http://upstream1:4000/mcp \
    MANIFEST_PATH=/app/data/manifest.json \
    PORT=8092 \
    CATALOG_TTL_SEC=1800 \
    LOG_LEVEL=INFO

VOLUME ["/app/data"]
EXPOSE 8092

HEALTHCHECK --interval=60s --timeout=10s --retries=3 --start-period=30s \
  CMD python -c "import socket;s=socket.create_connection(('127.0.0.1',8092),timeout=5);s.close()" || exit 1

CMD ["python", "/app/server.py"]
