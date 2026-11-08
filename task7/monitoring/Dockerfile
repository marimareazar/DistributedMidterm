FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir 'fastapi>=0.110,<1' 'uvicorn>=0.29,<1' 'httpx>=0.27,<1' 'prometheus-client>=0.20,<1' 'psycopg[binary]>=3.1,<4'
COPY task7/monitoring/metrics.py /app/metrics.py
CMD ["uvicorn","metrics:app","--host","0.0.0.0","--port","9109"]
