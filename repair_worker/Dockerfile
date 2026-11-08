FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir "httpx>=0.27,<1.0"
COPY repair_worker/main.py /app/main.py
CMD ["python", "-u", "main.py"]
