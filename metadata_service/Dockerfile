FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY metadata_service /app/metadata_service
COPY shared /app/shared
EXPOSE 8000
CMD ["uvicorn", "metadata_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
