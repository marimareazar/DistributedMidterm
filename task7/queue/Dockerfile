FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir 'pika>=1.3,<2' 'httpx>=0.27,<1'
COPY task7/queue/*.py /app/
CMD ["python","-u","worker.py"]
