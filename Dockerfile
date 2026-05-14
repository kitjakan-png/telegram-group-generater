FROM python:3.11-slim

WORKDIR /app

# ติดตั้ง dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# คัดลอก source code
COPY webapp_cloud.py .

# Cloud Run ใช้ PORT environment variable
ENV PORT=8080

CMD uvicorn webapp_cloud:app --host 0.0.0.0 --port $PORT
