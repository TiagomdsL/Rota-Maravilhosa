FROM python:3.10-slim
WORKDIR /app
COPY prediction_service_UC5_UC6/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dataset_service/ ./dataset_service/
COPY prediction_service_UC5_UC6/main.py .
RUN mkdir -p /app/cache
EXPOSE 8002
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
