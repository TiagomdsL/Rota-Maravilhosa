FROM python:3.10-slim
WORKDIR /app
COPY prediction_service_UC9_UC10/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dataset_service/ ./dataset_service/
COPY prediction_service_UC9_UC10/main.py .
EXPOSE 8008
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8008"]
