FROM python:3.10-slim
WORKDIR /app
COPY data_service_UC8_UC11/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dataset_service/ ./dataset_service/
COPY data_service_UC8_UC11/main.py .
EXPOSE 8007
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8007"]
