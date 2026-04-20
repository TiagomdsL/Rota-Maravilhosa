FROM python:3.10-slim
WORKDIR /app
COPY dataset_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dataset_service/ ./dataset_service/
EXPOSE 8005
CMD ["uvicorn", "dataset_service.main:app", "--host", "0.0.0.0", "--port", "8005"]
