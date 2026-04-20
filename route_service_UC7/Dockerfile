FROM python:3.10-slim
WORKDIR /app
COPY route_service_UC7/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY route_service_UC7/main.py .
EXPOSE 8003
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8003"]
