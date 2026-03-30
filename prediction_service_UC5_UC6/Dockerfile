FROM python:3.10-slim

WORKDIR /app

# Copiar dataset_service como dependência
COPY ../dataset_service /app/dataset_service
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Criar diretório para cache
RUN mkdir -p /app/cache

EXPOSE 8002

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]