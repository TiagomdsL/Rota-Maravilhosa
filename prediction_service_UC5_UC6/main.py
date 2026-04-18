from datetime import datetime  
import os
from fastapi import FastAPI, HTTPException
import httpx
from pydantic import BaseModel
from dataset_service.repository import get_dataset
from dataset_service.loader import load_dataset

app = FastAPI(title="Prediction Service")
DATA_SERVICE_URL = os.getenv("DATA_SERVICE_URL", "http://localhost:8001")

# Modelo de entrada
class SeverityInput(BaseModel):
    visibility: float
    precipitation: float
    weather_condition: str

class RiskRequest(BaseModel):
    latitude: float
    longitude: float
    timestamp: datetime
    
# Carregar dataset no startup
@app.get("/health")
async def health():
    return {"gateway": "ok"}

@app.on_event("startup")
def startup_event():
    dataset_path = os.path.join("dataset", "US_Accidents_March23.csv")
    load_dataset(dataset_path, max_rows=500000)

@app.post("/predict-severity")
def predict_severity(input_data: SeverityInput):
    """
    Calcula severidade média com base em condições meteorológicas
    """
    df = get_dataset()  # obtém dataset do repository

    # Pequeno intervalo para float
    vis_range = 0.1
    prec_range = 0.01

    # Filtra dataset por condições próximas
    filtered = df[
        (df["visibility"].between(input_data.visibility - vis_range, input_data.visibility + vis_range)) &
        (df["precipitation"].between(input_data.precipitation - prec_range, input_data.precipitation + prec_range)) &
        (df["weather_condition"].str.lower() == input_data.weather_condition.lower())
    ]

    if filtered.empty:
        # Sem histórico próximo, retorna média global
        predicted_severity = df["severity"].mean()
    else:
        predicted_severity = filtered["severity"].mean()

    return {"predicted_severity": round(predicted_severity, 2)}

@app.post("/risk/score")
async def calculate_risk_score(request: RiskRequest):

    # 1. Buscar features históricas ao data_service
    async with httpx.AsyncClient() as http_client:
        try:
            response = await http_client.post(
                f"{DATA_SERVICE_URL}/features",
                json={
                    "latitude": request.latitude,
                    "longitude": request.longitude,
                    "timestamp": request.timestamp.isoformat(),
                }
            )
            response.raise_for_status()
            features = response.json()
        except httpx.HTTPStatusError:
            # data_service não encontrou dados → assume sem histórico
            features = {
                "total_accidents": 0,
                "avg_severity": 0.0,
                "avg_visibility": None,
                "avg_precipitation": None,
                "weather_distribution": {}
            }
        except httpx.HTTPError as e:
            # data_service inacessível → erro real de infraestrutura
            raise HTTPException(status_code=502, detail=f"Data service unavailable: {str(e)}")

    total_accidents   = features["total_accidents"]
    avg_severity      = features["avg_severity"]
    avg_visibility    = features.get("avg_visibility")
    avg_precipitation = features.get("avg_precipitation")

    # 2. Calcular accident_probability
    ACCIDENT_THRESHOLD = 10
    accident_probability = min(total_accidents / ACCIDENT_THRESHOLD, 1.0)

    if avg_visibility is not None and avg_visibility < 5.0:
        accident_probability = min(accident_probability * 1.3, 1.0)

    if avg_precipitation is not None and avg_precipitation > 0.1:
        accident_probability = min(accident_probability * 1.2, 1.0)

    # 3. Calcular predicted_severity (1–4 como no dataset)
    if total_accidents == 0 or avg_severity == 0:
        predicted_severity = 1
    else:
        predicted_severity = max(1, min(4, round(avg_severity)))

    return {
        "accident_probability": round(accident_probability, 4),
        "predicted_severity": predicted_severity,
        "nearby_accidents_count": total_accidents
    }