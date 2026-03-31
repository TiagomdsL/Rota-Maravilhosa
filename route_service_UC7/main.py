# route_service/main.py
import os
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Route Service")

PREDICTION_SERVICE_URL = os.getenv("PREDICTION_SERVICE_URL", "http://localhost:8002")

# Número de pontos intermédios entre origem e destino
NUM_WAYPOINTS = 5

class RouteRequest(BaseModel):
    origin_lat: float
    origin_lon: float
    destination_lat: float
    destination_lon: float
    timestamp: str = "2016-02-08T08:00:00"  

def interpolate_waypoints(
    origin_lat: float, origin_lon: float,
    destination_lat: float, destination_lon: float,
    n: int = NUM_WAYPOINTS
) -> list[dict]:
    """
    Gera n pontos intermédios entre origem e destino por interpolação linear.
    Inclui origem e destino.
    Futuro: trocar por Google Routes API para waypoints reais.
    """
    waypoints = []
    for i in range(n + 2):  
        t = i / (n + 1)
        waypoints.append({
            "latitude": round(origin_lat + t * (destination_lat - origin_lat), 6),
            "longitude": round(origin_lon + t * (destination_lon - origin_lon), 6),
        })
    return waypoints


async def get_risk_for_waypoint(
    client: httpx.AsyncClient,
    lat: float,
    lon: float,
    timestamp: str
) -> dict:
    """
    Chama o prediction_service para obter o risk score de um waypoint.
    """
    try:
        response = await client.post(
            f"{PREDICTION_SERVICE_URL}/risk/score",
            json={"latitude": lat, "longitude": lon, "timestamp": timestamp}
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError:
        # Se falhar um waypoint, assume risco neutro para não bloquear a rota
        return {"accident_probability": 0.0, "predicted_severity": 1, "nearby_accidents_count": 0}


def aggregate_risk(waypoint_scores: list[dict]) -> float:
    """
    Agrega os risk scores dos waypoints numa só métrica.
    Usa média ponderada: accident_probability * predicted_severity normalizado.
    """
    if not waypoint_scores:
        return 0.0

    scores = [
        wp["accident_probability"] * (wp["predicted_severity"] / 4)
        for wp in waypoint_scores
    ]
    return round(sum(scores) / len(scores), 4)

@app.post("/route/analyze")
async def analyze_route(request: RouteRequest):
    """
    UC7 – Route Risk Analysis.
    Gera waypoints entre origem e destino, calcula o risco de cada um,
    e devolve a rota com o risk score agregado.
    """
    waypoints = interpolate_waypoints(
        request.origin_lat, request.origin_lon,
        request.destination_lat, request.destination_lon
    )

    async with httpx.AsyncClient(timeout=10.0) as client:
        scores = []
        for wp in waypoints:
            score = await get_risk_for_waypoint(
                client, wp["latitude"], wp["longitude"], request.timestamp
            )
            scores.append({
                **wp,
                "accident_probability": score.get("accident_probability", 0.0),
                "predicted_severity": score.get("predicted_severity", 1),
                "nearby_accidents_count": score.get("nearby_accidents_count", 0),
            })

    risk_score = aggregate_risk(scores)

    return {
        "recommended_route": {
            "origin": {"latitude": request.origin_lat, "longitude": request.origin_lon},
            "destination": {"latitude": request.destination_lat, "longitude": request.destination_lon},
            "waypoints": scores,
        },
        "risk_score": risk_score,
        "risk_level": _risk_label(risk_score),
        "total_waypoints_analyzed": len(scores),
    }


def _risk_label(score: float) -> str:
    if score < 0.2:
        return "Low"
    elif score < 0.5:
        return "Medium"
    else:
        return "High"


@app.get("/health")
async def health():
    return {"status": "ok"}