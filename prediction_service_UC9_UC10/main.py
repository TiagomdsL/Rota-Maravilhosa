# prediction_service/main.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List
import pandas as pd
import numpy as np
import os
import logging
import pickle
from collections import Counter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Accident Prediction Service", version="1.0.0")

dataset_path = os.path.join("dataset", "US_Accidents_March23.csv")
CACHE_PATH = "accident_stats.pkl"


class PredictRequest(BaseModel):
    latitude: float
    longitude: float
    hour: int = Field(..., ge=0, le=23)
    weather_condition: str

class PredictResponse(BaseModel):
    accident_probability: float
    risk_level: str

class SimulateRequest(BaseModel):
    latitude: float
    longitude: float
    hour: int = Field(..., ge=0, le=23)
    weather_condition: str
    road_topology: str

class SimulateResponse(BaseModel):
    probability_score: float
    predicted_severity: str
    explanation: List[str]

def load_stats():
    """Carrega estatisticas do cache ou cria novo"""
    
    # Tenta carregar do cache
    if os.path.exists(CACHE_PATH):
        logger.info("Carregando estatisticas do cache...")
        with open(CACHE_PATH, 'rb') as f:
            stats = pickle.load(f)
        logger.info(f"Cache carregado: {stats['total']:,} acidentes")
        return stats
    
    logger.info("Cache nao encontrado. Processando dataset...")
    
    # Le apenas uma amostra representativa
    # 500,000 linhas sao suficientes para estatisticas confiaveis
    SAMPLE_SIZE = 500000
    
    try:
        # Tenta ler apenas as primeiras N linhas
        df = pd.read_csv(
            dataset_path,
            nrows=SAMPLE_SIZE,
            usecols=['Start_Time', 'Weather_Condition', 'Junction', 'Roundabout'],
            low_memory=False
        )
        
        logger.info(f"Amostra carregada: {len(df):,} registos")
        
    except:
        # Se falhar, le o arquivo inteiro mas em chunks
        logger.info("Usando leitura em chunks...")
        df_list = []
        chunksize = 100000
        total_read = 0
        
        for chunk in pd.read_csv(
            dataset_path,
            chunksize=chunksize,
            usecols=['Start_Time', 'Weather_Condition', 'Junction', 'Roundabout'],
            low_memory=False
        ):
            df_list.append(chunk)
            total_read += len(chunk)
            if total_read >= SAMPLE_SIZE:
                break
        
        df = pd.concat(df_list, ignore_index=True)
        logger.info(f"Amostra carregada: {len(df):,} registos")
    
    # Extrai hora de forma simples
    def get_hour_simple(time_str):
        try:
            # Pega apenas a hora do timestamp
            time_str = str(time_str)
            if ' ' in time_str:
                hour_part = time_str.split(' ')[1]
                return int(hour_part.split(':')[0])
            return -1
        except:
            return -1
    
    df['hour'] = df['Start_Time'].apply(get_hour_simple)
    df = df[df['hour'] >= 0]
    
    # Limpa clima
    df['Weather_Condition'] = df['Weather_Condition'].fillna('Unknown').astype(str).str.lower().str[:20]
    
    total = len(df)
    
    # Estatisticas por hora
    hour_stats = {}
    for h in range(24):
        hour_stats[h] = len(df[df['hour'] == h]) / total if total > 0 else 0
    
    # Top 20 condicoes climaticas
    weather_counts = df['Weather_Condition'].value_counts().head(20)
    weather_stats = {}
    for w, count in weather_counts.items():
        weather_stats[w] = count / total
    
    # Estatisticas de topologias
    junction_count = len(df[df['Junction'] == True])
    roundabout_count = len(df[df['Roundabout'] == True])
    
    stats = {
        'total': total,
        'hour_stats': hour_stats,
        'weather_stats': weather_stats,
        'junction_factor': 1.0 + (junction_count / total) if total > 0 else 1.0,
        'roundabout_factor': 1.0 + (roundabout_count / total) if total > 0 else 1.0
    }
    
    # Salva cache
    with open(CACHE_PATH, 'wb') as f:
        pickle.dump(stats, f)
    
    logger.info(f"Estatisticas salvas em cache: {total:,} registos")
    logger.info(f"Hora pico: {max(hour_stats, key=hour_stats.get)}")
    logger.info(f"Clima mais comum: {max(weather_stats, key=weather_stats.get)}")
    
    return stats

# Carrega estatisticas uma vez
STATS = load_stats()



def get_hour_risk(hour):
    """Retorna risco baseado na hora"""
    return STATS['hour_stats'].get(hour, 0.04)

def get_weather_risk(weather):
    """Retorna risco baseado no clima"""
    weather_lower = weather.lower().strip()[:20]
    
    # Match exato
    if weather_lower in STATS['weather_stats']:
        return STATS['weather_stats'][weather_lower]
    
    # Match parcial
    for w, risk in STATS['weather_stats'].items():
        if weather_lower in w or w in weather_lower:
            return risk
    
    # Valor medio se nao encontrar
    return 0.04

def predict_probability(hour, weather):
    """Calcula probabilidade final"""
    hour_risk = get_hour_risk(hour)
    weather_risk = get_weather_risk(weather)
    
    # Media simples
    probability = (hour_risk + weather_risk) / 2
    
    # Normaliza
    probability = min(0.9, max(0.01, probability))
    
    return probability

def classify_risk(prob):
    """Classifica nivel de risco"""
    if prob < 0.25:
        return "Low"
    elif prob < 0.5:
        return "Medium"
    elif prob < 0.75:
        return "High"
    else:
        return "Critical"

def get_road_factor(road):
    """Fator de risco por topologia"""
    road_map = {
        'junction': STATS['junction_factor'],
        'intersection': STATS['junction_factor'],
        'crossroads': STATS['junction_factor'],
        'roundabout': STATS['roundabout_factor'],
        'circle': STATS['roundabout_factor'],
        'curve': 1.15,
        'bend': 1.15,
        'traffic_signal': 1.1,
        'straight': 0.9
    }
    
    return road_map.get(road.lower().strip(), 1.0)


@app.post("/accidents/predict-occurrence", response_model=PredictResponse)
def predict_occurrence(request: PredictRequest):
    """Prediz probabilidade de acidente"""
    try:
        prob = predict_probability(request.hour, request.weather_condition)
        risk = classify_risk(prob)
        
        return PredictResponse(
            accident_probability=round(prob, 4),
            risk_level=risk
        )
    except Exception as e:
        logger.error(f"Erro: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/accidents/simulate-risk", response_model=SimulateResponse)
def simulate_risk(request: SimulateRequest):
    """Simula risco com topologia"""
    try:
        # Probabilidade base
        base_prob = predict_probability(request.hour, request.weather_condition)
        
        # Aplica fator da via
        road_factor = get_road_factor(request.road_topology)
        final_prob = min(0.95, base_prob * road_factor)
        
        severity = classify_risk(final_prob)
        
        # Gera explicacao simples
        explanation = []
        
        hour_risk = get_hour_risk(request.hour)
        if hour_risk > 0.06:
            explanation.append(f"Peak hour: {request.hour}:00 has high accident rate")
        
        weather_risk = get_weather_risk(request.weather_condition)
        if weather_risk > 0.05:
            explanation.append(f"'{request.weather_condition}' increases accident risk")
        
        if road_factor > 1.2:
            explanation.append(f"{request.road_topology} is a high-risk road feature")
        
        if final_prob > 0.7:
            explanation.append("Multiple risk factors combined")
        
        return SimulateResponse(
            probability_score=round(final_prob, 4),
            predicted_severity=severity,
            explanation=explanation
        )
    
    except Exception as e:
        logger.error(f"Erro: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
def get_stats():
    """Retorna estatisticas"""
    return {
        "total_accidents_sampled": STATS['total'],
        "top_hours": sorted(STATS['hour_stats'].items(), key=lambda x: x[1], reverse=True)[:5],
        "top_weather": sorted(STATS['weather_stats'].items(), key=lambda x: x[1], reverse=True)[:10],
        "junction_factor": round(STATS['junction_factor'], 2),
        "roundabout_factor": round(STATS['roundabout_factor'], 2)
    }


@app.get("/health")
def health():
    return {"status": "healthy", "samples": STATS['total']}


@app.post("/cache/clear")
def clear_cache():
    """Limpa cache para recarregar dados"""
    global STATS
    if os.path.exists(CACHE_PATH):
        os.remove(CACHE_PATH)
    STATS = load_stats()
    return {"message": "Cache cleared and reloaded"}