#!/bin/bash
# apply-circuit-breakers.sh - Aplicar todas as configurações

echo " A aplicar Circuit Breakers..."

# Aplicar todas as DestinationRules
kubectl apply -f ../kubernetes/circuit-breakers/cb-api-gateway.yaml
kubectl apply -f ../kubernetes/circuit-breakers/cb-data-service-uc4.yaml
kubectl apply -f ../kubernetes/circuit-breakers/cb-data-service-uc123.yaml
kubectl apply -f ../kubernetes/circuit-breakers/cb-data-service-uc8-uc11.yaml
kubectl apply -f ../kubernetes/circuit-breakers/cb-prediction-uc5-uc6.yaml
kubectl apply -f ../kubernetes/circuit-breakers/cb-prediction-uc9-uc10.yaml
kubectl apply -f ../kubernetes/circuit-breakers/cb-route-service-uc7.yaml

echo " Circuit Breakers aplicados!"
echo ""
echo "Para verificar:"
echo "kubectl get destinationrules -n rota-maravilhosa"