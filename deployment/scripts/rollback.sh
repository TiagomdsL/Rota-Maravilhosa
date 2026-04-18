#!/bin/bash

set -e

NAMESPACE="rota-maravilhosa"
DEPLOYMENT=$1

if [ -z "$DEPLOYMENT" ]; then
    echo "Uso: ./03-rollback.sh <deployment-name>"
    echo ""
    echo "Exemplos:"
    echo "  ./03-rollback.sh api-gateway"
    echo "  ./03-rollback.sh data-service-uc4"
    echo "  ./03-rollback.sh prediction-service-uc5-uc6"
    echo ""
    echo "Deployments disponiveis:"
    kubectl get deployments -n $NAMESPACE -o name | cut -d'/' -f2
    exit 1
fi

echo "Verificando deployment: $DEPLOYMENT"

if ! kubectl get deployment $DEPLOYMENT -n $NAMESPACE &>/dev/null; then
    echo "ERRO: Deployment $DEPLOYMENT nao encontrado"
    exit 1
fi

echo "Historico de revisoes:"
kubectl rollout history deployment $DEPLOYMENT -n $NAMESPACE

echo ""
read -p "Confirmar rollback para revisao anterior? (y/N): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Fazendo rollback..."
    kubectl rollout undo deployment $DEPLOYMENT -n $NAMESPACE
    
    echo "Aguardando rollback..."
    kubectl rollout status deployment $DEPLOYMENT -n $NAMESPACE --timeout=120s
    
    echo "Rollback concluido!"
else
    echo "Cancelado."
fi