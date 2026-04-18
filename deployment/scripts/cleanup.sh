#!/bin/bash

set -e

NAMESPACE="rota-maravilhosa"
CLUSTER_NAME="rota-maravilhosa-cluster"
REGION="europe-west1"

echo "========================================="
echo "LIMPEZA DE RECURSOS"
echo "========================================="
echo ""
echo "ATENCAO: Isto vai remover todos os recursos!"
echo ""

read -p "Remover apenas a aplicacao (namespace)? (y/N): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Removendo namespace $NAMESPACE..."
    kubectl delete namespace $NAMESPACE --ignore-not-found
    echo "Namespace removido."
    exit 0
fi

read -p "Remover tambem o cluster GKE? (y/N): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Removendo cluster $CLUSTER_NAME..."
    gcloud container clusters delete $CLUSTER_NAME --region $REGION --quiet
    echo "Cluster removido."
else
    echo "Apenas a aplicacao foi removida."
fi