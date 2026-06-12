#!/bin/bash
set -e

CLUSTER_NAME="rota-maravilhosa-cluster"
REGION="us-west4"
NODE_COUNT=2
MACHINE_TYPE="e2-standard-2"

echo "[1/5] Verificando autenticacao..."
gcloud auth print-access-token &>/dev/null
if [ $? -ne 0 ]; then
    echo "ERRO: Nao autenticado. Execute: gcloud auth login"
    exit 1
fi

echo "[2/5] Habilitando API do Kubernetes..."
gcloud services enable container.googleapis.com

echo "[3/5] Criando cluster GKE..."
gcloud container clusters create $CLUSTER_NAME \
    --region $REGION \
    --num-nodes $NODE_COUNT \
    --machine-type $MACHINE_TYPE \
    --disk-type pd-standard \
    --disk-size 30

echo "[4/5] Obtendo credentials..."
gcloud container clusters get-credentials $CLUSTER_NAME --region $REGION

echo "[5/5] Verificando cluster..."
kubectl get nodes

echo "✅ Cluster criado com sucesso!"