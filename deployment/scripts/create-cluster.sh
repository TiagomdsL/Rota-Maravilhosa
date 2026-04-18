#!/bin/bash

set -e

PROJECT_ID="rota-maravilhosa-2024"
CLUSTER_NAME="rota-maravilhosa-cluster"
REGION="europe-west1"
ZONE="europe-west1-b"
NODE_COUNT=2
MACHINE_TYPE="e2-standard-2"

echo "[1/6] Verificando autenticacao..."
gcloud auth print-access-token &>/dev/null
if [ $? -ne 0 ]; then
    echo "ERRO: Nao autenticado. Execute: gcloud auth login"
    exit 1
fi

echo "[2/6] Configurando projeto..."
gcloud config set project $PROJECT_ID

echo "[3/6] Habilitando APIs..."
gcloud services enable container.googleapis.com
gcloud services enable artifactregistry.googleapis.com

echo "[4/6] Criando cluster GKE..."
gcloud container clusters create $CLUSTER_NAME \
    --region $REGION \
    --zone $ZONE \
    --node-locations $ZONE \
    --num-nodes $NODE_COUNT \
    --machine-type $MACHINE_TYPE \
    --disk-size 30 \
    --disk-type pd-standard \
    --image-type COS_CONTAINERD \
    --enable-autorepair \
    --enable-autoupgrade \
    --enable-autoscaling \
    --min-nodes 1 \
    --max-nodes 5

echo "[5/6] Obtendo credentials..."
gcloud container clusters get-credentials $CLUSTER_NAME --region $REGION

echo "[6/6] Verificando cluster..."
kubectl get nodes

echo "Cluster criado com sucesso!"