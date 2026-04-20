#!/bin/bash
set -e

PROJECT="proj1cc-493515"
NAMESPACE="rota-maravilhosa"
BASE_DIR=~/Rota-Maravilhosa

echo "========================================="
echo "Build e Deploy da Rota Maravilhosa"
echo "========================================="

echo "[1/4] Habilitando APIs..."
gcloud services enable cloudbuild.googleapis.com artifactregistry.googleapis.com --project $PROJECT

echo "[2/4] Configurando permissões Cloud Build..."
PROJECT_NUMBER=$(gcloud projects describe $PROJECT --format="value(projectNumber)")
gcloud projects add-iam-policy-binding $PROJECT \
    --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
    --role="roles/cloudbuild.builds.builder" --quiet
gcloud projects add-iam-policy-binding $PROJECT \
    --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
    --role="roles/artifactregistry.writer" --quiet

echo "[3/4] Building e pushing imagens..."
gcloud builds submit "$BASE_DIR" \
    --config "$BASE_DIR/cloudbuild.yaml" \
    --project $PROJECT

echo "[4/4] Fazendo deploy no Kubernetes..."
cd "$BASE_DIR/deployment/scripts"
./deploy-app.sh

INGRESS_IP=$(kubectl get ingress rota-maravilhosa-ingress -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
echo ""
echo "========================================="
echo "Concluído!"
echo "========================================="
if [ -n "$INGRESS_IP" ]; then
    echo "API disponível em: http://$INGRESS_IP/health"
else
    echo "Verifica IP com: kubectl get ingress -n $NAMESPACE"
fi
