#!/bin/bash
set -e

PROJECT="proj1cc-493515"
NAMESPACE="rota-maravilhosa"
BASE_DIR=~/Rota-Maravilhosa

echo "========================================="
echo "Build e Deploy da Rota Maravilhosa"
echo "========================================="

if ! kubectl get namespace "$NAMESPACE" &>/dev/null; then
    kubectl create namespace "$NAMESPACE"
fi

echo "[0/6] Verificando infraestrutura necessária..."

cd "$BASE_DIR/deployment/scripts"
./Jaeger.sh
./Istio.sh
./circuit-breakers.sh

kubectl label namespace $NAMESPACE istio-injection=enabled --overwrite

enable_api() {
    local api=$1
    local max_retries=5
    local retry_delay=5
    
    for i in $(seq 1 $max_retries); do
        echo "Tentativa $i: Habilitando $api..."
        if gcloud services enable "$api" --project $PROJECT 2>/dev/null; then
            echo "✓ $api habilitada com sucesso"
            return 0
        else
            if [ $i -lt $max_retries ]; then
                echo "  Erro. Aguardando ${retry_delay}s antes de tentar novamente..."
                sleep $retry_delay
                retry_delay=$((retry_delay * 2))  # Exponential backoff
            fi
        fi
    done
    echo "✗ Falha ao habilitar $api após $max_retries tentativas"
    return 1
}

# Habilitar APIs uma por uma
enable_api "cloudbuild.googleapis.com"
enable_api "artifactregistry.googleapis.com"

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
