#!/bin/bash
set -e

PROJECT="proj1cc-493515"
NAMESPACE="rota-maravilhosa"
BASE_DIR=~/Rota-Maravilhosa

echo "========================================="
echo "Build e Deploy da Rota Maravilhosa"
echo "========================================="

# Configurar kubectl para o cluster correto
echo "[0/6] Configurando kubectl..."
gcloud container clusters get-credentials rota-maravilhosa-cluster --zone=us-central1-a --project=$PROJECT

if ! kubectl get namespace "$NAMESPACE" &>/dev/null; then
    kubectl create namespace "$NAMESPACE"
fi

echo "[1/6] Verificando infraestrutura necessaria..."

cd "$BASE_DIR/deployment/scripts"
./Jaeger.sh
./Istio.sh
./circuit-breakers.sh

# ATENÇÃO: Isto ativa Istio para todos os pods
kubectl label namespace $NAMESPACE istio-injection=enabled --overwrite

# MAS desativa especificamente para o Keycloak
echo "[...] Configurando Keycloak sem Istio..."
kubectl label namespace $NAMESPACE istio-injection=enabled --overwrite
kubectl annotate deployment keycloak -n $NAMESPACE sidecar.istio.io/inject="false" --overwrite 2>/dev/null || true

enable_api() {
    local api=$1
    local max_retries=5
    local retry_delay=5
    
    for i in $(seq 1 $max_retries); do
        echo "Tentativa $i: Habilitando $api..."
        if gcloud services enable "$api" --project $PROJECT 2>/dev/null; then
            echo "$api habilitada com sucesso"
            return 0
        else
            if [ $i -lt $max_retries ]; then
                echo "  Erro. Aguardando ${retry_delay}s antes de tentar novamente..."
                sleep $retry_delay
                retry_delay=$((retry_delay * 2))
            fi
        fi
    done
    echo "Falha ao habilitar $api apos $max_retries tentativas"
    return 1
}

enable_api "cloudbuild.googleapis.com"
enable_api "artifactregistry.googleapis.com"

echo "[2/4] Configurando permissoes Cloud Build..."
PROJECT_NUMBER=$(gcloud projects describe $PROJECT --format="value(projectNumber)")
gcloud projects add-iam-policy-binding $PROJECT \
    --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
    --role="roles/cloudbuild.builds.builder" --quiet 2>/dev/null || true
gcloud projects add-iam-policy-binding $PROJECT \
    --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
    --role="roles/artifactregistry.writer" --quiet 2>/dev/null || true

echo "[3/4] Instalando nginx ingress controller..."
if ! kubectl get namespace ingress-nginx &>/dev/null; then
    kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.8.1/deploy/static/provider/cloud/deploy.yaml
    echo "Aguardando ingress controller ficar pronto..."
    sleep 30
    kubectl wait --namespace ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=120s 2>/dev/null || true
fi

echo "[4/4] Building e pushing imagens..."
gcloud builds submit "$BASE_DIR" \
    --config "$BASE_DIR/cloudbuild.yaml" \
    --project $PROJECT

echo "[5/5] Fazendo deploy no Kubernetes..."
cd "$BASE_DIR/deployment/scripts"

./deploy-app.sh

INGRESS_IP=$(kubectl get ingress rota-maravilhosa-ingress -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
echo ""
echo "========================================="
echo "Concluido"
echo "========================================="
if [ -n "$INGRESS_IP" ]; then
    echo "API disponivel em: http://$INGRESS_IP/health"
    echo "Swagger disponivel em: http://$INGRESS_IP/docs"
else
    echo "Verifica IP com: kubectl get ingress -n $NAMESPACE -w"
fi