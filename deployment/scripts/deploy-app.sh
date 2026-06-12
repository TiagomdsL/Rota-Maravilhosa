#!/bin/bash
set -e

NAMESPACE="rota-maravilhosa"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBERNETES_DIR="$SCRIPT_DIR/../kubernetes"
DEPLOYMENTS_DIR="$SCRIPT_DIR/../deployments"
SERVICES_DIR="$SCRIPT_DIR/../services"

for dir in "$KUBERNETES_DIR" "$DEPLOYMENTS_DIR" "$SERVICES_DIR"; do
    if [ ! -d "$dir" ]; then
        echo "ERRO: Directório não encontrado: $dir"
        exit 1
    fi
done

#echo "[1/10] Criando namespace..."
#kubectl apply -f "$KUBERNETES_DIR/namespace.yaml"

echo "[2/10] Aplicando NetworkPolicy (default-deny)..."
kubectl apply -f "$KUBERNETES_DIR/networkpolicy.yaml" -n "$NAMESPACE"

echo "[2/9] Criando Secret do BigQuery..."
if [ -f "$SCRIPT_DIR/bq-key.json" ]; then
    kubectl create secret generic bq-secret \
        --namespace=$NAMESPACE \
        --from-file=API_TOKEN="$SCRIPT_DIR/bq-key.json" \
        --dry-run=client -o yaml | kubectl apply -f -
    echo "✓ Secret bq-secret criada a partir de $SCRIPT_DIR/bq-key.json"
else
    echo "⚠️ Ficheiro bq-key.json não encontrado em $SCRIPT_DIR"
fi

echo "[3/10] Criando Secret do Keycloak..."
kubectl create secret generic keycloak-secret \
    --namespace=$NAMESPACE \
    --from-literal=client-secret=bEGwBhvpIfQlKCbcLNkLVACoHNyH8Krx \
    --dry-run=client -o yaml | kubectl apply -f -
echo "✓ Secret keycloak-secret criada"

echo "[3/9] Verificando modelos no BigQuery..."
# Verificar se os modelos já existem
MODELS_EXIST=$(bq ls --models proj1cc-493515:accidents 2>/dev/null | grep -E "severity_model|risk_model|occurrence_model" | wc -l || echo "0")
if [ "$MODELS_EXIST" -lt 3 ]; then
    echo "   Modelos não encontrados (encontrados: $MODELS_EXIST/3). A criar..."
    bash "$SCRIPT_DIR/create-models.sh" || echo "⚠️ Erro ao criar modelos"
else
    echo "✓ Modelos já existem no BigQuery. A saltar criação."
fi

echo "[4/10] Aplicando ConfigMap..."
kubectl apply -f "$KUBERNETES_DIR/configmap.yaml" -n "$NAMESPACE"
kubectl apply -f "$KUBERNETES_DIR/prometheus_configmap.yaml" -n "$NAMESPACE"

echo "[5/10] Aplicando Secrets..."
if [ -f "$KUBERNETES_DIR/secrets.yaml" ]; then
    kubectl apply -f "$KUBERNETES_DIR/secrets.yaml" -n "$NAMESPACE"
else
    echo "   (secrets.yaml não encontrado - a saltar)"
fi

echo "[6/10] Aplicando Services..."
for svc in $(ls "$SERVICES_DIR"/*.yaml | sort); do
    echo "   - $(basename "$svc")"
    kubectl apply -f "$svc" -n "$NAMESPACE"
done

echo "[7/10] Aplicando Deployments..."
for deploy in $(ls "$DEPLOYMENTS_DIR"/*.yaml | sort); do
    echo "   - $(basename "$deploy")"
    kubectl apply -f "$deploy" -n "$NAMESPACE"
done

echo "[8/10] Aguardando pods ficarem prontos..."
if ! kubectl wait --for=condition=ready pod --all -n "$NAMESPACE" --timeout=300s; then
    echo "ERRO: Pods não ficaram prontos. Estado actual:"
    kubectl get pods -n "$NAMESPACE"
    kubectl describe pods -n "$NAMESPACE" | grep -A5 "Warning"
    exit 1
fi

echo "[9/10] Aplicando HPA e Ingress..."
kubectl apply -f "$KUBERNETES_DIR/horizontalpodautoscalers.yaml" -n "$NAMESPACE"
kubectl apply -f "$KUBERNETES_DIR/ingress.yaml" -n "$NAMESPACE"

echo ""
echo "Deploy concluído com sucesso!"
echo ""
INGRESS_IP=$(kubectl get ingress rota-maravilhosa-ingress -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
if [ -n "$INGRESS_IP" ]; then
    echo "API disponível em: http://$INGRESS_IP/health"
else
    echo "Para verificar:"
    echo "  kubectl get all -n $NAMESPACE"
    echo "  kubectl get ingress -n $NAMESPACE"
fi
