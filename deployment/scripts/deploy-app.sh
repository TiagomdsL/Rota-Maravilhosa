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

echo "[1/7] Criando namespace..."
kubectl apply -f "$KUBERNETES_DIR/namespace.yaml"

echo "[2/7] Aplicando ConfigMap..."
kubectl apply -f "$KUBERNETES_DIR/configmap.yaml" -n "$NAMESPACE"
kubectl apply -f "$KUBERNETES_DIR/prometheus_configmap.yaml" -n "$NAMESPACE"

echo "[3/7] Aplicando Secrets..."
if [ -f "$KUBERNETES_DIR/secrets.yaml" ]; then
    kubectl apply -f "$KUBERNETES_DIR/secrets.yaml" -n "$NAMESPACE"
else
    echo "   (secrets.yaml não encontrado - a saltar)"
fi

echo "[4/7] Aplicando Services..."
for svc in $(ls "$SERVICES_DIR"/*.yaml | sort); do
    echo "   - $(basename "$svc")"
    kubectl apply -f "$svc" -n "$NAMESPACE"
done

echo "[5/7] Aplicando Deployments..."
for deploy in $(ls "$DEPLOYMENTS_DIR"/*.yaml | sort); do
    echo "   - $(basename "$deploy")"
    kubectl apply -f "$deploy" -n "$NAMESPACE"
done

echo "[6/7] Aguardando pods ficarem prontos..."
if ! kubectl wait --for=condition=ready pod --all -n "$NAMESPACE" --timeout=300s; then
    echo "ERRO: Pods não ficaram prontos. Estado actual:"
    kubectl get pods -n "$NAMESPACE"
    kubectl describe pods -n "$NAMESPACE" | grep -A5 "Warning"
    exit 1
fi

echo "[7/7] Aplicando HPA e Ingress..."
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
