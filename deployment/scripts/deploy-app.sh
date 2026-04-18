#!/bin/bash

set -e

NAMESPACE="rota-maravilhosa"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$SCRIPT_DIR/../k8s"

echo "[1/8] Criando namespace..."
kubectl apply -f "$K8S_DIR/00-namespace.yaml"

echo "[2/8] Aplicando ConfigMap..."
kubectl apply -f "$K8S_DIR/01-configmap.yaml"

echo "[3/8] Aplicando Secrets..."
if [ -f "$K8S_DIR/02-secrets.yaml" ]; then
    kubectl apply -f "$K8S_DIR/02-secrets.yaml"
else
    echo "   (Ficheiro 02-secrets.yaml nao encontrado - a saltar)"
fi

echo "[4/8] Aplicando Volumes..."
if [ -f "$K8S_DIR/03-volumes.yaml" ]; then
    kubectl apply -f "$K8S_DIR/03-volumes.yaml"
else
    echo "   (Ficheiro 03-volumes.yaml nao encontrado - a saltar)"
fi

echo "[5/8] Aplicando Deployments..."
for deploy in "$K8S_DIR"/deployments/*.yaml; do
    if [ -f "$deploy" ]; then
        echo "   - $(basename $deploy)"
        kubectl apply -f "$deploy"
    fi
done

echo "[6/8] Aguardando pods ficarem prontos..."
kubectl wait --for=condition=ready pod --all -n $NAMESPACE --timeout=300s 2>/dev/null || true

echo "[7/8] Aplicando Services..."
for svc in "$K8S_DIR"/services/*.yaml; do
    if [ -f "$svc" ]; then
        echo "   - $(basename $svc)"
        kubectl apply -f "$svc"
    fi
done

echo "[8/8] Aplicando HPA e Ingress..."
kubectl apply -f "$K8S_DIR/06-hpa.yaml"
kubectl apply -f "$K8S_DIR/07-ingress.yaml"

echo ""
echo "Deploy concluido!"
echo ""
echo "Para verificar:"
echo "  kubectl get all -n $NAMESPACE"
echo "  kubectl get hpa -n $NAMESPACE"
echo "  kubectl get ingress -n $NAMESPACE"