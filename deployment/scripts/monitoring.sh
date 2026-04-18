#!/bin/bash

NAMESPACE="rota-maravilhosa"

echo "========================================="
echo "MONITORIZACAO - Rota Maravilhosa"
echo "========================================="

echo ""
echo "=== PODS ==="
kubectl get pods -n $NAMESPACE -o wide

echo ""
echo "=== DEPLOYMENTS ==="
kubectl get deployments -n $NAMESPACE

echo ""
echo "=== SERVICES ==="
kubectl get svc -n $NAMESPACE

echo ""
echo "=== HPA ==="
kubectl get hpa -n $NAMESPACE

echo ""
echo "=== INGRESS ==="
kubectl get ingress -n $NAMESPACE

echo ""
echo "=== RECURSOS (CPU/MEMORIA) ==="
kubectl top pods -n $NAMESPACE 2>/dev/null || echo "  (metrics-server nao instalado)"

echo ""
echo "=== EVENTOS RECENTES ==="
kubectl get events -n $NAMESPACE --sort-by='.lastTimestamp' | tail -15