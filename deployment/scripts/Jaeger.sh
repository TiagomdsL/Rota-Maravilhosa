#!/bin/bash
# 01-install-jaeger.sh
# Script para instalar Jaeger 

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo " A instalar Jaeger e OpenTelemetry Collector..."

# 1. Criar namespace
echo " Criando namespace observability..."
kubectl create namespace observability --dry-run=client -o yaml | kubectl apply -f -

# 2. Instalar Jaeger all-in-one (sem operator)
echo " Instalando Jaeger..."
cat << 'EOF' | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jaeger
  namespace: observability
spec:
  replicas: 1
  selector:
    matchLabels:
      app: jaeger
  template:
    metadata:
      labels:
        app: jaeger
    spec:
      containers:
      - name: jaeger
        image: jaegertracing/all-in-one:latest
        ports:
        - containerPort: 16686
          name: ui
        - containerPort: 4318
          name: otlp-http
        env:
        - name: COLLECTOR_OTLP_ENABLED
          value: "true"
---
apiVersion: v1
kind: Service
metadata:
  name: jaeger
  namespace: observability
spec:
  selector:
    app: jaeger
  ports:
  - name: ui
    port: 16686
    targetPort: 16686
  - name: otlp-http
    port: 4318
    targetPort: 4318
EOF

# 3. Aguardar Jaeger
echo " Aguardando Jaeger..."
sleep 10
kubectl wait --namespace observability --for=condition=ready pod --all --timeout=60s

# 4. Instalar OpenTelemetry Collector a partir do YAML
echo " Instalando OpenTelemetry Collector..."
kubectl apply -f "$SCRIPT_DIR/otel-collector.yaml"

# 5. Criar alias DNS no namespace rota-maravilhosa
echo " Criando alias DNS..."
cat << 'EOT' | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: otel-collector
  namespace: rota-maravilhosa
spec:
  type: ExternalName
  externalName: otel-collector.observability.svc.cluster.local
EOT

echo ""
echo " Jaeger e OpenTelemetry Collector instalados!"
echo ""
echo " Para verificar:"
echo "   kubectl get pods -n observability"
echo ""
echo " Para aceder ao Jaeger:"
echo "   kubectl port-forward -n observability svc/jaeger 16686:16686"