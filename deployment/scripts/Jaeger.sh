#!/bin/bash
# 01-install-jaeger.sh
# Script para instalar Jaeger e OpenTelemetry Collector

set -e  # Para o script se algum comando falhar

echo "🚀 A instalar Jaeger e OpenTelemetry Collector..."

# 1. Criar namespace
echo "📁 Criando namespace observability..."
kubectl create namespace observability --dry-run=client -o yaml | kubectl apply -f -

# 2. Instalar cert-manager (requisito para o Jaeger Operator)
echo "📦 Instalando cert-manager..."
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.2/cert-manager.yaml

# 3. Aguardar cert-manager iniciar
echo "⏳ Aguardando cert-manager..."
kubectl wait --namespace cert-manager --for=condition=ready pod --all --timeout=120s

# 4. Instalar Jaeger Operator
echo "📦 Instalando Jaeger Operator..."
kubectl create -f https://github.com/jaegertracing/jaeger-operator/releases/download/v1.51.0/jaeger-operator.yaml -n observability

# 5. Aguardar Operator iniciar
echo "⏳ Aguardando Jaeger Operator..."
kubectl wait --namespace observability --for=condition=ready pod -l name=jaeger-operator --timeout=60s

# 6. Criar instância do Jaeger
echo "🔧 Criando instância Jaeger..."
kubectl apply -n observability -f - <<EOF
apiVersion: jaegertracing.io/v1
kind: Jaeger
metadata:
  name: jaeger-traces
spec:
  strategy: allInOne
  allInOne:
    options:
      log-level: info
  ingress:
    enabled: false
  storage:
    type: memory
    options:
      memory:
        max-traces: 100000
EOF

# 7. Aplicar OpenTelemetry Collector (se o arquivo existir)
if [ -f "../kubernetes/otel-collector.yaml" ]; then
    echo "📦 Aplicando OpenTelemetry Collector..."
    kubectl apply -f ../kubernetes/otel-collector.yaml
else
    echo "⚠️ Ficheiro otel-collector.yaml não encontrado em ../kubernetes/"
fi

echo ""
echo "✅ Jaeger e OpenTelemetry Collector instalados com sucesso!"
echo ""
echo "🔍 Para verificar:"
echo "   kubectl get pods -n observability"
echo ""
echo "🌐 Para aceder à interface do Jaeger (port-forward):"
echo "   kubectl port-forward -n observability svc/jaeger-traces-query 16686:16686"
echo "   Aceda em: http://localhost:16686"