#!/bin/bash
# 02-install-istio.sh
# Script para instalar Istio e ativar sidecar injection

set -e  # Para o script se algum comando falhar

echo "🚀 A instalar Istio Service Mesh..."

# Verificar se o Istio já está instalado
if kubectl get pods -n istio-system &>/dev/null; then
    echo "⚠️ Istio já parece estar instalado. A verificar..."
    if kubectl get pods -n istio-system | grep -q "Running"; then
        echo "✅ Istio já está instalado e a funcionar."
        read -p "Deseja reinstalar? (s/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Ss]$ ]]; then
            echo "🚫 A abortar instalação."
            exit 0
        fi
    fi
fi

# 1. Download do Istio
echo "📥 Fazendo download do Istio..."
curl -L https://istio.io/downloadIstio | sh -

# 2. Mover para a pasta do Istio
echo "📁 Entrando na pasta do Istio..."
cd istio-*

# 3. Adicionar istioctl ao PATH
export PATH=$PWD/bin:$PATH
echo "✅ istioctl adicionado ao PATH"

# 4. Verificar pré-requisitos
echo "🔍 Verificando pré-requisitos..."
istioctl x precheck

# 5. Instalar Istio
echo "📦 Instalando Istio (perfil default)..."
istioctl install --set profile=default -y

# 6. Verificar instalação
echo "🔍 Verificando pods do Istio..."
kubectl get pods -n istio-system

# 7. Voltar à pasta anterior
cd ..

# 8. Ativar sidecar injection no namespace
echo "🔧 Ativando sidecar injection no namespace rota-maravilhosa..."
kubectl label namespace rota-maravilhosa istio-injection=enabled --overwrite

# 9. Verificar se a label foi aplicada
echo "🔍 Verificando label do namespace..."
kubectl get namespace rota-maravilhosa -o yaml | grep istio-injection

echo ""
echo "✅ Istio instalado com sucesso!"
echo ""
echo "⚠️ IMPORTANTE: Precisa reiniciar os serviços para os sidecars serem injetados."
echo ""
echo "📋 Para reiniciar todos os serviços:"
echo "   kubectl rollout restart deployment -n rota-maravilhosa"
echo ""
echo "🔍 Para verificar (deve mostrar 2/2 READY):"
echo "   kubectl get pods -n rota-maravilhosa"