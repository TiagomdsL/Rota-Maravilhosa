#!/bin/bash

set -e

NAMESPACE="rota-maravilhosa"
CLUSTER_NAME="rota-maravilhosa-cluster"
ZONE="us-west4"
PROJECT="proj1cc-493515"

echo "========================================="
echo "LIMPEZA DE RECURSOS"
echo "========================================="
echo ""
echo "Escolhe uma opcao:"
echo "  1) Remover apenas a aplicacao (namespace)"
echo "  2) Scale down do cluster para 0 nos (poupa dinheiro, mantem cluster)"
echo "  3) Eliminar cluster completamente"
echo ""
read -p "Opcao (1/2/3): " -n 1 -r
echo ""

case $REPLY in
    1)
        echo "Removendo namespace $NAMESPACE..."
        kubectl delete namespace $NAMESPACE --ignore-not-found
        echo "Namespace removido."
        ;;
    2)
        echo "Fazendo scale down do cluster para 0 nos..."
        gcloud container clusters resize $CLUSTER_NAME \
            --region $ZONE \
            --node-pool default-pool \
            --num-nodes 0 \
            --project $PROJECT \
            --quiet
        echo "Cluster em modo economico (0 nos)."
        echo "Para voltar a ligar: gcloud container clusters resize $CLUSTER_NAME --region $ZONE --node-pool default-pool --num-nodes 2 --project $PROJECT"
        ;;
    3)
        echo "Eliminando cluster $CLUSTER_NAME..."
        gcloud container clusters delete $CLUSTER_NAME \
            --region $ZONE \
            --project $PROJECT \
            --quiet
        echo "Cluster eliminado."
        ;;
    *)
        echo "Opcao invalida."
        exit 1
        ;;
esac
