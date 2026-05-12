#!/bin/bash
echo "🚀 Criando modelos no BigQuery..."

# Modelo de severidade
echo "📊 Criando modelo de predição de severidade..."
bq query --use_legacy_sql=false << 'EOSQL'
CREATE OR REPLACE MODEL `proj1cc-493515.accidents.severity_model`
OPTIONS(model_type='random_forest_classifier', input_label_cols=['Severity']) AS
SELECT 
  Visibility_mi_ as visibility,
  Precipitation_in_ as precipitation,
  Severity
FROM `proj1cc-493515.accidents.accidents`
WHERE Severity IS NOT NULL
EOSQL

echo "✅ Modelo de severidade criado!"

# Modelo de risco (apenas hora)
echo "📊 Criando modelo de risco..."
bq query --use_legacy_sql=false << 'EOSQL'
CREATE OR REPLACE MODEL `proj1cc-493515.accidents.risk_model`
OPTIONS(model_type='linear_reg', input_label_cols=['severity_score']) AS
SELECT 
  EXTRACT(HOUR FROM Start_Time) as hour,
  Severity as severity_score
FROM `proj1cc-493515.accidents.accidents`
WHERE Start_Time IS NOT NULL
  AND Severity IS NOT NULL
EOSQL

echo "✅ Modelo de risco criado!"

# Modelo de ocorrência (apenas hora)
echo "📊 Criando modelo de previsão de ocorrência..."
bq query --use_legacy_sql=false << 'EOSQL'
CREATE OR REPLACE MODEL `proj1cc-493515.accidents.occurrence_model`
OPTIONS(model_type='logistic_reg', input_label_cols=['has_accident']) AS
SELECT 
  EXTRACT(HOUR FROM Start_Time) as hour,
  CASE 
    WHEN Severity >= 2 THEN 1
    ELSE 0
  END as has_accident
FROM `proj1cc-493515.accidents.accidents`
WHERE Start_Time IS NOT NULL
  AND Severity IS NOT NULL
EOSQL

echo "✅ Modelo de ocorrência criado!"

echo ""
echo "✅ Modelos criados com sucesso!"
