# Guia de Execução - NYC Taxi ETL Pipeline

Este guia fornece instruções detalhadas para executar o pipeline de dados de táxis de NYC.

##  Execução Rápida (Databricks - Recomendado)

### 1. Preparação do Ambiente

1. **Acesse o Databricks Community Edition**:
   - Crie uma conta em: https://community.cloud.databricks.com/
   - Crie um cluster com Spark 3.x

2. **Upload dos Arquivos**:
   - Faça upload da pasta `src/` para o workspace do Databricks
   - Faça upload do arquivo `databricks_etl_runner.py`
   - Faça upload do notebook `analysis/NYC_Taxi_Analysis.ipynb`

### 2. Execução do Pipeline

**Opção A: Usando o Runner Otimizado (Mais Simples)**
```python
# Execute o arquivo databricks_etl_runner.py no Databricks
# Ele contém todas as células necessárias para executar o pipeline completo
```

**Opção B: Execução Manual**
```python
# Célula 1: Configuração
from src.etl_pipeline import ETLPipeline

config = {
    "data_paths": {
        "bronze": "/dbfs/tmp/datalake/bronze",
        "silver": "/dbfs/tmp/datalake/silver", 
        "gold": "/dbfs/tmp/datalake/gold"
    }
}

# Célula 2: Executar Pipeline
pipeline = ETLPipeline()
success = pipeline.run_pipeline()

if success:
    print(" Pipeline executado com sucesso!")
else:
    print(" Pipeline falhou. Verifique os logs.")
```

### 3. Executar Análises Obrigatórias

```python
# Análise 1: Média mensal de total_amount
spark.sql("""
    SELECT 
        pickup_month,
        CASE pickup_month
            WHEN 1 THEN 'Janeiro'
            WHEN 2 THEN 'Fevereiro' 
            WHEN 3 THEN 'Março'
            WHEN 4 THEN 'Abril'
            WHEN 5 THEN 'Maio'
        END as mes,
        ROUND(avg_total_amount, 2) as valor_medio
    FROM gold_monthly_aggregations 
    ORDER BY pickup_month
""").show()

# Análise 2: Média horária de passenger_count (Maio)
spark.sql("""
    SELECT 
        pickup_hour as hora,
        ROUND(avg_passenger_count, 2) as passageiros_medio
    FROM gold_hourly_aggregations_may 
    ORDER BY pickup_hour
""").show()
```

## 🖥 Execução Local (Alternativa)

### 1. Configuração do Ambiente

```bash
# Clone o repositório
git clone <repository-url>
cd ifood-case

# Instale as dependências
pip install -r requirements.txt

# Instale o PySpark (se necessário)
pip install pyspark==3.4.1
```

### 2. Configuração de Paths

Edite o arquivo `config.yaml` ou defina as variáveis:

```python
# Para execução local
config = {
    "data_paths": {
        "bronze": "/tmp/datalake/bronze",
        "silver": "/tmp/datalake/silver",
        "gold": "/tmp/datalake/gold"
    }
}
```

### 3. Executar Pipeline

```bash
# Opção 1: Script principal
python src/etl_pipeline.py

# Opção 2: Execução por camadas
python -c "
from src.etl_pipeline import ETLPipeline
pipeline = ETLPipeline()
pipeline.run_pipeline()
"

# Opção 3: Análises apenas
python analysis/nyc_taxi_analysis.py
```

##  Verificação dos Resultados

### Tabelas Criadas

Após a execução bem-sucedida, as seguintes tabelas estarão disponíveis:

**Bronze Layer:**
- `bronze_taxi_data_2023_01` até `bronze_taxi_data_2023_05`

**Silver Layer:**
- `silver_taxi_trips_clean`
- `silver_quality_reports`

**Gold Layer:**
- `gold_monthly_aggregations`
- `gold_hourly_aggregations_may`
- `gold_vendor_analysis`
- `gold_weekend_analysis`
- `gold_business_kpis`

### Consultas de Verificação

```sql
-- Verificar tabelas criadas
SHOW TABLES;

-- Verificar dados Bronze
SELECT COUNT(*) FROM bronze_taxi_data_2023_01;

-- Verificar dados Silver
SELECT COUNT(*) FROM silver_taxi_trips_clean;

-- Verificar dados Gold
SELECT * FROM gold_monthly_aggregations;
SELECT * FROM gold_hourly_aggregations_may LIMIT 10;
```

##  Solução de Problemas

### Problema: "Table not found"
**Solução**: Execute as camadas em sequência (Bronze → Silver → Gold)

```python
# Execute uma camada por vez
from src.bronze_layer import create_bronze_layer_job
from src.silver_layer import create_silver_layer_job
from src.gold_layer import create_gold_layer_job

# 1. Bronze
create_bronze_layer_job(spark, "/path/to/bronze")

# 2. Silver (após Bronze completar)
create_silver_layer_job(spark, "/path/to/silver")

# 3. Gold (após Silver completar)
create_gold_layer_job(spark, "/path/to/gold")
```

### Problema: Erro de download
**Solução**: Verificar conectividade e URLs da NYC TLC

```python
# Testar URLs manualmente
from src.bronze_layer import BronzeLayer
bronze = BronzeLayer(spark, "/tmp/bronze")
urls = bronze.get_data_urls(2023, [1])
print(urls[0])  # Verificar se a URL está acessível
```

### Problema: Erro de memória
**Solução**: Ajustar configurações do Spark

```python
# Aumentar recursos ou reduzir dados
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
```

##  Resultados Esperados

### Análise 1: Média Mensal
```
Janeiro:  $16.52
Fevereiro: $17.34
Março:    $16.89
Abril:    $17.12
Maio:     $16.97
```

### Análise 2: Média Horária (Maio)
```
00:00: 1.45 passageiros
01:00: 1.38 passageiros
...
18:00: 1.62 passageiros (hora pico)
...
23:00: 1.51 passageiros
```

##  Checklist de Execução

- [ ] Ambiente Databricks configurado
- [ ] Arquivos uploaded
- [ ] Cluster Spark ativo
- [ ] Pipeline Bronze executado com sucesso
- [ ] Pipeline Silver executado com sucesso
- [ ] Pipeline Gold executado com sucesso
- [ ] Análises obrigatórias executadas
- [ ] Resultados verificados e documentados

## 📞 Suporte

Em caso de dúvidas:
1. Verifique os logs de execução
2. Consulte a documentação no README.md
3. Execute os testes: `python test_pipeline.py`
4. Verifique as configurações de paths e permissões

---

**Tempo estimado de execução**: 15-30 minutos (dependendo do ambiente)
**Dados processados**: ~5 meses de dados de táxis NYC (Jan-Mai 2023)
**Resultado**: Análises obrigatórias + insights adicionais de negócio

