# NYC Taxi Data Engineering Pipeline

Uma solução completa de engenharia de dados para ingestão, tratamento, disponibilização e análise de dados de corridas de táxis Yellow Cab de Nova York, aplicando arquitetura em camadas (Bronze, Prata e Ouro) e utilizando PySpark no Databricks.

## Visão Geral do Projeto

Este projeto implementa um pipeline de dados end-to-end que processa dados de corridas de táxis de NYC de janeiro a maio de 2023, seguindo as melhores práticas de engenharia de dados com arquitetura em camadas.

### Arquitetura da Solução

```
Raw Data (NYC TLC) → Bronze Layer → Silver Layer → Gold Layer → Analytics
                      (Ingestão)   (Limpeza)     (Consumo)   (Insights)
```

## Estrutura do Projeto

```
ifood-case/
├── src/                          # Código fonte da solução
│   ├── __init__.py
│   ├── bronze_layer.py           # Ingestão de dados (Bronze)
│   ├── silver_layer.py           # Limpeza e transformação (Prata)
│   ├── gold_layer.py             # Agregações analíticas (Ouro)
│   └── etl_pipeline.py           # Orquestrador principal
├── analysis/                     # Análises e notebooks
│   ├── __init__.py
│   ├── nyc_taxi_analysis.py      # Script de análise Python
│   └── NYC_Taxi_Analysis.ipynb   # Notebook Jupyter/Databricks
├── test/                         # Testes e relatórios
│   ├── __init__.py
│   ├── README.md                 # Documentação dos testes
│   ├── test_pipeline.py          # Testes unitários completos
│   ├── test_simple.py            # Testes básicos sem PySpark
│   ├── test_syntax.py            # Validação de sintaxe
│   ├── test_compatibility.py     # Teste de compatibilidade
│   ├── generate_test_log.py      # Gerador de logs JSON
│   ├── show_test_results.py      # Visualizador de resultados
│   ├── final_test_report.py      # Relatório final
│   └── *.json                    # Logs e relatórios JSON
├── config.yaml                   # Configurações do pipeline
├── databricks_etl_runner.py      # Runner otimizado para Databricks
├── EXECUTION_GUIDE.md            # Guia de execução detalhado
├── requirements.txt              # Dependências Python
└── README.md                     # Documentação (este arquivo)
```

## Tecnologias Utilizadas

- **PySpark**: Processamento distribuído de dados
- **Databricks**: Plataforma de análise de dados
- **Python**: Linguagem principal
- **Pandas**: Análise de dados
- **Matplotlib/Seaborn**: Visualizações
- **Loguru**: Logging estruturado

## Fonte dos Dados

Os dados são obtidos do portal oficial da NYC Taxi and Limousine Commission:
- **URL**: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Período**: Janeiro a Maio de 2023
- **Formato**: Arquivos Parquet
- **Tipo**: Yellow Cab Trip Records

### Colunas Obrigatórias

O pipeline garante a presença e qualidade das seguintes colunas:
- `VendorID`: Identificador do fornecedor
- `passenger_count`: Número de passageiros
- `total_amount`: Valor total da corrida
- `tpep_pickup_datetime`: Data/hora de início da corrida
- `tpep_dropoff_datetime`: Data/hora de fim da corrida

## Camadas da Arquitetura

### Bronze Layer (Ingestão)
- **Objetivo**: Armazenar dados brutos sem transformação
- **Funcionalidades**:
  - Download automático dos arquivos da NYC TLC
  - Validação básica dos dados
  - Adição de metadados de auditoria
  - Particionamento por ano/mês

### Silver Layer (Transformação)
- **Objetivo**: Dados limpos e padronizados
- **Funcionalidades**:
  - Validação e aplicação de schema
  - Limpeza de dados (remoção de nulos, outliers)
  - Aplicação de regras de qualidade
  - Enriquecimento com colunas derivadas
  - Relatórios de qualidade

### Gold Layer (Consumo)
- **Objetivo**: Dados prontos para análise
- **Funcionalidades**:
  - Agregações mensais e horárias
  - Análise de fornecedores
  - Comparação fim de semana vs. dias úteis
  - KPIs de negócio
  - Otimização para consultas analíticas

## Configuração do Ambiente

### Pré-requisitos

1. **Python 3.8+**
2. **Databricks Community Edition** (recomendado) ou ambiente PySpark local
3. **Acesso à internet** para download dos dados

### Instalação das Dependências

```bash
# Clone o repositório
git clone <repository-url>
cd ifood-case

# Instale as dependências
pip install -r requirements.txt
```

### Configuração no Databricks

1. **Crie um cluster no Databricks**:
   - Runtime: 11.3 LTS ou superior
   - Configurações recomendadas:
     ```
     spark.sql.adaptive.enabled true
     spark.sql.adaptive.coalescePartitions.enabled true
     ```

2. **Faça upload dos arquivos**:
   - Upload da pasta `src/` para o workspace
   - Upload do notebook `analysis/NYC_Taxi_Analysis.ipynb`

3. **Configure os paths de armazenamento**:
   ```python
   # Para Databricks
   bronze_path = "/dbfs/mnt/datalake/bronze"
   silver_path = "/dbfs/mnt/datalake/silver"
   gold_path = "/dbfs/mnt/datalake/gold"
   ```

## Execução do Pipeline

### Opção 1: Execução Completa (Recomendado)

```python
# No Databricks ou ambiente PySpark
from src.etl_pipeline import ETLPipeline

# Criar e executar o pipeline completo
pipeline = ETLPipeline()
success = pipeline.run_pipeline()

if success:
    print("✅ Pipeline executado com sucesso!")
else:
    print("❌ Pipeline falhou. Verifique os logs.")
```

### Opção 2: Execução por Camadas

```python
from pyspark.sql import SparkSession
from src.bronze_layer import create_bronze_layer_job
from src.silver_layer import create_silver_layer_job
from src.gold_layer import create_gold_layer_job

spark = SparkSession.builder.appName("NYC_Taxi_ETL").getOrCreate()

# Executar Bronze Layer
create_bronze_layer_job(spark, "/path/to/bronze")

# Executar Silver Layer
create_silver_layer_job(spark, "/path/to/silver")

# Executar Gold Layer
create_gold_layer_job(spark, "/path/to/gold")
```

### Opção 3: Via Linha de Comando

```bash
# Execute o pipeline principal
python src/etl_pipeline.py

# Ou execute a análise diretamente
python analysis/nyc_taxi_analysis.py
```

## Análises Obrigatórias

O projeto implementa as seguintes análises conforme especificado:

### 1. Média de total_amount por mês

```sql
SELECT 
    pickup_month,
    ROUND(avg_total_amount, 2) as avg_total_amount,
    total_trips
FROM gold_monthly_aggregations 
ORDER BY pickup_month;
```

### 2. Média de passenger_count por hora do dia (Maio)

```sql
SELECT 
    pickup_hour,
    ROUND(avg_passenger_count, 2) as avg_passenger_count,
    total_trips
FROM gold_hourly_aggregations_may 
ORDER BY pickup_hour;
```

## Execução das Análises

### Usando o Notebook (Recomendado)

1. Abra o notebook `analysis/NYC_Taxi_Analysis.ipynb` no Databricks
2. Execute todas as células sequencialmente
3. Os resultados serão exibidos com visualizações

### Usando Script Python

```python
from analysis.nyc_taxi_analysis import NYCTaxiAnalyzer

# Inicializar analisador
analyzer = NYCTaxiAnalyzer(spark)

# Executar análise completa
analyzer.run_complete_analysis()

# Ou executar análises específicas
monthly_results = analyzer.get_monthly_average_total_amount()
hourly_results = analyzer.get_hourly_average_passenger_count_may()
```

## Resultados Esperados

### Análise 1: Média Mensal de total_amount
```
Janeiro:  $XX.XX (de X,XXX,XXX viagens)
Fevereiro: $XX.XX (de X,XXX,XXX viagens)
Março:    $XX.XX (de X,XXX,XXX viagens)
Abril:    $XX.XX (de X,XXX,XXX viagens)
Maio:     $XX.XX (de X,XXX,XXX viagens)
```

### Análise 2: Média Horária de passenger_count (Maio)
```
00:00: X.XX passageiros (de XXX,XXX viagens)
01:00: X.XX passageiros (de XXX,XXX viagens)
...
23:00: X.XX passageiros (de XXX,XXX viagens)
```

## Monitoramento e Qualidade

### Logs e Auditoria
- Logs detalhados em cada camada
- Métricas de qualidade de dados
- Rastreabilidade completa dos dados
- Relatórios de execução

### Validações Implementadas
- **Schema**: Verificação de tipos e colunas obrigatórias
- **Qualidade**: Remoção de outliers e dados inconsistentes
- **Integridade**: Validação de relacionamentos temporais
- **Completude**: Verificação de dados faltantes

### Métricas de Qualidade
```python
{
    "total_records": 10000000,
    "null_analysis": {...},
    "numeric_statistics": {...},
    "date_range": {...}
}
```

## Otimizações Implementadas

### Performance
- Particionamento por ano/mês
- Adaptive Query Execution
- Coalescing automático de partições
- Caching estratégico

### Escalabilidade
- Processamento distribuído com PySpark
- Arquitetura modular
- Configuração flexível de recursos

## Troubleshooting

### Problemas Comuns

1. **Erro de download de dados**:
   - Verifique conectividade com internet
   - URLs da NYC TLC podem ter mudado

2. **Erro de memória**:
   - Aumente recursos do cluster Databricks
   - Ajuste configurações de particionamento

3. **Tabelas não encontradas**:
   - Execute as camadas em sequência (Bronze → Silver → Gold)
   - Verifique se as tabelas foram criadas corretamente

4. **Erro de permissões**:
   - Verifique permissões de escrita nos paths configurados
   - No Databricks, use paths dentro de `/dbfs/`

### Logs e Debugging

```python
# Habilitar logs detalhados
import logging
logging.getLogger("pyspark").setLevel(logging.INFO)

# Verificar status das tabelas
spark.sql("SHOW TABLES").show()

# Verificar dados em cada camada
spark.table("bronze_taxi_data_2023_01").count()
spark.table("silver_taxi_trips_clean").count()
spark.table("gold_monthly_aggregations").count()
```

## Extensões Futuras

### Melhorias Sugeridas
1. **Automação**: Implementar scheduling com Airflow
2. **Streaming**: Adicionar processamento em tempo real
3. **ML**: Modelos preditivos de demanda
4. **Dashboard**: Interface web para visualizações
5. **Alertas**: Monitoramento proativo de qualidade

### Novas Análises
1. Análise de rotas mais populares
2. Padrões sazonais e tendências
3. Análise de preços dinâmicos
4. Otimização de frotas

## Testes e Validação

O projeto inclui uma suite completa de testes na pasta `test/`:

### Execução dos Testes

```bash
# Testes básicos (sempre funcionam)
python test/test_simple.py

# Testes de sintaxe e estrutura
python test/test_syntax.py

# Gerar logs JSON detalhados
python test/generate_test_log.py

# Ver resultados formatados
python test/show_test_results.py

# Relatório final consolidado
python test/final_test_report.py
```

### Status dos Testes

- **Estrutura**: 13/13 arquivos obrigatórios presentes
- **Sintaxe**: Todos os arquivos Python válidos
- **Padrões**: 21/21 padrões obrigatórios implementados
- **Análises**: 2/2 análises obrigatórias funcionais
- **Compatibilidade**: Python 3.13 + PySpark (usar Databricks)

### Logs JSON

Todos os resultados são documentados em logs JSON auditáveis:
- `test_log_*.json`: Logs completos detalhados
- `test_summary_*.json`: Resumos executivos
- `final_report_*.json`: Relatórios consolidados

Consulte `test/README.md` para documentação completa dos testes.

## Contato e Suporte

Para dúvidas ou suporte:
1. Verifique a documentação completa
2. Consulte os logs de execução
3. Revise os exemplos nos notebooks
4. Abra uma issue no repositório

---

## Licença

Este projeto é desenvolvido para fins educacionais e de demonstração técnica.

**Desenvolvido com PySpark e Databricks**

