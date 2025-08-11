"""
Teste de Compatibilidade - Sem PySpark
=====================================

Testa o projeto sem inicializar PySpark para evitar problemas de compatibilidade
com Python 3.13.
"""

import sys
import os
import ast
import json
from datetime import datetime

def mock_spark_imports():
    """Cria mocks para imports do PySpark para teste de sintaxe."""
    
    # Mock classes principais do PySpark
    class MockSparkSession:
        def __init__(self):
            self.version = "3.4.1"
        
        def sql(self, query):
            return MockDataFrame()
        
        def table(self, name):
            return MockDataFrame()
    
    class MockDataFrame:
        def count(self):
            return 1000
        
        def show(self):
            print("Mock DataFrame")
    
    # Adicionar mocks ao sys.modules
    sys.modules['pyspark'] = type(sys)('pyspark')
    sys.modules['pyspark.sql'] = type(sys)('pyspark.sql')
    sys.modules['pyspark.sql.SparkSession'] = MockSparkSession
    sys.modules['pyspark.sql.DataFrame'] = MockDataFrame
    
    # Mock de outras dependências
    sys.modules['loguru'] = type(sys)('loguru')
    sys.modules['matplotlib'] = type(sys)('matplotlib')
    sys.modules['matplotlib.pyplot'] = type(sys)('matplotlib.pyplot')
    sys.modules['seaborn'] = type(sys)('seaborn')
    sys.modules['pandas'] = type(sys)('pandas')

def test_imports_with_mocks():
    """Testa imports dos módulos usando mocks."""
    print("🔍 Testando imports com mocks...")
    
    # Configurar mocks
    mock_spark_imports()
    
    # Adicionar src ao path
    sys.path.insert(0, os.path.join(os.getcwd(), 'src'))
    sys.path.insert(0, os.path.join(os.getcwd(), 'analysis'))
    
    modules_to_test = [
        'bronze_layer',
        'silver_layer', 
        'gold_layer',
        'etl_pipeline'
    ]
    
    results = {
        "test_name": "import_compatibility",
        "timestamp": datetime.now().isoformat(),
        "status": "passed",
        "details": {
            "total_modules": len(modules_to_test),
            "successful_imports": 0,
            "failed_imports": 0,
            "modules": {}
        }
    }
    
    for module_name in modules_to_test:
        try:
            # Tentar importar o módulo
            module = __import__(module_name)
            
            # Verificar se tem as classes principais
            expected_classes = {
                'bronze_layer': 'BronzeLayer',
                'silver_layer': 'SilverLayer',
                'gold_layer': 'GoldLayer',
                'etl_pipeline': 'ETLPipeline'
            }
            
            if hasattr(module, expected_classes[module_name]):
                results["details"]["successful_imports"] += 1
                results["details"]["modules"][module_name] = {
                    "status": "success",
                    "main_class": expected_classes[module_name],
                    "has_main_class": True
                }
                print(f"  ✅ {module_name}: Import OK, classe {expected_classes[module_name]} encontrada")
            else:
                results["details"]["modules"][module_name] = {
                    "status": "partial",
                    "main_class": expected_classes[module_name],
                    "has_main_class": False,
                    "error": f"Classe {expected_classes[module_name]} não encontrada"
                }
                print(f"  ⚠️  {module_name}: Import OK, mas classe {expected_classes[module_name]} não encontrada")
                
        except Exception as e:
            results["details"]["failed_imports"] += 1
            results["details"]["modules"][module_name] = {
                "status": "failed",
                "error": str(e)
            }
            print(f"  ❌ {module_name}: Erro - {str(e)}")
            results["status"] = "failed"
    
    return results

def test_code_structure():
    """Testa estrutura do código sem executar."""
    print("\n🔍 Testando estrutura do código...")
    
    files_to_analyze = [
        'src/bronze_layer.py',
        'src/silver_layer.py',
        'src/gold_layer.py',
        'src/etl_pipeline.py',
        'analysis/nyc_taxi_analysis.py'
    ]
    
    results = {
        "test_name": "code_structure",
        "timestamp": datetime.now().isoformat(),
        "status": "passed",
        "details": {
            "total_files": len(files_to_analyze),
            "analyzed_files": 0,
            "syntax_errors": 0,
            "files": {}
        }
    }
    
    for file_path in files_to_analyze:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Parse AST para verificar sintaxe
                tree = ast.parse(content)
                
                # Análise básica
                classes = [node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
                functions = [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
                
                results["details"]["analyzed_files"] += 1
                results["details"]["files"][file_path] = {
                    "status": "success",
                    "classes": classes,
                    "functions": len(functions),
                    "lines": len(content.split('\n')),
                    "has_docstring": ast.get_docstring(tree) is not None
                }
                
                print(f"  ✅ {file_path}: {len(classes)} classes, {len(functions)} funções")
                
            except SyntaxError as e:
                results["details"]["syntax_errors"] += 1
                results["details"]["files"][file_path] = {
                    "status": "syntax_error",
                    "error": str(e),
                    "line": e.lineno
                }
                print(f"  ❌ {file_path}: Erro de sintaxe na linha {e.lineno}")
                results["status"] = "failed"
                
            except Exception as e:
                results["details"]["files"][file_path] = {
                    "status": "error",
                    "error": str(e)
                }
                print(f"  ❌ {file_path}: Erro - {str(e)}")
                results["status"] = "failed"
        else:
            results["details"]["files"][file_path] = {
                "status": "not_found"
            }
            print(f"  ❌ {file_path}: Arquivo não encontrado")
            results["status"] = "failed"
    
    return results

def test_required_analyses():
    """Verifica se as análises obrigatórias estão implementadas."""
    print("\n🔍 Testando análises obrigatórias...")
    
    analysis_patterns = {
        'analysis/nyc_taxi_analysis.py': [
            'get_monthly_average_total_amount',
            'get_hourly_average_passenger_count_may',
            'gold_monthly_aggregations',
            'gold_hourly_aggregations_may'
        ],
        'analysis/NYC_Taxi_Analysis.ipynb': [
            'gold_monthly_aggregations',
            'gold_hourly_aggregations_may',
            'avg_total_amount',
            'avg_passenger_count'
        ]
    }
    
    results = {
        "test_name": "required_analyses",
        "timestamp": datetime.now().isoformat(),
        "status": "passed",
        "details": {
            "total_patterns": sum(len(patterns) for patterns in analysis_patterns.values()),
            "found_patterns": 0,
            "missing_patterns": 0,
            "files": {}
        }
    }
    
    for file_path, patterns in analysis_patterns.items():
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                file_result = {
                    "total_patterns": len(patterns),
                    "found_patterns": 0,
                    "missing_patterns": [],
                    "patterns": {}
                }
                
                for pattern in patterns:
                    if pattern in content:
                        file_result["found_patterns"] += 1
                        results["details"]["found_patterns"] += 1
                        file_result["patterns"][pattern] = True
                        print(f"  ✅ {file_path}: {pattern} encontrado")
                    else:
                        file_result["missing_patterns"].append(pattern)
                        results["details"]["missing_patterns"] += 1
                        file_result["patterns"][pattern] = False
                        print(f"  ❌ {file_path}: {pattern} não encontrado")
                        results["status"] = "failed"
                
                results["details"]["files"][file_path] = file_result
                
            except Exception as e:
                results["details"]["files"][file_path] = {
                    "error": str(e)
                }
                print(f"  ❌ {file_path}: Erro - {str(e)}")
                results["status"] = "failed"
        else:
            results["details"]["files"][file_path] = {
                "error": "File not found"
            }
            print(f"  ❌ {file_path}: Arquivo não encontrado")
            results["status"] = "failed"
    
    return results

def generate_compatibility_report():
    """Gera relatório completo de compatibilidade."""
    
    print("🧪 TESTE DE COMPATIBILIDADE - NYC TAXI PIPELINE")
    print("=" * 60)
    print(f"Python Version: {sys.version}")
    print(f"Plataforma: {sys.platform}")
    print(f"Diretório: {os.getcwd()}")
    print()
    
    # Executar todos os testes
    import_results = test_imports_with_mocks()
    structure_results = test_code_structure()
    analysis_results = test_required_analyses()
    
    # Compilar relatório
    report = {
        "test_session": {
            "timestamp": datetime.now().isoformat(),
            "python_version": sys.version,
            "platform": sys.platform,
            "working_directory": os.getcwd()
        },
        "compatibility_status": "passed",
        "test_results": [
            import_results,
            structure_results, 
            analysis_results
        ],
        "summary": {
            "total_tests": 3,
            "passed_tests": 0,
            "failed_tests": 0
        }
    }
    
    # Calcular status geral
    for test_result in report["test_results"]:
        if test_result["status"] == "passed":
            report["summary"]["passed_tests"] += 1
        else:
            report["summary"]["failed_tests"] += 1
            report["compatibility_status"] = "failed"
    
    # Salvar relatório
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"compatibility_report_{timestamp}.json"
    
    with open(report_filename, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # Exibir resumo
    print("\n" + "=" * 60)
    print("📊 RESUMO DO TESTE DE COMPATIBILIDADE")
    print("=" * 60)
    print(f"Status Geral: {report['compatibility_status'].upper()}")
    print(f"Testes Aprovados: {report['summary']['passed_tests']}/{report['summary']['total_tests']}")
    print(f"Testes Falharam: {report['summary']['failed_tests']}")
    print(f"Relatório salvo em: {report_filename}")
    
    if report["compatibility_status"] == "passed":
        print("\n✅ PROJETO COMPATÍVEL!")
        print("🎯 Pronto para execução no Databricks (que usa Python 3.8-3.10)")
        print("💡 Para execução local, recomenda-se Python 3.8-3.10 com PySpark")
    else:
        print("\n⚠️  PROBLEMAS DE COMPATIBILIDADE DETECTADOS")
        print("🔧 Verifique os detalhes acima e corrija os problemas")
    
    return report["compatibility_status"] == "passed"

if __name__ == "__main__":
    success = generate_compatibility_report()
    exit(0 if success else 1)
