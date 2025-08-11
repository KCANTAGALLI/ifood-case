"""
Gerador de Log JSON dos Testes
==============================

Executa todos os testes e gera um log detalhado em formato JSON.
"""

import json
import os
import ast
import sys
from datetime import datetime
import hashlib

def get_file_hash(file_path):
    """Calcula hash MD5 de um arquivo."""
    try:
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except:
        return None

def get_file_stats(file_path):
    """Obtém estatísticas de um arquivo."""
    try:
        stat = os.stat(file_path)
        return {
            "size_bytes": stat.st_size,
            "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "created_time": datetime.fromtimestamp(stat.st_ctime).isoformat()
        }
    except:
        return None

def analyze_python_file(file_path):
    """Análise detalhada de um arquivo Python."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Parse AST
        tree = ast.parse(content)
        
        analysis = {
            "syntax_valid": True,
            "lines_of_code": len([line for line in content.split('\n') if line.strip() and not line.strip().startswith('#')]),
            "total_lines": len(content.split('\n')),
            "docstring": ast.get_docstring(tree),
            "has_docstring": ast.get_docstring(tree) is not None,
            "classes": [],
            "functions": [],
            "imports": [],
            "constants": [],
            "complexity_metrics": {
                "total_nodes": 0,
                "function_count": 0,
                "class_count": 0,
                "import_count": 0
            }
        }
        
        # Análise detalhada dos nós
        for node in ast.walk(tree):
            analysis["complexity_metrics"]["total_nodes"] += 1
            
            if isinstance(node, ast.ClassDef):
                class_info = {
                    "name": node.name,
                    "line_number": node.lineno,
                    "methods": [],
                    "docstring": ast.get_docstring(node)
                }
                
                # Métodos da classe
                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        class_info["methods"].append({
                            "name": item.name,
                            "line_number": item.lineno,
                            "args_count": len(item.args.args),
                            "docstring": ast.get_docstring(item)
                        })
                
                analysis["classes"].append(class_info)
                analysis["complexity_metrics"]["class_count"] += 1
                
            elif isinstance(node, ast.FunctionDef):
                func_info = {
                    "name": node.name,
                    "line_number": node.lineno,
                    "args_count": len(node.args.args),
                    "docstring": ast.get_docstring(node),
                    "is_method": False
                }
                analysis["functions"].append(func_info)
                analysis["complexity_metrics"]["function_count"] += 1
                
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    analysis["imports"].append({
                        "module": alias.name,
                        "alias": alias.asname,
                        "type": "import",
                        "line_number": node.lineno
                    })
                analysis["complexity_metrics"]["import_count"] += 1
                
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ''
                for alias in node.names:
                    analysis["imports"].append({
                        "module": f"{module}.{alias.name}",
                        "alias": alias.asname,
                        "type": "from_import",
                        "line_number": node.lineno
                    })
                analysis["complexity_metrics"]["import_count"] += 1
                
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id.isupper():
                        analysis["constants"].append({
                            "name": target.id,
                            "line_number": node.lineno
                        })
        
        return True, analysis
        
    except SyntaxError as e:
        return False, {
            "syntax_valid": False,
            "syntax_error": {
                "message": str(e),
                "line_number": e.lineno,
                "offset": e.offset
            }
        }
    except Exception as e:
        return False, {
            "syntax_valid": False,
            "error": str(e)
        }

def test_project_structure():
    """Testa estrutura do projeto com detalhes."""
    required_files = [
        'src/__init__.py',
        'src/bronze_layer.py',
        'src/silver_layer.py', 
        'src/gold_layer.py',
        'src/etl_pipeline.py',
        'analysis/__init__.py',
        'analysis/nyc_taxi_analysis.py',
        'analysis/NYC_Taxi_Analysis.ipynb',
        'requirements.txt',
        'README.md',
        'config.yaml',
        'databricks_etl_runner.py',
        'EXECUTION_GUIDE.md'
    ]
    
    structure_test = {
        "test_name": "project_structure",
        "description": "Verifica se todos os arquivos obrigatórios estão presentes",
        "timestamp": datetime.now().isoformat(),
        "status": "passed",
        "details": {
            "required_files": len(required_files),
            "found_files": 0,
            "missing_files": [],
            "extra_files": [],
            "file_details": {}
        }
    }
    
    # Verificar arquivos obrigatórios
    for file_path in required_files:
        if os.path.exists(file_path):
            structure_test["details"]["found_files"] += 1
            structure_test["details"]["file_details"][file_path] = {
                "exists": True,
                "hash": get_file_hash(file_path),
                "stats": get_file_stats(file_path)
            }
        else:
            structure_test["details"]["missing_files"].append(file_path)
            structure_test["details"]["file_details"][file_path] = {
                "exists": False
            }
    
    # Verificar arquivos extras
    all_files = []
    for root, dirs, files in os.walk('.'):
        for file in files:
            if not file.startswith('.') and not file.endswith('.pyc'):
                file_path = os.path.join(root, file).replace('.\\', '').replace('\\', '/')
                all_files.append(file_path)
    
    for file_path in all_files:
        if file_path not in required_files:
            structure_test["details"]["extra_files"].append(file_path)
    
    # Status final
    if structure_test["details"]["missing_files"]:
        structure_test["status"] = "failed"
        structure_test["error"] = f"Missing files: {structure_test['details']['missing_files']}"
    
    return structure_test

def test_python_files():
    """Testa todos os arquivos Python."""
    python_files = [
        'src/bronze_layer.py',
        'src/silver_layer.py',
        'src/gold_layer.py', 
        'src/etl_pipeline.py',
        'analysis/nyc_taxi_analysis.py'
    ]
    
    python_test = {
        "test_name": "python_files_analysis",
        "description": "Análise detalhada de todos os arquivos Python",
        "timestamp": datetime.now().isoformat(),
        "status": "passed",
        "details": {
            "total_files": len(python_files),
            "passed_files": 0,
            "failed_files": 0,
            "total_lines": 0,
            "total_classes": 0,
            "total_functions": 0,
            "files": {}
        }
    }
    
    for file_path in python_files:
        if os.path.exists(file_path):
            success, analysis = analyze_python_file(file_path)
            
            file_result = {
                "file_path": file_path,
                "analysis_success": success,
                "hash": get_file_hash(file_path),
                "stats": get_file_stats(file_path)
            }
            
            if success:
                file_result.update(analysis)
                python_test["details"]["passed_files"] += 1
                python_test["details"]["total_lines"] += analysis.get("total_lines", 0)
                python_test["details"]["total_classes"] += len(analysis.get("classes", []))
                python_test["details"]["total_functions"] += len(analysis.get("functions", []))
            else:
                file_result.update(analysis)
                python_test["details"]["failed_files"] += 1
                if python_test["status"] == "passed":
                    python_test["status"] = "failed"
            
            python_test["details"]["files"][file_path] = file_result
        else:
            python_test["details"]["files"][file_path] = {
                "file_path": file_path,
                "analysis_success": False,
                "error": "File not found"
            }
            python_test["details"]["failed_files"] += 1
            python_test["status"] = "failed"
    
    return python_test

def test_required_patterns():
    """Testa padrões obrigatórios do projeto."""
    patterns_config = [
        {
            "file": "src/bronze_layer.py",
            "patterns": [
                {"pattern": "class BronzeLayer", "description": "Classe principal Bronze Layer"},
                {"pattern": "def ingest_data", "description": "Método de ingestão de dados"},
                {"pattern": "https://d37ci6vzurychx.cloudfront.net", "description": "URL base NYC TLC"},
                {"pattern": "yellow_tripdata", "description": "Padrão nome arquivo taxi"}
            ]
        },
        {
            "file": "src/silver_layer.py", 
            "patterns": [
                {"pattern": "class SilverLayer", "description": "Classe principal Silver Layer"},
                {"pattern": "def clean_data", "description": "Método de limpeza"},
                {"pattern": "required_columns", "description": "Definição colunas obrigatórias"},
                {"pattern": "VendorID", "description": "Coluna VendorID"},
                {"pattern": "passenger_count", "description": "Coluna passenger_count"},
                {"pattern": "total_amount", "description": "Coluna total_amount"},
                {"pattern": "tpep_pickup_datetime", "description": "Coluna pickup datetime"},
                {"pattern": "tpep_dropoff_datetime", "description": "Coluna dropoff datetime"}
            ]
        },
        {
            "file": "src/gold_layer.py",
            "patterns": [
                {"pattern": "class GoldLayer", "description": "Classe principal Gold Layer"},
                {"pattern": "def create_monthly_aggregations", "description": "Agregações mensais"},
                {"pattern": "def create_hourly_aggregations", "description": "Agregações horárias"},
                {"pattern": "gold_monthly_aggregations", "description": "Tabela agregações mensais"},
                {"pattern": "gold_hourly_aggregations_may", "description": "Tabela agregações horárias Maio"}
            ]
        },
        {
            "file": "analysis/nyc_taxi_analysis.py",
            "patterns": [
                {"pattern": "def get_monthly_average_total_amount", "description": "Análise 1 obrigatória"},
                {"pattern": "def get_hourly_average_passenger_count_may", "description": "Análise 2 obrigatória"},
                {"pattern": "gold_monthly_aggregations", "description": "Query tabela mensal"},
                {"pattern": "gold_hourly_aggregations_may", "description": "Query tabela horária"}
            ]
        }
    ]
    
    patterns_test = {
        "test_name": "required_patterns",
        "description": "Verifica se todos os padrões obrigatórios estão implementados",
        "timestamp": datetime.now().isoformat(),
        "status": "passed",
        "details": {
            "total_files": len(patterns_config),
            "total_patterns": sum(len(f["patterns"]) for f in patterns_config),
            "found_patterns": 0,
            "missing_patterns": 0,
            "files": {}
        }
    }
    
    for file_config in patterns_config:
        file_path = file_config["file"]
        file_result = {
            "file_path": file_path,
            "total_patterns": len(file_config["patterns"]),
            "found_patterns": 0,
            "missing_patterns": 0,
            "patterns": {}
        }
        
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            for pattern_config in file_config["patterns"]:
                pattern = pattern_config["pattern"]
                description = pattern_config["description"]
                
                if pattern in content:
                    file_result["found_patterns"] += 1
                    patterns_test["details"]["found_patterns"] += 1
                    file_result["patterns"][pattern] = {
                        "found": True,
                        "description": description
                    }
                else:
                    file_result["missing_patterns"] += 1
                    patterns_test["details"]["missing_patterns"] += 1
                    file_result["patterns"][pattern] = {
                        "found": False,
                        "description": description
                    }
                    patterns_test["status"] = "failed"
        else:
            file_result["error"] = "File not found"
            file_result["missing_patterns"] = file_result["total_patterns"]
            patterns_test["details"]["missing_patterns"] += file_result["total_patterns"]
            patterns_test["status"] = "failed"
        
        patterns_test["details"]["files"][file_path] = file_result
    
    return patterns_test

def generate_system_info():
    """Gera informações do sistema."""
    return {
        "timestamp": datetime.now().isoformat(),
        "python_version": sys.version,
        "platform": sys.platform,
        "working_directory": os.getcwd(),
        "environment": {
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": os.environ.get("PYTHONPATH", ""),
            "USER": os.environ.get("USERNAME", os.environ.get("USER", "unknown"))
        }
    }

def generate_complete_test_log():
    """Gera log completo dos testes em JSON."""
    
    print("Gerando log detalhado dos testes...")
    
    # Executar todos os testes
    structure_test = test_project_structure()
    python_test = test_python_files()
    patterns_test = test_required_patterns()
    
    # Compilar log completo
    complete_log = {
        "test_session": {
            "session_id": hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8],
            "start_time": datetime.now().isoformat(),
            "test_framework": "NYC Taxi ETL Pipeline Test Suite",
            "version": "1.0.0"
        },
        "system_info": generate_system_info(),
        "project_info": {
            "name": "NYC Taxi Data Engineering Pipeline",
            "description": "Pipeline de dados end-to-end para processamento de dados de táxis de NYC",
            "architecture": "Bronze-Silver-Gold",
            "target_platform": "Databricks + PySpark"
        },
        "test_results": {
            "summary": {
                "total_tests": 3,
                "passed_tests": 0,
                "failed_tests": 0,
                "overall_status": "unknown"
            },
            "individual_tests": [
                structure_test,
                python_test,
                patterns_test
            ]
        },
        "metrics": {
            "project_files": {
                "total_python_files": python_test["details"]["total_files"],
                "total_lines_of_code": python_test["details"]["total_lines"],
                "total_classes": python_test["details"]["total_classes"],
                "total_functions": python_test["details"]["total_functions"]
            },
            "coverage": {
                "required_files_found": structure_test["details"]["found_files"],
                "required_files_total": structure_test["details"]["required_files"],
                "required_patterns_found": patterns_test["details"]["found_patterns"],
                "required_patterns_total": patterns_test["details"]["total_patterns"]
            }
        }
    }
    
    # Calcular status geral
    passed_tests = sum(1 for test in complete_log["test_results"]["individual_tests"] if test["status"] == "passed")
    failed_tests = sum(1 for test in complete_log["test_results"]["individual_tests"] if test["status"] == "failed")
    
    complete_log["test_results"]["summary"]["passed_tests"] = passed_tests
    complete_log["test_results"]["summary"]["failed_tests"] = failed_tests
    complete_log["test_results"]["summary"]["overall_status"] = "passed" if failed_tests == 0 else "failed"
    
    # Finalizar log
    complete_log["test_session"]["end_time"] = datetime.now().isoformat()
    complete_log["test_session"]["duration_seconds"] = 1  # Placeholder
    
    return complete_log

def main():
    """Função principal."""
    print("GERADOR DE LOG JSON DOS TESTES")
    print("=" * 50)
    
    # Gerar log completo
    log_data = generate_complete_test_log()
    
    # Salvar em arquivo JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"test_log_{timestamp}.json"
    
    with open(log_filename, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, indent=2, ensure_ascii=False, default=str)
    
    # Salvar versão resumida também
    summary_filename = f"test_summary_{timestamp}.json"
    summary_data = {
        "session_id": log_data["test_session"]["session_id"],
        "timestamp": log_data["test_session"]["start_time"],
        "overall_status": log_data["test_results"]["summary"]["overall_status"],
        "summary": log_data["test_results"]["summary"],
        "metrics": log_data["metrics"]
    }
    
    with open(summary_filename, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)
    
    # Exibir resultados
    print(f"Log completo salvo em: {log_filename}")
    print(f"Resumo salvo em: {summary_filename}")
    
    print(f"\nRESUMO DOS TESTES:")
    print(f"Status Geral: {log_data['test_results']['summary']['overall_status'].upper()}")
    print(f"Testes Aprovados: {log_data['test_results']['summary']['passed_tests']}")
    print(f"Testes Falharam: {log_data['test_results']['summary']['failed_tests']}")
    print(f"Total de Arquivos Python: {log_data['metrics']['project_files']['total_python_files']}")
    print(f"Total de Linhas de Código: {log_data['metrics']['project_files']['total_lines_of_code']}")
    print(f"Cobertura de Arquivos: {log_data['metrics']['coverage']['required_files_found']}/{log_data['metrics']['coverage']['required_files_total']}")
    print(f"Cobertura de Padrões: {log_data['metrics']['coverage']['required_patterns_found']}/{log_data['metrics']['coverage']['required_patterns_total']}")
    
    return log_data["test_results"]["summary"]["overall_status"] == "passed"

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
