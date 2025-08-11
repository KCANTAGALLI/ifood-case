"""
Visualizador de Resultados dos Testes
====================================

Exibe os resultados dos testes de forma amigável.
"""

import json
import os
from datetime import datetime

def show_test_results():
    # Encontrar o arquivo de log mais recente
    log_files = [f for f in os.listdir('.') if f.startswith('test_log_') and f.endswith('.json')]
    
    if not log_files:
        print("❌ Nenhum arquivo de log encontrado!")
        return
    
    # Pegar o mais recente
    latest_log = sorted(log_files)[-1]
    
    print(f"📊 RELATÓRIO DETALHADO DOS TESTES")
    print("=" * 60)
    print(f"📄 Arquivo: {latest_log}")
    
    try:
        with open(latest_log, 'r', encoding='utf-8') as f:
            log = json.load(f)
        
        # Informações da sessão
        session = log["test_session"]
        print(f"🔍 Session ID: {session['session_id']}")
        print(f"⏰ Timestamp: {session['start_time']}")
        print(f"🎯 Status Geral: {log['test_results']['summary']['overall_status'].upper()}")
        
        # Resumo dos testes
        summary = log["test_results"]["summary"]
        print(f"✅ Testes Aprovados: {summary['passed_tests']}/{summary['total_tests']}")
        print(f"❌ Testes Falharam: {summary['failed_tests']}")
        
        # Métricas do projeto
        print(f"\n📊 MÉTRICAS DO PROJETO:")
        metrics = log["metrics"]["project_files"]
        print(f"  📁 Arquivos Python: {metrics['total_python_files']}")
        print(f"  📝 Linhas de código: {metrics['total_lines_of_code']:,}")
        print(f"  🏗️  Classes: {metrics['total_classes']}")
        print(f"  ⚙️  Funções: {metrics['total_functions']}")
        
        # Cobertura
        print(f"\n📋 COBERTURA:")
        coverage = log["metrics"]["coverage"]
        files_pct = (coverage['required_files_found'] / coverage['required_files_total']) * 100
        patterns_pct = (coverage['required_patterns_found'] / coverage['required_patterns_total']) * 100
        
        print(f"  📂 Arquivos obrigatórios: {coverage['required_files_found']}/{coverage['required_files_total']} ({files_pct:.0f}%)")
        print(f"  🎯 Padrões obrigatórios: {coverage['required_patterns_found']}/{coverage['required_patterns_total']} ({patterns_pct:.0f}%)")
        
        # Detalhes dos testes individuais
        print(f"\n🧪 DETALHES DOS TESTES:")
        for i, test in enumerate(log["test_results"]["individual_tests"], 1):
            status_icon = "✅" if test["status"] == "passed" else "❌"
            print(f"  {i}. {status_icon} {test['test_name']}: {test['status'].upper()}")
            print(f"     {test['description']}")
        
        # Arquivos analisados
        print(f"\n📁 ARQUIVOS ANALISADOS:")
        python_test = next(t for t in log["test_results"]["individual_tests"] if t["test_name"] == "python_files_analysis")
        
        for file_path, details in python_test["details"]["files"].items():
            if details.get("analysis_success", False):
                lines = details.get("total_lines", 0)
                classes = len(details.get("classes", []))
                functions = len(details.get("functions", []))
                print(f"  ✅ {file_path}")
                print(f"     Linhas: {lines}, Classes: {classes}, Funções: {functions}")
            else:
                print(f"  ❌ {file_path}: {details.get('error', 'Erro desconhecido')}")
        
        print(f"\n" + "=" * 60)
        if log["test_results"]["summary"]["overall_status"] == "passed":
            print("🎉 TODOS OS TESTES PASSARAM!")
            print("✅ O projeto está pronto para execução no Databricks")
        else:
            print("⚠️  ALGUNS TESTES FALHARAM!")
            print("❌ Verifique os problemas reportados")
            
    except Exception as e:
        print(f"❌ Erro ao ler arquivo de log: {e}")

if __name__ == "__main__":
    show_test_results()
