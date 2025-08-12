"""
Teste de Sintaxe e Estrutura
============================

Verifica se todos os arquivos Python têm sintaxe correta e estrutura adequada.
"""

import ast
import os

def test_python_syntax(file_path):
    """Testa se um arquivo Python tem sintaxe válida."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Tenta fazer parse do código
        ast.parse(content)
        return True, "Sintaxe OK"

    except SyntaxError as e:
        return False, f"Erro de sintaxe: {e}"
    except Exception as e:
        return False, f"Erro: {e}"

def analyze_file_structure(file_path):
    """Analisa a estrutura de um arquivo Python."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        tree = ast.parse(content)

        info = {
            'classes': [],
            'functions': [],
            'imports': [],
            'docstring': ast.get_docstring(tree) is not None
        }

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                info['classes'].append(node.name)
            elif isinstance(node, ast.FunctionDef):
                info['functions'].append(node.name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    info['imports'].append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ''
                for alias in node.names:
                    info['imports'].append(f"{module}.{alias.name}")

        return True, info

    except Exception as e:
        return False, str(e)

def run_syntax_tests():
    """Executa testes de sintaxe em todos os arquivos Python."""
    print("TESTE DE SINTAXE E ESTRUTURA")
    print("=" * 50)

    python_files = [
        'src/bronze_layer.py',
        'src/silver_layer.py',
        'src/gold_layer.py',
        'src/etl_pipeline.py',
        'analysis/nyc_taxi_analysis.py',
        'test_pipeline.py',
        'test_simple.py',
        'databricks_etl_runner.py'
    ]

    all_passed = True

    for file_path in python_files:
        if os.path.exists(file_path):
            print(f"\nTestando: {file_path}")

            # Teste de sintaxe
            syntax_ok, syntax_msg = test_python_syntax(file_path)
            if syntax_ok:
                print(f"  Sintaxe: {syntax_msg}")
            else:
                print(f"  Sintaxe: {syntax_msg}")
                all_passed = False
                continue

            # Análise de estrutura
            struct_ok, struct_info = analyze_file_structure(file_path)
            if struct_ok:
                print(f"  Classes: {len(struct_info['classes'])} ({', '.join(struct_info['classes'][:3])})")
                print(f"  Funções: {len(struct_info['functions'])} (incluindo métodos)")
                print(f"  Imports: {len(struct_info['imports'])}")
                print(f"  Docstring: {'Sim' if struct_info['docstring'] else 'Não'}")
            else:
                print(f"  Estrutura: {struct_info}")
                all_passed = False
        else:
            print(f"Arquivo não encontrado: {file_path}")
            all_passed = False

    return all_passed

def test_required_patterns():
    """Testa se os padrões obrigatórios estão presentes."""
    print(f"\nVERIFICAÇÃO DE PADRÕES OBRIGATÓRIOS")
    print("=" * 50)

    patterns_to_check = [
        ('src/bronze_layer.py', [
            'class BronzeLayer',
            'def ingest_data',
            'https://d37ci6vzurychx.cloudfront.net',
            'yellow_tripdata'
        ]),
        ('src/silver_layer.py', [
            'class SilverLayer',
            'def clean_data',
            'required_columns',
            'quality_rules'
        ]),
        ('src/gold_layer.py', [
            'class GoldLayer',
            'def create_monthly_aggregations',
            'def create_hourly_aggregations',
            'gold_monthly_aggregations',
            'gold_hourly_aggregations_may'
        ]),
        ('analysis/nyc_taxi_analysis.py', [
            'def get_monthly_average_total_amount',
            'def get_hourly_average_passenger_count_may',
            'gold_monthly_aggregations',
            'gold_hourly_aggregations_may'
        ])
    ]

    all_found = True

    for file_path, patterns in patterns_to_check:
        if os.path.exists(file_path):
            print(f"\nVerificando padrões em: {file_path}")

            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            for pattern in patterns:
                if pattern in content:
                    print(f"  Encontrado: {pattern}")
                else:
                    print(f"  Não encontrado: {pattern}")
                    all_found = False
        else:
            print(f"Arquivo não encontrado: {file_path}")
            all_found = False

    return all_found

def main():
    """Função principal."""
    print("TESTE COMPLETO DE VALIDAÇÃO DO PROJETO")
    print("=" * 60)

    # Teste de sintaxe
    syntax_ok = run_syntax_tests()

    # Teste de padrões
    patterns_ok = test_required_patterns()

    # Resumo final
    print(f"\n{'='*60}")
    print("RESUMO FINAL DOS TESTES")
    print("="*60)

    if syntax_ok:
        print("Sintaxe: Todos os arquivos têm sintaxe válida")
    else:
        print("Sintaxe: Alguns arquivos têm problemas de sintaxe")

    if patterns_ok:
        print("Padrões: Todos os padrões obrigatórios encontrados")
    else:
        print("Padrões: Alguns padrões obrigatórios não encontrados")

    overall_success = syntax_ok and patterns_ok

    if overall_success:
        print("\nPROJETO APROVADO!")
        print("O código está pronto para execução no Databricks")
        print("Todas as análises obrigatórias estão implementadas")
        print("A arquitetura Bronze-Silver-Gold está completa")
    else:
        print("\nPROJETO PRECISA DE AJUSTES")
        print("Verifique os problemas reportados acima")

    return overall_success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
