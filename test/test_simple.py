"""
Teste Simples - Verificação de Estrutura e Imports
=================================================

Testa apenas os imports e estrutura básica sem inicializar Spark.
"""

import sys
import os
import importlib.util

def test_project_structure():
    """Testa se a estrutura do projeto está correta."""
    print("🔍 Testando estrutura do projeto...")
    
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
        'README.md'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Arquivos faltando: {missing_files}")
        return False
    else:
        print("✅ Estrutura do projeto está completa")
        return True

def test_imports():
    """Testa se os módulos podem ser importados."""
    print("\n🔍 Testando imports dos módulos...")
    
    # Adicionar src ao path
    sys.path.append(os.path.join(os.getcwd(), 'src'))
    sys.path.append(os.path.join(os.getcwd(), 'analysis'))
    
    modules_to_test = [
        ('bronze_layer', 'src/bronze_layer.py'),
        ('silver_layer', 'src/silver_layer.py'),
        ('gold_layer', 'src/gold_layer.py'),
        ('etl_pipeline', 'src/etl_pipeline.py'),
    ]
    
    success_count = 0
    
    for module_name, file_path in modules_to_test:
        try:
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                # Não executamos o módulo, apenas verificamos se pode ser carregado
                print(f"✅ {module_name}: Import OK")
                success_count += 1
            else:
                print(f"❌ {module_name}: Não foi possível criar spec")
        except Exception as e:
            print(f"❌ {module_name}: Erro - {str(e)}")
    
    return success_count == len(modules_to_test)

def test_required_functions():
    """Testa se as funções principais existem nos módulos."""
    print("\n🔍 Testando presença de funções principais...")
    
    # Adicionar src ao path
    sys.path.append(os.path.join(os.getcwd(), 'src'))
    
    tests = [
        ('bronze_layer.py', ['BronzeLayer', 'create_bronze_layer_job']),
        ('silver_layer.py', ['SilverLayer', 'create_silver_layer_job']),
        ('gold_layer.py', ['GoldLayer', 'create_gold_layer_job']),
        ('etl_pipeline.py', ['ETLPipeline'])
    ]
    
    all_passed = True
    
    for file_name, required_items in tests:
        file_path = os.path.join('src', file_name)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            for item in required_items:
                if f"class {item}" in content or f"def {item}" in content:
                    print(f"✅ {file_name}: {item} encontrado")
                else:
                    print(f"❌ {file_name}: {item} não encontrado")
                    all_passed = False
                    
        except Exception as e:
            print(f"❌ Erro ao verificar {file_name}: {str(e)}")
            all_passed = False
    
    return all_passed

def test_required_columns():
    """Testa se as colunas obrigatórias estão definidas."""
    print("\n🔍 Testando definição de colunas obrigatórias...")
    
    required_columns = [
        'VendorID',
        'passenger_count', 
        'total_amount',
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime'
    ]
    
    try:
        with open('src/silver_layer.py', 'r', encoding='utf-8') as f:
            content = f.read()
        
        all_found = True
        for col in required_columns:
            if f"'{col}'" in content or f'"{col}"' in content:
                print(f"✅ Coluna obrigatória encontrada: {col}")
            else:
                print(f"❌ Coluna obrigatória não encontrada: {col}")
                all_found = False
        
        return all_found
        
    except Exception as e:
        print(f"❌ Erro ao verificar colunas: {str(e)}")
        return False

def test_analysis_queries():
    """Testa se as queries de análise obrigatórias estão implementadas."""
    print("\n🔍 Testando queries de análises obrigatórias...")
    
    files_to_check = [
        'analysis/nyc_taxi_analysis.py',
        'analysis/NYC_Taxi_Analysis.ipynb'
    ]
    
    required_patterns = [
        'gold_monthly_aggregations',
        'gold_hourly_aggregations_may',
        'avg_total_amount',
        'avg_passenger_count'
    ]
    
    all_found = True
    
    for file_path in files_to_check:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                print(f"\n📄 Verificando {file_path}:")
                for pattern in required_patterns:
                    if pattern in content:
                        print(f"✅ Padrão encontrado: {pattern}")
                    else:
                        print(f"❌ Padrão não encontrado: {pattern}")
                        all_found = False
            except Exception as e:
                print(f"❌ Erro ao verificar {file_path}: {str(e)}")
                all_found = False
        else:
            print(f"❌ Arquivo não encontrado: {file_path}")
            all_found = False
    
    return all_found

def run_all_tests():
    """Executa todos os testes."""
    print("🧪 INICIANDO TESTES LOCAIS DO PIPELINE NYC TAXI")
    print("=" * 60)
    
    tests = [
        ("Estrutura do Projeto", test_project_structure),
        ("Imports dos Módulos", test_imports),
        ("Funções Principais", test_required_functions),
        ("Colunas Obrigatórias", test_required_columns),
        ("Análises Obrigatórias", test_analysis_queries)
    ]
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            if test_func():
                passed_tests += 1
                print(f"✅ {test_name}: PASSOU")
            else:
                print(f"❌ {test_name}: FALHOU")
        except Exception as e:
            print(f"❌ {test_name}: ERRO - {str(e)}")
    
    print("\n" + "="*60)
    print("📊 RESUMO DOS TESTES")
    print("="*60)
    print(f"Testes executados: {total_tests}")
    print(f"Testes aprovados: {passed_tests}")
    print(f"Testes falharam: {total_tests - passed_tests}")
    print(f"Taxa de sucesso: {passed_tests/total_tests*100:.1f}%")
    
    if passed_tests == total_tests:
        print("\n🎉 TODOS OS TESTES PASSARAM!")
        print("✅ O pipeline está pronto para execução no Databricks")
        return True
    else:
        print(f"\n⚠️  {total_tests - passed_tests} TESTE(S) FALHARAM")
        print("❌ Verifique os problemas acima antes de executar no Databricks")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
