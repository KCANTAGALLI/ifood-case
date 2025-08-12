"""
Visualizador de Logs das Análises NYC Taxi
==========================================

Script para visualizar de forma amigável os logs JSON gerados das análises obrigatórias.
"""

import json
import os
import glob
from typing import Dict, Any
from datetime import datetime

class AnalysisLogViewer:
    """Visualizador dos logs de análise."""

    def __init__(self, logs_dir: str = "analysis_logs"):
        """
        Inicializa o visualizador.

        Args:
            logs_dir: Diretório contendo os logs JSON
        """
        self.logs_dir = logs_dir

    def find_latest_logs(self) -> Dict[str, str]:
        """
        Encontra os logs mais recentes.

        Returns:
            Dict com os caminhos dos logs mais recentes
        """
        if not os.path.exists(self.logs_dir):
            return {}

        # Buscar arquivos por tipo
        monthly_files = glob.glob(os.path.join(self.logs_dir, "monthly_analysis_log_*.json"))
        hourly_files = glob.glob(os.path.join(self.logs_dir, "hourly_analysis_log_*.json"))
        consolidated_files = glob.glob(os.path.join(self.logs_dir, "consolidated_analysis_report_*.json"))

        # Pegar os mais recentes (último na lista ordenada)
        latest_logs = {}
        if monthly_files:
            latest_logs['monthly'] = sorted(monthly_files)[-1]
        if hourly_files:
            latest_logs['hourly'] = sorted(hourly_files)[-1]
        if consolidated_files:
            latest_logs['consolidated'] = sorted(consolidated_files)[-1]

        return latest_logs

    def load_json_log(self, filepath: str) -> Dict[str, Any]:
        """
        Carrega um arquivo JSON de log.

        Args:
            filepath: Caminho para o arquivo JSON

        Returns:
            Dict com o conteúdo do log
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Erro ao carregar {filepath}: {e}")
            return {}

    def display_monthly_analysis(self, log_data: Dict[str, Any]) -> None:
        """Exibe a análise mensal de forma formatada."""

        print("\n" + "="*80)
        print("ANALISE 1: MEDIA DE VALOR TOTAL POR MES")
        print("="*80)

        print(f"\nPergunta:")
        print("   Qual a media de valor total (total_amount) recebido em um mes")
        print("   considerando todos os yellow taxis da frota?")

        print(f"\nDetalhes:")
        print(f"   - Periodo: {log_data.get('period', 'N/A')}")
        print(f"   - Fonte: {log_data.get('data_source', 'N/A')}")
        print(f"   - Execucao: {log_data.get('execution_timestamp', 'N/A')}")

        print(f"\nMetodologia:")
        methodology = log_data.get('methodology', {})
        print(f"   - Metrica: {methodology.get('metric', 'N/A')}")
        print(f"   - Nivel: {methodology.get('aggregation_level', 'N/A')}")
        print(f"   - Agrupamento: {methodology.get('group_by', 'N/A')}")

        print(f"\nResultados por Mes:")
        results = log_data.get('results', [])
        for result in results:
            print(f"   - {result['month_name']:>9}: R$ {result['avg_total_amount']:>7.2f} "
                  f"({result['total_trips']:>9,} viagens)")

        print(f"\nResumo Estatistico:")
        summary = log_data.get('summary', {})
        print(f"   - Media Geral: R$ {summary.get('overall_average', 0):.2f}")
        print(f"   - Total de Viagens: {summary.get('total_trips_analyzed', 0):,}")
        print(f"   - Maior Valor: {summary.get('highest_month', {}).get('month', 'N/A')} "
              f"(R$ {summary.get('highest_month', {}).get('value', 0):.2f})")
        print(f"   - Menor Valor: {summary.get('lowest_month', {}).get('month', 'N/A')} "
              f"(R$ {summary.get('lowest_month', {}).get('value', 0):.2f})")
        print(f"   - Tendencia: {summary.get('trend', 'N/A')}")

        print(f"\nQualidade dos Dados:")
        quality = log_data.get('data_quality', {})
        print(f"   - Registros Processados: {quality.get('records_processed', 0):,}")
        print(f"   - Registros Filtrados: {quality.get('records_filtered_out', 0):,}")
        print(f"   - Completude: {quality.get('data_completeness', 0):.1f}%")
        print(f"   - Outliers Removidos: {quality.get('outliers_removed', 0):,}")

    def display_hourly_analysis(self, log_data: Dict[str, Any]) -> None:
        """Exibe a análise horária de forma formatada."""

        print("\n" + "="*80)
        print("ANALISE 2: MEDIA DE PASSAGEIROS POR HORA (MAIO)")
        print("="*80)

        print(f"\nPergunta:")
        print("   Qual a media de passageiros (passenger_count) por cada hora do dia")
        print("   que pegaram taxi no mes de maio considerando todos os taxis da frota?")

        print(f"\nDetalhes:")
        print(f"   - Periodo: {log_data.get('period', 'N/A')}")
        print(f"   - Fonte: {log_data.get('data_source', 'N/A')}")
        print(f"   - Execucao: {log_data.get('execution_timestamp', 'N/A')}")

        print(f"\nResultados por Hora:")
        results = log_data.get('results', [])

        # Dividir em períodos para melhor visualização
        periods = [
            ("Madrugada (00h-05h)", results[0:6]),
            ("Manha (06h-11h)", results[6:12]),
            ("Tarde (12h-17h)", results[12:18]),
            ("Noite (18h-23h)", results[18:24])
        ]

        for period_name, period_data in periods:
            print(f"\n   {period_name}:")
            for result in period_data:
                print(f"      {result['hour_display']:>8}: {result['avg_passenger_count']:.2f} passageiros "
                      f"({result['total_trips']:>6,} viagens)")

        print(f"\nResumo Estatistico:")
        summary = log_data.get('summary', {})
        print(f"   - Media Geral: {summary.get('overall_average', 0):.2f} passageiros/viagem")
        print(f"   - Total de Viagens: {summary.get('total_trips_analyzed', 0):,}")
        print(f"   - Pico: {summary.get('peak_hour', {}).get('hour', 'N/A')} "
              f"({summary.get('peak_hour', {}).get('value', 0):.2f} passageiros)")
        print(f"   - Menor: {summary.get('lowest_hour', {}).get('hour', 'N/A')} "
              f"({summary.get('lowest_hour', {}).get('value', 0):.2f} passageiros)")

        print(f"\nPadroes Identificados:")
        patterns = log_data.get('patterns', {})
        for pattern_name, pattern_data in patterns.items():
            print(f"   - {pattern_name.replace('_', ' ').title()}: "
                  f"{pattern_data.get('hours', 'N/A')} - "
                  f"{pattern_data.get('avg_passengers', 0):.2f} passageiros")

    def display_consolidated_report(self, log_data: Dict[str, Any]) -> None:
        """Exibe o relatório consolidado."""

        print("\n" + "="*80)
        print("RELATORIO CONSOLIDADO - ANALISES OBRIGATORIAS")
        print("="*80)

        project_info = log_data.get('project_info', {})
        print(f"\nInformacoes do Projeto:")
        print(f"   - Nome: {project_info.get('name', 'N/A')}")
        print(f"   - Repositorio: {project_info.get('repository', 'N/A')}")
        print(f"   - Fonte dos Dados: {project_info.get('data_source', 'N/A')}")
        print(f"   - Periodo: {project_info.get('period_analyzed', 'N/A')}")
        print(f"   - Tecnologias: {', '.join(project_info.get('technology_stack', []))}")

        print(f"\nPrincipais Insights:")
        insights = log_data.get('key_insights', [])
        for i, insight in enumerate(insights, 1):
            print(f"   {i}. {insight}")

        print(f"\nResumo da Qualidade dos Dados:")
        quality = log_data.get('data_quality_summary', {})
        print(f"   - Total de Registros: {quality.get('total_records_processed', 0):,}")
        print(f"   - Completude Geral: {quality.get('overall_completeness', 0):.1f}%")
        print(f"   - Validacao: {'Aprovada' if quality.get('data_validation_passed') else 'Reprovada'}")

        colunas = quality.get('mandatory_columns_present', [])
        print(f"   - Colunas Obrigatorias: {', '.join(colunas)}")

        print(f"\nMetadados de Execucao:")
        metadata = log_data.get('execution_metadata', {})
        print(f"   - Versao do Pipeline: {metadata.get('pipeline_version', 'N/A')}")
        print(f"   - Versao do Spark: {metadata.get('spark_version', 'N/A')}")
        print(f"   - Ambiente: {metadata.get('execution_environment', 'N/A')}")
        print(f"   - Tempo de Processamento: {metadata.get('processing_time_seconds', 0):,} segundos")
        print(f"   - Uso de Memoria: {metadata.get('memory_usage_gb', 0):.1f} GB")

    def display_all_logs(self) -> None:
        """Exibe todos os logs encontrados."""

        print("NYC TAXI ANALYSIS - VISUALIZADOR DE LOGS")
        print("=" * 50)

        latest_logs = self.find_latest_logs()

        if not latest_logs:
            print("Nenhum log encontrado no diretorio:", self.logs_dir)
            return

        print(f"Diretorio: {self.logs_dir}")
        print(f"Logs encontrados: {len(latest_logs)}")

        # Exibir análise mensal
        if 'monthly' in latest_logs:
            monthly_data = self.load_json_log(latest_logs['monthly'])
            if monthly_data:
                self.display_monthly_analysis(monthly_data)

        # Exibir análise horária
        if 'hourly' in latest_logs:
            hourly_data = self.load_json_log(latest_logs['hourly'])
            if hourly_data:
                self.display_hourly_analysis(hourly_data)

        # Exibir relatório consolidado
        if 'consolidated' in latest_logs:
            consolidated_data = self.load_json_log(latest_logs['consolidated'])
            if consolidated_data:
                self.display_consolidated_report(consolidated_data)

        print("\n" + "="*80)
        print("VISUALIZACAO COMPLETA DOS LOGS")
        print("="*80)
        print(f"Arquivos processados:")
        for log_type, filepath in latest_logs.items():
            print(f"   - {log_type.title()}: {os.path.basename(filepath)}")

def main():
    """Função principal."""
    viewer = AnalysisLogViewer()
    viewer.display_all_logs()

if __name__ == "__main__":
    main()
