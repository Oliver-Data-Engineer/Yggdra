from datetime import datetime
import pandas as pd
import re

class ReportManager:
    """
    Gerencia a construção de relatórios de execução ricos (HTML/Markdown).
    Centraliza métricas de processamento, linhagem de dados e Data Quality.
    """
    
    def __init__(self, job_args: dict):
        self.start_time = datetime.now()
        self.job_args = job_args
        self.partitions_results = []
        self.errors = []
        self.lineage = []
        self.PRODUCT_NAME = "YGGDRA FÁBRICA DE SOT"

    def add_partition_result(self, partition: str, status: str, elapsed: float, query_id: str, row_count: int = 0):
        """
        Registra o resultado de cada partição processada.
        
        Args:
            partition (str): Valor da partição (ex: '20240325').
            status (str): Status retornado pelo orquestrador (Success, Empty, Error).
            elapsed (float): Tempo de execução em segundos.
            query_id (str): ID da execução no AWS Athena.
            row_count (int): Quantidade de linhas gravadas (Data Quality).
        """
        # Mapeamento de Ícones e Status
        status_display = status
        if status == "Success":
            status_display = "✅ Success" if row_count > 0 else "⚠️ Success (Empty)"
        elif "Empty" in status:
            status_display = f"⚠️ {status}"
        elif "First Load" in status:
            status_display = f"🏗️ {status}"
        elif "Error" in status or "Fail" in status:
            status_display = f"❌ {status}"

        self.partitions_results.append({
            "Partição": f"{partition}",
            "Status": status_display,
            "Linhas Gravadas": row_count,  # Mantido como int para o Pandas
            "Tempo (s)": f"{elapsed}s",
            "Query ID": query_id
        })

    def add_error(self, step: str, message: str):
        """Registra falhas críticas ou alertas de qualidade no processo."""
        self.errors.append({
            "Etapa": step, 
            "Erro": str(message),
            "Timestamp": datetime.now().strftime('%H:%M:%S')
        })

    def set_lineage(self, lineage_data: list):
        """Injeta os dados de origem mapeados pelo SourceGuardian."""
        self.lineage = lineage_data

    def _get_css(self):
        """Retorna o CSS embutido para o relatório HTML."""
        return """
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6; background-color: #f4f7f9; padding: 20px; }
            .container { width: 95%; max-width: 1100px; margin: auto; background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.08); }
            .header { background-color: #232f3e; color: white; padding: 20px; border-radius: 8px 8px 0 0; margin: -30px -30px 25px -30px; }
            .header h2 { margin: 0; font-size: 22px; }
            .status-sucesso { color: #28a745; font-weight: bold; }
            .status-erro { color: #dc3545; font-weight: bold; }
            .yggdra-table { width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 13px; border: 1px solid #eee; }
            .yggdra-table th { background-color: #f8f9fa; color: #555; border: 1px solid #dee2e6; padding: 12px; text-align: left; }
            .yggdra-table td { border: 1px solid #dee2e6; padding: 10px; }
            .yggdra-table tr:nth-child(even) { background-color: #fafafa; }
            .yggdra-table tr:hover { background-color: #f1f4f9; }
            .param-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 15px; margin-bottom: 25px; }
            .param-box { background: #fdfdfd; padding: 15px; border-radius: 8px; border: 1px solid #eee; border-left: 5px solid #232f3e; }
            .error-box { background-color: #fff5f5; border-left: 5px solid #dc3545; padding: 15px; margin-top: 10px; border-radius: 4px; color: #721c24; }
            .footer { margin-top: 40px; font-size: 12px; color: #888; text-align: center; border-top: 1px solid #eee; padding-top: 15px; }
            a { color: #007bff; text-decoration: none; }
            a:hover { text-decoration: underline; }
            code { background: #f1f1f1; padding: 2px 5px; border-radius: 3px; font-family: monospace; }
        </style>
        """

    def generate_html(self) -> str:
        """Gera a string HTML completa com tabelas de processamento e linhagem."""
        end_time = datetime.now()
        duration = round((end_time - self.start_time).total_seconds(), 2)
        has_errors = len(self.errors) > 0
        
        status_label = "⚠️ FINALIZADO COM ERROS" if has_errors else "✅ SUCESSO"
        status_class = "status-erro" if has_errors else "status-sucesso"

        # 1. Tabela de Partições (Destino)
        table_html = "<p><i>Nenhuma partição processada nesta execução.</i></p>"
        if self.partitions_results:
            df = pd.DataFrame(self.partitions_results)
            
            # Formatação Data Quality: Milhar padrão BR e destaque para ZERO
            df['Linhas Gravadas'] = df['Linhas Gravadas'].apply(
                lambda x: f"<b>{x:,}</b>".replace(",", ".") if x > 0 else '<span style="color: #e67e22; font-weight: bold;">⚠️ 0 (Vazio)</span>'
            )
            
            # Formatação de Links do Athena
            if 'Query ID' in df.columns:
                df['Query ID'] = df['Query ID'].apply(
                    lambda x: f'<a href="https://console.aws.amazon.com/athena/home#query/history/{x}" target="_blank">link_athena</a>'
                )
            
            table_html = df.to_html(index=False, escape=False, classes="yggdra-table")

        # 2. Tabela de Linhagem (Origens)
        lineage_html = "<p><i>Linhagem não disponível para esta tabela.</i></p>"
        if self.lineage:
            df_l = pd.DataFrame(self.lineage)
            cols = ["database", "table", "last_partition_value", "expected_defasagem", "inferred_format"]
            available_cols = [c for c in cols if c in df_l.columns]
            lineage_html = df_l[available_cols].to_html(index=False, classes="yggdra-table")

        # 3. Bloco de Erros/Alertas
        errors_html = ""
        if has_errors:
            errors_html = "<h3>🚨 Falhas e Alertas de Execução</h3>"
            for e in self.errors:
                errors_html += f'<div class="error-box">[{e["Timestamp"]}] <b>{e["Etapa"]}:</b> {e["Erro"]}</div>'

        # 4. Composição Final
        return f"""
        <html>
        <head>
            <meta charset="utf-8">
            <title>Relatório Yggdra - {self.job_args.get('table_name')}</title>
            {self._get_css()}
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>🌳 {self.PRODUCT_NAME}</h2>
                    <span>Relatório de Orquestração de Dados</span>
                </div>
                
                <p><b>Status da Execução:</b> <span class="{status_class}">{status_label}</span></p>

                <div class="param-grid">
                    <div class="param-box">
                        <b>📍 Tabela Alvo</b><br>
                        <code>{self.job_args.get('db')}.{self.job_args.get('table_name')}</code><br>
                        <b>Owner:</b> {self.job_args.get('owner', 'N/A')}
                    </div>
                    <div class="param-box">
                        <b>⏱️ Performance</b><br>
                        <b>Duração:</b> {duration}s<br>
                        <b>Início:</b> {self.start_time.strftime('%H:%M:%S')}
                    </div>
                    <div class="param-box">
                        <b>⚙️ Configuração</b><br>
                        <b>Partição:</b> {self.job_args.get('partition_name')} ({self.job_args.get('partition_type')})<br>
                        <b>Lag:</b> {self.job_args.get('defasagem', 0)}
                    </div>
                </div>

                <h3>🗂️ Resultados por Partição (Data Quality)</h3>
                {table_html}

                <h3>🔗 Linhagem de Dados (Upstream)</h3>
                {lineage_html}

                {errors_html}

                <div class="footer">
                    <p>Relatório gerado automaticamente pelo motor <b>{self.PRODUCT_NAME}</b><br>
                    Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
                </div>
            </div>
        </body>
        </html>
        """

    def generate_markdown(self) -> str:
        """Gera uma versão simplificada em Markdown para logs de console."""
        df = pd.DataFrame(self.partitions_results)
        # Formatação simples para MD
        df['Linhas Gravadas'] = df['Linhas Gravadas'].apply(lambda x: f"{x:,}".replace(",", "."))
        
        md = f"### 🕒 Resumo YGGDRA - {self.job_args.get('table_name')}\n"
        md += f"- **Status:** {'✅ Sucesso' if not self.errors else '❌ Erro'}\n"
        md += f"- **Duração:** {round((datetime.now() - self.start_time).total_seconds(), 2)}s\n\n"
        md += df.to_markdown(index=False)
        return md