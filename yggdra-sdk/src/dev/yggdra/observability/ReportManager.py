from datetime import datetime
import pandas as pd 

class ReportManager:
    """
    Gerencia a construção de um relatório de execução em Markdown.
    """
    def __init__(self, job_args: dict):
        self.start_time = datetime.now()
        self.job_args = job_args
        self.partitions_results = []
        self.errors = []
        self.summary = {}

    def add_partition_result(self, partition: str, status: str, elapsed: float, query_id: str):
        """Registra o resultado de cada partição do loop."""
        self.partitions_results.append({
            "Partição": f"`{partition}`",
            "Status": "✅ Sucesso" if status == "Success" else "❌ Falha",
            "Tempo (s)": f"{elapsed}s",
            "Query ID": f"[{query_id}](https://console.aws.amazon.com/athena/home#query/history/{query_id})"
        })

    def add_error(self, step: str, message: str):
        """Registra falhas críticas no processo."""
        self.errors.append({"Etapa": step, "Erro": message})

    def generate_markdown(self) -> str:
        """Constrói a string final em Markdown."""
        end_time = datetime.now()
        duration = round((end_time - self.start_time).total_seconds(), 2)
        
        # Cabeçalho e Metadados
        md = f"# 🕒 Relatório de Execução - YGGDRA FÁBRICA DE SOT\n"
        md += f"**Status Global:** {'✅ SUCESSO' if not self.errors else '⚠️ FINALIZADO COM ERROS'}\n\n"
        
        md += "### 📋 Parâmetros do Job\n"
        md += f"- **Tabela:** `{self.job_args['db']}.{self.job_args['table_name']}`\n"
        md += f"- **Tipo Partição:** `{self.job_args['partition_name']}`\n"
        md += f"- **Início:** `{self.start_time.strftime('%Y-%m-%d %H:%M:%S')}`\n"
        md += f"- **Duração Total:** `{duration}s`\n\n"

        # Tabela de Partições (usando Pandas para conversão rápida para MD)
        if self.partitions_results:
            md += "### 🗂️ Detalhes do Processamento\n"
            df = pd.DataFrame(self.partitions_results)
            md += df.to_markdown(index=False)
            md += "\n\n"

        # Seção de Erros
        if self.errors:
            md += "### 🚨 Erros Encontrados\n"
            for err in self.errors:
                md += f"- **{err['Etapa']}:** {err['Erro']}\n"
            md += "\n"

        md += "---\n*Relatório gerado automaticamente pelo motor YGGDRA FÁBRICA DE SOT*"
        return md   
    
    def _get_css(self):
        """Retorna o estilo visual do relatório."""
        return """
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6; }
            .container { width: 90%; margin: auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; }
            .header { background-color: #232f3e; color: white; padding: 15px; border-radius: 5px 5px 0 0; }
            .status-sucesso { color: #28a745; font-weight: bold; }
            .status-erro { color: #dc3545; font-weight: bold; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; }
            th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
            th { background-color: #f4f4f4; color: #555; }
            tr:nth-child(even) { background-color: #fafafa; }
            .footer { margin-top: 30px; font-size: 0.8em; color: #777; border-top: 1px solid #eee; padding-top: 10px; }
            .error-box { background-color: #fff5f5; border-left: 5px solid #dc3545; padding: 10px; margin-top: 10px; }
        </style>
        """
    
    def set_lineage(self, lineage_data: list):
            """Recebe o output do SourceGuardian para enriquecer o relatório."""
            self.lineage = lineage_data

    def generate_html(self) -> str:
        """Constrói a string final em HTML estilizado com linhagem e configurações avançadas."""
        end_time = datetime.now()
        duration = round((end_time - self.start_time).total_seconds(), 2)
        has_errors = len(self.errors) > 0
        
        status_label = "⚠️ FINALIZADO COM ERROS" if has_errors else "✅ SUCESSO"
        status_class = "status-erro" if has_errors else "status-sucesso"

        # -------------------------------------------------------------
        # 1. Tabela de Resultados das Partições
        # -------------------------------------------------------------
        table_html = "<p>Nenhuma partição processada.</p>"
        if getattr(self, 'partitions_results', None):
            df = pd.DataFrame(self.partitions_results)
            if 'Query ID' in df.columns:
                df['Query ID'] = df['Query ID'].apply(
                    lambda x: f'<a href="https://console.aws.amazon.com/athena/home#query/history/{x}" target="_blank">{x}</a>'
                )
            # A classe 'yggdra-table' pode ser estilizada no seu _get_css()
            table_html = df.to_html(index=False, escape=False, classes="yggdra-table")

        # -------------------------------------------------------------
        # 2. Tabela de Linhagem (Origens - SourceGuardian)
        # -------------------------------------------------------------
        lineage_html = "<p><i>Nenhuma linhagem mapeada ou configurada.</i></p>"
        if getattr(self, 'lineage', None):
            df_lineage = pd.DataFrame(self.lineage)
            # Renomear colunas para apresentação no HTML
            rename_map = {
                "database": "Database",
                "table": "Tabela Origem",
                "partition_keys": "Chaves de Partição",
                "last_partition_value": "Última Partição Lida",
                "inferred_format": "Formato Inferido"
            }
            df_lineage.rename(columns=rename_map, inplace=True)
            
            # Filtra apenas as colunas amigáveis para exibir
            cols_to_show = [c for c in rename_map.values() if c in df_lineage.columns]
            lineage_html = df_lineage[cols_to_show].to_html(index=False, escape=False, classes="yggdra-table")

        # -------------------------------------------------------------
        # 3. Montagem dos Erros
        # -------------------------------------------------------------
        errors_html = ""
        if has_errors:
            errors_html = "<h3>🚨 Erros Encontrados</h3>"
            for err in self.errors:
                errors_html += f'<div class="error-box"><b>{err["Etapa"]}:</b> {err["Erro"]}</div>'

        # -------------------------------------------------------------
        # 4. Template HTML Final
        # -------------------------------------------------------------
        html = f"""
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                /* Garanta que seu _get_css() tenha formatação para .yggdra-table */
                {self._get_css()}
                .yggdra-table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
                .yggdra-table th, .yggdra-table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                .yggdra-table th {{ background-color: #f2f2f2; }}
                .grid-params {{ display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 20px; }}
                .param-box {{ background: #f9f9f9; padding: 10px; border-radius: 5px; min-width: 250px; border-left: 4px solid #4CAF50; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>🌳 Yggdra Data Factory - Relatório de Execução</h2>
                </div>
                <p><b>Status Global:</b> <span class="{status_class}">{status_label}</span></p>
                
                <h3>📋 Parâmetros da Construção</h3>
                <div class="grid-params">
                    <div class="param-box">
                        <h4>Destino</h4>
                        <b>Tabela:</b> <code>{self.job_args.get('db')}.{self.job_args.get('table_name')}</code><br>
                        <b>Owner:</b> {self.job_args.get('owner', 'N/A')}
                    </div>
                    <div class="param-box">
                        <h4>Regras de Partição (Temporal)</h4>
                        <b>Coluna Física:</b> <code>{self.job_args.get('partition_name', 'N/A')}</code><br>
                        <b>Grão Lógico:</b> <code>{self.job_args.get('partition_type', 'N/A')}</code><br>
                        <b>Formato Customizado:</b> <code>{self.job_args.get('partition_format', 'Padrão')}</code><br>
                        <b>Defasagem (Lag):</b> <code>{self.job_args.get('defasagem', 0)}</code>
                    </div>
                    <div class="param-box">
                        <h4>Orquestração</h4>
                        <b>Reprocessamento:</b> {'Ativo' if self.job_args.get('reprocessamento') else 'Inativo'}<br>
                        <b>Janela Inicial:</b> {self.job_args.get('dt_ini', 'Auto')}<br>
                        <b>Janela Final:</b> {self.job_args.get('dt_fim', 'Auto')}<br>
                        <b>Duração Total:</b> {duration}s
                    </div>
                </div>

                <h3>🔗 Linhagem de Dados (Origens Inspecionadas)</h3>
                {lineage_html}

                <h3>🗂️ Detalhes do Processamento (Destino)</h3>
                {table_html}

                {errors_html}

                <div class="footer">
                    <p>Este é um relatório automático gerado pelo motor <b>YGGDRA DATA FACTORY</b>.<br>
                    <i>Data de geração: {end_time.strftime('%d/%m/%Y %H:%M:%S')}</i></p>
                </div>
            </div>
        </body>
        </html>
        """
        return html