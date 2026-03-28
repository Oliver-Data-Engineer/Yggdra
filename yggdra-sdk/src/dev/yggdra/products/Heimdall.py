import json
import re
from datetime import datetime
from typing import List, Dict, Any
from dateutil.relativedelta import relativedelta

from ..aws.S3Manager import S3Manager
from ..aws.AthenaManager import AthenaManager
from ..aws.GlueManager import GlueManager
from ..core.GenericLogger import GenericLogger
from ..core.DataUtils import DataUtils


class Heimdall:
    """
    O Guardião Autônomo da Yggdra.
    Roda isoladamente do DataFactory para validar parâmetros, queries e dependências.
    Gera uma trilha de auditoria completa (Checklist) para o relatório final.
    """
    
    def __init__(self, job_args: dict):
        self.job_args = job_args
        self.logger = GenericLogger(name="YGGDRA", level=job_args.get('log_level', 'INFO'))
        
        self.is_valid = True
        self.security_logs: List[str] = []
        self.validation_checklist: List[Dict[str, Any]] = [] # 💡 NOVO: Trilha de Auditoria
        
        # 1. INICIALIZAÇÃO DE CONEXÕES (AWS)
        region = job_args.get('region_name')
        self.s3 = S3Manager(region_name=region, logger_name="YGGDRA.Heimdall")
        self.glue = GlueManager(region_name=region, logger_name="YGGDRA.Heimdall")
        self.athena = AthenaManager(region_name=region, logger_name="YGGDRA.Heimdall")
        
        # 2. MONTAGEM DE CAMINHOS
        self.bucket_name = job_args.get('bucket_name') or getattr(self.s3, 'get_bucket_default', lambda: "SEU_BUCKET_DEFAULT")()
        self.project_path = f"{job_args.get('db')}/{job_args.get('table_name')}"

        # 3. VALIDAÇÃO ESTRUTURAL (Registra no Checklist)
        self._validate_central()
        self._validate_owner_email()
        self._validate_arquiteture()

    def _register_fault(self, msg: str):
        """Disjuntor interno: Registra falha bloqueante, mas permite fluxo continuar até o authorize."""
        self.is_valid = False
        self.security_logs.append(msg)
        self.logger.warning(f"🛡️ [FALHA BLOQUEADA] {msg}")

    def _add_check(self, etapa: str, alvo: str, status: bool, detalhes: str):
        """💡 Adiciona o laudo de um teste específico na Trilha de Auditoria."""
        self.validation_checklist.append({
            "etapa": etapa,
            "alvo": alvo,
            "status": status,
            "detalhes": detalhes
        })

    # =========================================================================
    # 🕵️ MÉTODOS DE VALIDAÇÃO DE INFRAESTRUTURA
    # =========================================================================
    def _validate_central(self):
        """Valida campos obrigatórios da fábrica de dados."""
        required_fields = ['db', 'table_name', 'partition_name', 'partition_type']
        ausentes = [f for f in required_fields if not self.job_args.get(f)]
        
        if ausentes:
            msg = f"Parâmetros ausentes: {', '.join(ausentes)}"
            self._register_fault(f"[ERRO ESTRUTURAL] {msg}")
            self._add_check("Parâmetros Base", "Campos Obrigatórios", False, msg)
        else:
            p_name = self.job_args.get('partition_name')
            if not isinstance(p_name, (str, list)):
                self._register_fault("[ERRO ESTRUTURAL] 'partition_name' deve ser string ou lista [year, month, day]")
                self._add_check("Parâmetros Base", "Formato partition_name", False, "Tipo inválido")
            else:
                self._add_check("Parâmetros Base", "Campos Obrigatórios", True, "Todos os parâmetros vitais estão presentes")

        query_ok = bool(self.job_args.get('query')) or bool(self.job_args.get('path_sql_origem'))
        if not query_ok:
            self._register_fault("[ERRO ESTRUTURAL] Necessário 'query' ou 'path_sql_origem'")
            self._add_check("Parâmetros Base", "Origem SQL", False, "Nenhuma instrução SQL mapeada")
        else:
            self._add_check("Parâmetros Base", "Origem SQL", True, "Instrução SQL mapeada com sucesso")

    def _validate_owner_email(self):
        owner = self.job_args.get('owner')
        allowed_domains = ["@itau-unibanco.com.br", "@itau.com.br", "@rede.com.br", "@corp.itau"]
        
        if not owner or not isinstance(owner, str) or '@' not in owner:
            self._register_fault("[GOVERNANÇA] Email de Owner inválido ou ausente.")
            self._add_check("Governança", "Email Owner", False, f"Recebido: '{owner}'")
            return

        if not any(owner.lower().endswith(d) for d in allowed_domains):
            self._register_fault(f"[GOVERNANÇA] Domínio não permitido: {owner}")
            self._add_check("Governança", "Email Owner", False, "Domínio fora da Whitelist corporativa")
            return

        self._add_check("Governança", "Email Owner", True, f"Aprovado: {owner}")

    def _validate_arquiteture(self):
        """Validações estritas dos parâmetros temporais."""
        # Defasagem
        defasagem = self.job_args.get('defasagem', 0)
        if not isinstance(defasagem, int) or defasagem < 0:
            self._register_fault(f"[ARQUITETURA] Defasagem inválida: {defasagem}")
            self._add_check("Config. Temporal", "Defasagem (Lag)", False, "Deve ser inteiro >= 0")
        else:
            self._add_check("Config. Temporal", "Defasagem (Lag)", True, f"Lag configurado para {defasagem}")

        # Tipo de Partição
        valid_types = ['data', 'anomesdia', 'anomes', 'ano', 'mes']
        p_type = self.job_args.get('partition_type')
        if p_type and p_type.lower() not in valid_types:
            self._register_fault(f"[ARQUITETURA] partition_type inválido: {p_type}")
            self._add_check("Config. Temporal", "Tipo Partição", False, f"Opções: {valid_types}")
        else:
            self._add_check("Config. Temporal", "Tipo Partição", True, f"Mapeado: {p_type}")

    # =========================================================================
    # 🌐 MÉTODOS DE DADOS E DEPENDÊNCIAS
    # =========================================================================
    def _get_origins_config_from_s3(self) -> List[Dict[str, Any]]:
        config_key = f"{self.project_path}/config/origens.json"
        try:
            response = self.s3.client.get_object(Bucket=self.bucket_name, Key=config_key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            return []

    def check_query_health(self) -> 'Heimdall':
        self.logger.info("🛡️ Validando sintaxe da Query (EXPLAIN)...")
        
        if not self.job_args.get('db') or not self.job_args.get('table_name'):
            return self # Já barrou no _validate_central

        query = self.job_args.get('query')
        if not query or not str(query).strip():
            sql_prefix = f"{self.project_path}/sql"
            filename = f"{self.job_args.get('table_name')}.sql"
            try:
                if hasattr(self.s3, 'get_content_sql'):
                    query = self.s3.get_content_sql(bucket=self.bucket_name, prefix=sql_prefix, filename=filename)
                else:
                    query = self.s3.client.get_object(Bucket=self.bucket_name, Key=f"{sql_prefix}/{filename}")['Body'].read().decode('utf-8')
            except Exception:
                self._register_fault("SQL não enviado no payload e não encontrado no S3.")
                self._add_check("Validação SQL", "Busca da Query", False, "Arquivo ausente")
                return self

        q_teste = str(query).replace('{anomesdia}', '20260101').replace('{anomes}', '202601').replace('{data}', '2026-01-01')
        
        try:
            res = self.athena.validate_query(sql=q_teste, database=self.job_args.get('db'), temp_s3=f"s3://{self.bucket_name}/{self.project_path}/temp/")
            if res.get('is_valid'):
                self._add_check("Validação SQL", "Sintaxe e Catálogo", True, "Aprovado via AWS Athena EXPLAIN")
            else:
                erro = str(res.get('error', 'Erro desconhecido')).split("FAILED:")[-1].strip()
                self._register_fault(f"SQL Inválido: {erro[:200]}")
                self._add_check("Validação SQL", "Sintaxe e Catálogo", False, erro)
        except Exception as e:
            self._register_fault(f"Erro no teste do Athena: {e}")
            self._add_check("Validação SQL", "Serviço Athena", False, str(e))

        return self

    def evaluate_upstream_readiness(self) -> 'Heimdall':
            origens = self._get_origins_config_from_s3()
            if not origens:
                self._add_check("Prontidão Upstream", "origens.json", True, "Nenhuma dependência mapeada (Tabela Isolada)")
                return self

            p_type = self.job_args.get('partition_type', 'data')

            # =====================================================================
            # 💡 NOVO: Motor de Simulação Exata da Janela de Execução
            # =====================================================================
            self.logger.debug("Simulando a matriz exata de partições que serão executadas neste Job...")
            
            # Pega as datas puras (datetime) que a DataFactory vai processar
            base_dates = DataUtils._get_base_dates(
                p_type=p_type,
                dt_ini=self.job_args.get('dt_ini', 190001),
                dt_fim=self.job_args.get('dt_fim', 190001),
                reprocessamento=self.job_args.get('reprocessamento', False),
                range_reprocessamento=self.job_args.get('range_reprocessamento', 0),
                dia_corte=self.job_args.get('dia_corte'),
                defasagem=self.job_args.get('defasagem', 0),
                p_format=self.job_args.get('partition_format')
            )

            if not base_dates:
                self.logger.warning("⚠️ Nenhuma partição a ser processada nesta janela.")
                self._add_check("Prontidão Upstream", "Janela de Execução", True, "Nenhuma partição gerada para os parâmetros atuais.")
                return self

            # Gera os nomes para o log ficar legível
            target_partitions_log = [DataUtils.format_partition(d, p_type, self.job_args.get('partition_format')) for d in base_dates]
            self.logger.info(f"🛡️ [Heimdall] Simulando execução para {len(base_dates)} partição(ões). Alvo inicial: {target_partitions_log[0]}")

            # =====================================================================
            # Varredura de Origens (Garantindo que CADA partição exigida existe)
            # =====================================================================
            for src in origens:
                db, table = src.get('database'), src.get('table')
                fmt = src.get('inferred_format', '%Y%m%d')
                lag = src.get('expected_defasagem', 0)
                keys = src.get('partition_keys', [])
                
                if not keys:
                    continue

                try:
                    # 1. Motor Matemático: Aplica o Lag/Defasagem em cada data exata do loop
                    expected_src_partitions = set()
                    for dt in base_dates:
                        if "01" in fmt or "%m" in fmt and "%d" not in fmt: 
                            target_dt = dt - relativedelta(months=lag)
                        else: 
                            target_dt = dt - relativedelta(days=lag)
                        
                        # Salva a partição exata exigida formatada para a origem atual
                        expected_src_partitions.add(target_dt.strftime(fmt))
                    
                    # Ordena a lista de exigências
                    expected_src_partitions = sorted(list(expected_src_partitions))

                    # 2. Busca partições reais no Glue (Com folga para não falhar a paginação)
                    search_limit = max(100, len(expected_src_partitions) + lag + 30)
                    reais = self.glue.get_last_n_partitions(db=db, table=table, partition_keys=keys, limit=search_limit)

                    # 3. Interseção de Conjuntos (O que eu preciso VS O que eu tenho)
                    faltantes = [p for p in expected_src_partitions if p not in reais]

                    if not faltantes:
                        self._add_check(
                            "Prontidão Upstream", 
                            f"{db}.{table}", 
                            True, 
                            f"Pronto! Cobre as {len(expected_src_partitions)} partições exigidas. (Última: {expected_src_partitions[-1]})"
                        )
                    else:
                        self._register_fault(f"Atraso em {db}.{table}. Faltam {len(faltantes)} partições da janela simulada. (Ausentes: {faltantes[:3]})")
                        self._add_check(
                            "Prontidão Upstream", 
                            f"{db}.{table}", 
                            False, 
                            f"Atrasado. Faltam: {faltantes[:2]}"
                        )
                        
                except Exception as e:
                    self._register_fault(f"Erro lendo {db}.{table}: {e}")
                    self._add_check("Prontidão Upstream", f"{db}.{table}", False, f"Falha de Catálogo: {e}")

            return self

    def check_historical_upstream_readiness(self, qtd_anomesdia: int = 1, qtd_anomes: int = 1, dt_referencia: str = None) -> 'Heimdall':
        origens = self._get_origins_config_from_s3()
        if not origens: return self

        anchor_date = datetime.now()
        if dt_referencia:
            try:
                c_val = re.sub(r'\D', '', str(dt_referencia))
                anchor_date = datetime.strptime(c_val[:8], '%Y%m%d') if len(c_val) >= 8 else datetime.strptime(c_val[:6], '%Y%m')
            except ValueError:
                self._add_check("Histórico (Backfill)", "Data Âncora", False, "Formato inválido")
                return self

        for src in origens:
            db, table, keys = src.get('database'), src.get('table'), src.get('partition_keys', [])
            if not keys: continue

            fmt, lag = src.get('inferred_format', '%Y%m%d'), src.get('expected_defasagem', 0)
            n_parts, delta = (qtd_anomes, 'months') if '01' in fmt or '%m' in fmt and '%d' not in fmt else (qtd_anomesdia, 'days')

            if n_parts <= 0: continue

            esperadas = [(anchor_date - relativedelta(**{delta: lag + i})).strftime(fmt) for i in range(n_parts)]
            reais = self.glue.get_last_n_partitions(db=db, table=table, partition_keys=keys, limit=max(100, n_parts + lag + 30))
            faltantes = [p for p in esperadas if p not in reais]

            if faltantes:
                self._register_fault(f"Histórico incompleto em {db}.{table}. Faltam: {len(faltantes)}")
                self._add_check("Histórico (Backfill)", f"{db}.{table}", False, f"Exigido: {n_parts}. Faltam: {faltantes[:2]}")
            else:
                self._add_check("Histórico (Backfill)", f"{db}.{table}", True, f"Todas as {n_parts} partições presentes")

        return self
    
    def check_specific_origin(self, db: str, table: str, partition_type: str, defasagem: int = 0, partition_format: str = None, dt_referencia: str = None) -> 'Heimdall':
        anchor_date = datetime.now()
        if dt_referencia:
            try:
                c_val = re.sub(r'\D', '', str(dt_referencia))
                anchor_date = datetime.strptime(c_val[:8], '%Y%m%d') if len(c_val) >= 8 else datetime.strptime(c_val[:6], '%Y%m')
            except ValueError: pass

        fmt = partition_format or {"ano": "%Y", "mes": "%m", "anomes": "%Y%m", "anomesdia": "%Y%m%d", "data": "%Y-%m-%d"}.get(partition_type.lower(), "%Y-%m-%d")
        delta = 'months' if 'mes' in partition_type.lower() or 'ano' in partition_type.lower() else 'days'
        if partition_format == '%Y-%m-01': anchor_date = anchor_date.replace(day=1)
        
        target = (anchor_date - relativedelta(**{delta: defasagem})).strftime(fmt)

        try:
            keys = [p.get("Name") for p in self.glue.get_description_table(db=db, table=table).get("PartitionKeys", [])]
            last_part = self.glue.get_last_n_partitions(db=db, table=table, partition_keys=keys, limit=1)[0] if keys else "VAZIO"

            if last_part != "VAZIO" and str(last_part) >= str(target):
                self._add_check("Validação Ad-Hoc", f"{db}.{table}", True, f"Real: {last_part} (Alvo: {target})")
            else:
                self._register_fault(f"Ad-Hoc {db}.{table} atrasado. Real: {last_part} | Alvo: {target}")
                self._add_check("Validação Ad-Hoc", f"{db}.{table}", False, f"Real: {last_part} | Alvo: {target}")
        except Exception as e:
            self._register_fault(f"Falha ao validar ad-hoc {db}.{table}")
            self._add_check("Validação Ad-Hoc", f"{db}.{table}", False, f"Erro AWS: {e}")

        return self

    # =========================================================================
    # 📝 RELATÓRIO E AUTORIZAÇÃO
    # =========================================================================
    def save_report(self) -> 'Heimdall':
        has_errors = not self.is_valid
        status_label = "🛑 BLOQUEADO" if has_errors else "✅ AUTORIZADO"
        status_color = "#dc3545" if has_errors else "#28a745"

        # 💡 NOVO: Montagem da Tabela da Trilha de Auditoria (Checklist)
        linhas_checklist = ""
        for check in self.validation_checklist:
            cor = "#d4edda" if check['status'] else "#f8d7da"
            icone = "✔️ Aprovado" if check['status'] else "❌ Reprovado"
            linhas_checklist += f"""
            <tr style="background-color: {cor};">
                <td><b>{check['etapa']}</b></td>
                <td><code>{check['alvo']}</code></td>
                <td>{icone}</td>
                <td><small>{check['detalhes']}</small></td>
            </tr>
            """

        auditoria_html = f"""
        <h3>📋 Auditoria Detalhada de Validações</h3>
        <table class="yggdra-table">
            <tr><th>Etapa</th><th>Alvo Validado</th><th>Status</th><th>Detalhes do Teste</th></tr>
            {linhas_checklist}
        </table>
        """

        html_content = f"""
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; background-color: #f4f7f6; color: #333; padding: 20px; }}
                .container {{ background-color: #fff; padding: 30px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); max-width: 1000px; margin: auto; }}
                h2 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
                .status-badge {{ font-size: 18px; font-weight: bold; color: white; background-color: {status_color}; padding: 12px 20px; border-radius: 6px; margin-bottom: 20px; display: inline-block; }}
                .metadata {{ background: #f9f9f9; padding: 15px; border-left: 4px solid #3498db; margin-bottom: 25px; font-size: 14px; }}
                .yggdra-table {{ width: 100%; border-collapse: collapse; margin-bottom: 25px; font-size: 14px; }}
                .yggdra-table th, .yggdra-table td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
                .yggdra-table th {{ background-color: #34495e; color: white; font-weight: normal; }}
                .footer {{ margin-top: 40px; font-size: 12px; color: #95a5a6; text-align: center; border-top: 1px solid #eee; padding-top: 15px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>🛡️ Heimdall - Trilha de Auditoria (Pre-Flight)</h2>
                <div class="status-badge">{status_label}</div>
                <div class="metadata">
                    <b>Destino:</b> <code>{self.job_args.get('db')}.{self.job_args.get('table_name')}</code> | 
                    <b>Verificado em:</b> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
                </div>
                {auditoria_html}
                <div class="footer">Documento Oficial de Compliance - <b>Yggdra Data Platform</b></div>
            </div>
        </body>
        </html>
        """

        prefix = f"{self.project_path}/heimdall/reports/{'error' if has_errors else 'success'}"
        filename = f"heimdall_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            s3_uri = self.s3.write_text_file(
                bucket_name=self.bucket_name, prefix=prefix, filename=filename, content=html_content, extension=".html"
            )
            self.logger.info(f"✅ Relatório Oficial salvo em: {s3_uri}")
            self.report_s3_path = s3_uri 
        except Exception as e:
            self.logger.error(f"❌ Falha ao salvar no S3: {e}")

        return self

    def authorize(self) -> bool:
        if not self.is_valid:
            raise PermissionError(f"Pipeline bloqueado! Encontrados {len(self.security_logs)} erros. Relatório: {getattr(self, 'report_s3_path', 'N/A')}")
        self.logger.info("🟢 [Heimdall] Todas as validações aprovadas.")
        return True