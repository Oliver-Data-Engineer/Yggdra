import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any

from ..aws.S3Manager import S3Manager
from ..aws.AthenaManager import AthenaManager
from ..aws.GlueManager import GlueManager
from ..core.GenericLogger import GenericLogger

class Heimdall:
    """
    O Guardião Autônomo da Yggdra.
    Roda isoladamente do DataFactory para validar se as origens estão prontas.
    Lê a configuração diretamente do S3 usando os argumentos do Job.
    """
    
    def __init__(self, job_args: dict):
        self.job_args = job_args
        self.logger = GenericLogger(name="YGGDRA.Heimdall", level=job_args.get('log_level', 'INFO'))
        
        # =========================================================
        # 🛡️ 1. VALIDAÇÃO ANTECIPADA (Fail Fast)
        # =========================================================
        # Se falhar aqui, o pipeline morre em milissegundos sem acionar a AWS
        self._validate_owner_email()
        self._validate_arquiteture()

        # =========================================================
        # ☁️ 2. INICIALIZAÇÃO DE CONEXÕES (AWS)
        # =========================================================
        region = job_args.get('region_name')
        self.s3 = S3Manager(region_name=region, logger_name="YGGDRA.Heimdall")
        self.glue = GlueManager(region_name=region, logger_name="YGGDRA.Heimdall")
        self.athena = AthenaManager(region_name=region, logger_name="YGGDRA.Heimdall")
        
        self.is_valid = True
        self.security_logs: List[str] = []
        
       
        self.bucket_name = self.bucket_name = job_args.get('bucket_name') or self.s3.get_bucket_default()
        self.project_path = f"{job_args['db']}/{job_args['table_name']}"

    def _register_fault(self, msg: str):
        """Registra uma falha e tranca o portão."""
        self.is_valid = False
        self.security_logs.append(msg)
        self.logger.warning(f"🛡️ [FALHA BLOQUEADA] {msg}")

    def _get_origins_config_from_s3(self) -> List[Dict[str, Any]]:
        """Lê o arquivo JSON com a linhagem e as regras de origem direto do S3."""
        config_key = f"{self.project_path}/config/origens.json"
        self.logger.info(f"🛡️ [Heimdall] Buscando regras de origem em: s3://{self.bucket_name}/{config_key}")
        
        try:
            # Baixa o conteúdo do arquivo usando o Boto3 nativo do S3Manager
            response = self.s3.client.get_object(Bucket=self.bucket_name, Key=config_key)
            json_content = response['Body'].read().decode('utf-8')
            return json.loads(json_content)
            
        except self.s3.client.exceptions.NoSuchKey:
            self._register_fault(f"Arquivo de configuração de origens não encontrado no S3 ({config_key}).")
            return []
        except Exception as e:
            self._register_fault(f"Falha ao ler o JSON de origens no S3: {e}")
            return []

    # =========================================================================
    # 🕵️ MÉTODOS DE VALIDAÇÃO (Fluent Interface)
    # =========================================================================

    def evaluate_upstream_readiness(self) -> 'Heimdall':
        """
        1. Lê o JSON de origens do S3 (origens.json).
        2. Busca a última partição no AWS Glue para cada origem.
        3. Compara o Real com o Esperado (Baseado na defasagem/inferred_format).
        4. Imprime um log detalhado de sucesso ou falha para cada tabela.
        5. Retorna 'self' para permitir encadeamento (Fluent Interface).
        """
        self.logger.info("🛡️ [Heimdall] Iniciando avaliação geral de prontidão dos dados Upstream...")
        
        origens_config = self._get_origins_config_from_s3()
        resultados = []
        
        if not origens_config:
            self.logger.warning("🛡️ [Heimdall] Nenhum arquivo origens.json encontrado. Pulando etapa.")
            return self

        hoje = datetime.now()

        for source in origens_config:
            db = source.get('database')
            table = source.get('table')
            inferred_fmt = source.get('inferred_format')
            defasagem = source.get('expected_defasagem', 0)
            partition_keys = source.get('partition_keys', [])
            
            expected_partition = "N/A"
            last_partition = "VAZIO"
            passed = False

            try:
                # 1. Busca no Glue a última partição real da tabela de origem
                if partition_keys:
                    last_parts = self.glue.get_last_n_partitions(db=db, table=table, partition_keys=partition_keys, limit=1)
                    if last_parts:
                        last_partition = last_parts[0]

                # 2. Motor de Cálculo da Partição Esperada
                if inferred_fmt in ["%Y-%m-%d", "%Y%m%d"]:
                    # Formato Diário -> Subtrai Dias
                    target_date = hoje - relativedelta(days=defasagem)
                    expected_partition = target_date.strftime(inferred_fmt)
                    
                elif inferred_fmt in ["%Y-%m-01", "%Y%m01", "%Y-%m", "%Y%m", "%Y"]:
                    # Formato Mensal/Anual -> Subtrai Meses/Anos
                    target_date = hoje - relativedelta(months=defasagem)
                    expected_partition = target_date.strftime(inferred_fmt)
                    
                else:
                    # Fallback (Assume defasagem em dias no padrão YYYYMMDD se nulo)
                    target_date = hoje - relativedelta(days=defasagem)
                    inferred_fmt_fallback = "%Y%m%d" if not inferred_fmt else inferred_fmt
                    expected_partition = target_date.strftime(inferred_fmt_fallback)

                # 3. Veredito Lógico
                if last_partition != "VAZIO":
                    passed = str(last_partition) >= str(expected_partition)
                else:
                    passed = False

                # 4. Logs Interativos e Registro de Falhas
                if passed:
                    self.logger.info(
                        f"✅ [PASSOU] Origem: {db}.{table} | "
                        f"Alvo: {expected_partition} | Real: {last_partition} (Lag: {defasagem})"
                    )
                else:
                    self.logger.error(
                        f"❌ [ATRASADO] Origem: {db}.{table} | "
                        f"Alvo: {expected_partition} | Real: {last_partition} (Lag: {defasagem})"
                    )
                    # Registra a falha no disjuntor interno do Heimdall
                    self._register_fault(
                        f"Tabela de Origem atrasada ou vazia: '{db}.{table}'. "
                        f"Esperava: {expected_partition} | Encontrou: {last_partition}"
                    )

            except Exception as e:
                self.logger.error(f"⚠️ [ERRO TÉCNICO] Falha ao inspecionar origem {db}.{table}: {e}")
                self._register_fault(f"Falha técnica ao validar origem {db}.{table}")

            # 5. Compila o resultado para um possível uso posterior
            resultados.append({
                "database": db,
                "table": table,
                "last_partition": last_partition,
                "expected_partition": expected_partition,
                "passed": passed
            })

        self.logger.info("🛡️ [Heimdall] Varredura geral de origens concluída com sucesso.")
        
        # 💡 Guarda o relatório na instância da classe em vez de retornar a lista
        self.upstream_readiness_report = resultados 
        
        # Retorna o próprio Guardião para continuar a corrente!
        return self

    def authorize(self) -> bool:
        """
        Avalia o resultado das checagens encadeadas.
        Se is_valid == False, dispara um erro interrompendo o fluxo.
        """
        if not self.is_valid:
            self.logger.error("🛑 [Heimdall] O portão da Bifrost permanece fechado! Falhas detectadas:")
            for log in self.security_logs:
                self.logger.error(f"   ✖️ {log}")
                
            raise PermissionError(f"Heimdall bloqueou o pipeline. {len(self.security_logs)} origem(ns) não atendem aos requisitos de D-1/M-0.")
            
        self.logger.info("🟢 [Heimdall] Todas as validações passaram. Autorização concedida.")
        return True

    def _validate_owner_email(self):
        owner = self.args.get('owner')

        # domínios permitidos (pode crescer no futuro)
        allowed_domains = [
            "@itau-unibanco.com.br",  # principal
            "@itau.com.br",
            "@rede.com.br",
            "@corp.itau"
        ]

        email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'

        # valida tipo
        if not isinstance(owner, str):
            raise TypeError(
                "[ERRO DE VALIDAÇÃO] O parâmetro 'owner' deve ser uma string (email)"
            )

        # valida formato
        if not re.match(email_pattern, owner):
            raise ValueError(
                f"[ERRO DE VALIDAÇÃO] O parâmetro 'owner' deve ser um email válido. Valor recebido: '{owner}'"
            )

        # 🔥 valida domínio permitido
        if not any(owner.lower().endswith(domain) for domain in allowed_domains):
            raise ValueError(
                "[ERRO DE VALIDAÇÃO] Domínio de email não permitido. "
                f"Domínios aceitos: {', '.join(allowed_domains)}"
            )

    def _validate_arquiteture(self):
        """Validações estritas dos parâmetros de engenharia e arquitetura temporal."""
        
        # 1. Validação de Defasagem (Lag)
        defasagem = self.args.get('defasagem', 0)
        if not isinstance(defasagem, int) or defasagem < 0:
            raise ValueError(
                f"[ERRO DE VALIDAÇÃO] O parâmetro 'defasagem' deve ser um inteiro positivo. Valor recebido: {defasagem}"
            )

        # 2. Validação de Reprocessamento
        reprocessamento = self.args.get('reprocessamento', False)
        if not isinstance(reprocessamento, bool):
            raise TypeError(
                f"[ERRO DE VALIDAÇÃO] O parâmetro 'reprocessamento' deve ser booleano (True/False). Valor recebido: {reprocessamento}"
            )

        range_rep = self.args.get('range_reprocessamento', 0)
        if not isinstance(range_rep, int) or range_rep < 0:
            raise ValueError(
                f"[ERRO DE VALIDAÇÃO] O parâmetro 'range_reprocessamento' deve ser um inteiro positivo. Valor recebido: {range_rep}"
            )

        # 3. Validação do Tipo de Partição Lógica
        valid_partition_types = ['data', 'anomesdia', 'anomes', 'ano', 'mes']
        p_type = self.args.get('partition_type')
        if p_type and p_type.lower() not in valid_partition_types:
            raise ValueError(
                f"[ERRO DE VALIDAÇÃO] 'partition_type' inválido. Opções aceitas: {valid_partition_types}"
            )

        # 4. Validação da Janela Manual (dt_ini e dt_fim)
        dt_ini = self.args.get('dt_ini')
        dt_fim = self.args.get('dt_fim')
        
        if dt_ini and dt_fim:
            str_ini, str_fim = str(dt_ini), str(dt_fim)
            # Verifica se o início não é maior que o fim (funciona tanto para '20240101' quanto para '2024-01-01')
            if str_ini > str_fim:
                raise ValueError(
                    f"[ERRO DE VALIDAÇÃO] Incoerência temporal: 'dt_ini' ({str_ini}) não pode ser maior que 'dt_fim' ({str_fim})."
                )

    # 🔹 Validação central
    def _validate(self):
        required_fields = ['db', 'table_name', 'owner', 'partition_name', 'partition_type']
        
        # valida obrigatórios
        for field in required_fields:
            if not self.args.get(field):
                raise ValueError(f"[ERRO DE VALIDAÇÃO] Parâmetro obrigatório ausente: '{field}'")

        # valida regra: precisa de query OU path_sql_origem
        if not self.args.get('query') and not self.args.get('path_sql_origem'):
            raise ValueError(
                "[ERRO DE VALIDAÇÃO] É necessário informar 'query' ou 'path_sql_origem'"
            )

        # valida tipo
        if self.args.get('query') and not isinstance(self.args['query'], str):
            raise TypeError(
                "[ERRO DE VALIDAÇÃO] O parâmetro 'query' deve ser do tipo string"
            )

        if self.args.get('path_sql_origem') and not isinstance(self.args['path_sql_origem'], str):
            raise TypeError(
                "[ERRO DE VALIDAÇÃO] O parâmetro 'path_sql_origem' deve ser do tipo string"
            )

    def check_historical_upstream_readiness(
        self, 
        qtd_anomesdia: int = 1, 
        qtd_anomes: int = 1, 
        dt_referencia: str = None
    ) -> 'Heimdall':
        """
        Verifica a existência de partições históricas contínuas nas origens, 
        aplicando regras distintas para tabelas diárias e mensais.
        
        :param qtd_anomesdia: Quantidade de dias a recuar para tabelas de grão diário.
        :param qtd_anomes: Quantidade de meses a recuar para tabelas de grão mensal.
        :param dt_referencia: Data âncora opcional (ex: '20260325' ou '202603'). Se nulo, usa a data atual.
        :return: self (Fluent Interface)
        """
        self.logger.info(
            f"🛡️ [Heimdall] Varredura Histórica - Exigindo: {qtd_anomesdia} dia(s) e {qtd_anomes} mês(es). "
            f"(Âncora: {dt_referencia or 'Hoje'})"
        )
        
        # 1. Definição da Data Âncora Inteligente
        anchor_date = datetime.now()
        if dt_referencia:
            try:
                clean_val = re.sub(r'\D', '', str(dt_referencia))
                if len(clean_val) == 8:
                    anchor_date = datetime.strptime(clean_val, '%Y%m%d')
                elif len(clean_val) == 6:
                    anchor_date = datetime.strptime(clean_val, '%Y%m')
            except ValueError as e:
                self._register_fault(f"Formato de âncora inválido para verificação histórica: {e}")
                return self

        origens_config = self._get_origins_config_from_s3()
        if not origens_config:
            self.logger.warning("🛡️ [Heimdall] Nenhum ficheiro de origens encontrado para validar o histórico.")
            return self

        # 2. Varredura Inteligente por Origem
        for source in origens_config:
            db = source.get('database')
            table = source.get('table')
            inferred_fmt = source.get('inferred_format')
            defasagem = source.get('expected_defasagem', 0)
            partition_keys = source.get('partition_keys', [])

            if not partition_keys:
                continue

            # 3. 🧠 Identificação do Grão Lógico
            is_daily = inferred_fmt in ["%Y-%m-%d", "%Y%m%d"]
            is_monthly = inferred_fmt in ["%Y-%m-01", "%Y%m01", "%Y-%m", "%Y%m"]

            # Configura os limites baseados no grão
            if is_daily:
                n_partitions = qtd_anomesdia
                delta_unit = 'days'
            elif is_monthly:
                n_partitions = qtd_anomes
                delta_unit = 'months'
            else:
                # Fallback de segurança assume diário
                n_partitions = qtd_anomesdia
                delta_unit = 'days'
                inferred_fmt = "%Y%m%d" if not inferred_fmt else inferred_fmt

            # Se o utilizador pediu para não verificar este grão (ex: passou 0)
            if n_partitions <= 0:
                self.logger.debug(f"Pular origem '{table}' (verificação de {delta_unit} configurada para 0).")
                continue

            # 4. Motor Matemático: Gera a lista EXATA das partições esperadas
            expected_partitions = []
            for i in range(n_partitions):
                total_lag = defasagem + i
                
                if delta_unit == 'days':
                    target_date = anchor_date - relativedelta(days=total_lag)
                else:
                    target_date = anchor_date - relativedelta(months=total_lag)
                    
                expected_partitions.append(target_date.strftime(inferred_fmt))

            # 5. Busca as partições reais no Glue (limite elástico)
            search_limit = max(100, n_partitions + defasagem + 30)
            actual_partitions = self.glue.get_last_n_partitions(
                db=db, table=table, partition_keys=partition_keys, limit=search_limit
            )

            # 6. Interseção de Conjuntos (Quais estão em falta?)
            missing_partitions = [p for p in expected_partitions if p not in actual_partitions]

            if missing_partitions:
                self._register_fault(
                    f"Origem '{db}.{table}' sem profundidade histórica. "
                    f"Faltam {len(missing_partitions)} partição(ões) das {n_partitions} exigidas. (Ausentes: {missing_partitions[:3]}...)"
                )
            else:
                self.logger.info(f"✅ Histórico de '{db}.{table}' validado ({n_partitions} partições contínuas presentes).")

        return self
    
    def check_specific_origin(
        self,
        db: str,
        table: str,
        partition_type: str,
        defasagem: int = 0,
        partition_format: str = None,
        dt_referencia: str = None
    ) -> 'Heimdall':
        """
        Verifica a prontidão de uma tabela de origem específica de forma ad-hoc.
        Calcula a partição esperada baseada no tipo e na defasagem, sem depender do SourceGuardian.
        
        :param db: Banco de dados no Glue Catalog.
        :param table: Nome da tabela.
        :param partition_type: Grão lógico ('data', 'anomesdia', 'anomes', 'mes', 'ano').
        :param defasagem: Quantidade de recuos a aplicar.
        :param partition_format: (Opcional) Máscara customizada, ex: '%Y-%m-01'.
        :param dt_referencia: (Opcional) Data âncora no formato numérico.
        :return: self (Fluent Interface)
        """
        self.logger.info(f"🛡️ [Heimdall] Inspeção Ad-hoc: Verificando {db}.{table} (Tipo: {partition_type}, Defasagem: {defasagem})")

        # 1. Definição da Data Âncora
        anchor_date = datetime.now()
        if dt_referencia:
            clean_val = re.sub(r'\D', '', str(dt_referencia))
            try:
                if len(clean_val) >= 8:
                    anchor_date = datetime.strptime(clean_val[:8], '%Y%m%d')
                elif len(clean_val) >= 6:
                    anchor_date = datetime.strptime(clean_val[:6], '%Y%m')
            except ValueError:
                self._register_fault(f"Formato de âncora inválido para {db}.{table}: {dt_referencia}")
                return self

        # 2. Resolução do Formato e do Grão de Tempo
        p_type_lower = partition_type.lower()
        is_monthly = 'mes' in p_type_lower or p_type_lower == 'ano' or partition_format == '%Y-%m-01'
        
        # Mapeia formato padrão da arquitetura Yggdra se não for passado um explícito
        if not partition_format:
            formats = {
                "ano": "%Y",
                "mes": "%m",
                "anomes": "%Y%m",
                "anomesdia": "%Y%m%d",
                "data": "%Y-%m-%d"
            }
            partition_format = formats.get(p_type_lower, "%Y-%m-%d")

        # 3. Motor Matemático: Cálculo da Partição Esperada
        if is_monthly:
            if partition_format == '%Y-%m-01':
                anchor_date = anchor_date.replace(day=1)
            target_date = anchor_date - relativedelta(months=defasagem)
        else:
            target_date = anchor_date - relativedelta(days=defasagem)
            
        expected_partition = target_date.strftime(partition_format)

        # 4. Varredura AWS Glue
        try:
            tb_desc = self.glue.get_description_table(db=db, table=table)
            partition_keys = [p.get("Name") for p in tb_desc.get("PartitionKeys", [])]
            
            if not partition_keys:
                self._register_fault(f"A tabela {db}.{table} não é particionada. Validação temporal abortada.")
                return self

            last_parts = self.glue.get_last_n_partitions(db=db, table=table, partition_keys=partition_keys, limit=1)
            last_partition = last_parts[0] if last_parts else "VAZIO"

            # 5. Veredito de Segurança
            if last_partition != "VAZIO" and str(last_partition) >= str(expected_partition):
                self.logger.info(f"✅ Origem Ad-hoc '{db}.{table}' validada! (Real: {last_partition} | Exigido: {expected_partition}).")
            else:
                self._register_fault(
                    f"Origem Ad-hoc '{db}.{table}' está atrasada ou vazia. "
                    f"Exigido: {expected_partition} | Real: {last_partition}"
                )
                
        except Exception as e:
            self.logger.error(f"Erro ao verificar origem ad-hoc {db}.{table}: {e}")
            self._register_fault(f"Falha técnica ao acessar {db}.{table} no Glue Catalog.")

        return self
    
    def save_report(self) -> 'Heimdall':
        """
        Gera um relatório HTML sofisticado contendo o laudo de todas as verificações.
        Salva o ficheiro no S3 na pasta reports/heimdall/success ou error.
        Retorna 'self' para não quebrar a corrente (Fluent Interface).
        """
        self.logger.info("📊 [Heimdall] Compilando relatório visual de segurança (HTML)...")

        has_errors = not self.is_valid
        status_label = "🛑 BLOQUEADO (FALHAS DE PRÉ-REQUISITO)" if has_errors else "✅ AUTORIZADO (SUCESSO)"
        status_color = "#dc3545" if has_errors else "#28a745"

        # =====================================================================
        # 1. Montagem da Tabela de Origens (Geral)
        # =====================================================================
        tabela_html = ""
        readiness_data = getattr(self, 'upstream_readiness_report', [])
        
        if readiness_data:
            linhas = ""
            for res in readiness_data:
                cor_linha = "#d4edda" if res['passed'] else "#f8d7da"
                icone = "✅" if res['passed'] else "❌"
                linhas += f"""
                <tr style="background-color: {cor_linha}; color: #333;">
                    <td>{res['database']}.{res['table']}</td>
                    <td>{res['expected_partition']}</td>
                    <td>{res['last_partition']}</td>
                    <td style="text-align: center;">{icone}</td>
                </tr>
                """
            
            tabela_html = f"""
            <h3>🔍 Detalhamento das Origens (Upstream Geral)</h3>
            <table class="yggdra-table">
                <tr>
                    <th>Tabela de Origem</th>
                    <th>Partição Exigida (Alvo)</th>
                    <th>Partição Existente (Real)</th>
                    <th>Status</th>
                </tr>
                {linhas}
            </table>
            """

        # =====================================================================
        # 2. Montagem dos Erros Consolidados (Histórico, Ad-Hoc e Sintaxe)
        # =====================================================================
        erros_html = ""
        if has_errors:
            lista_erros = "".join([f"<li>{err}</li>" for err in self.security_logs])
            erros_html = f"""
            <div class="error-box">
                <h3>🚨 Auditoria de Falhas Detectadas</h3>
                <ul>{lista_erros}</ul>
            </div>
            """
        elif not readiness_data:
             erros_html = "<p><i>Nenhuma falha detectada. Pipeline 100% aderente aos requisitos.</i></p>"

        # =====================================================================
        # 3. Template HTML (Estilizado)
        # =====================================================================
        html_content = f"""
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; background-color: #f4f7f6; color: #333; padding: 20px; }}
                .container {{ background-color: #fff; padding: 30px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); max-width: 950px; margin: auto; }}
                h2 {{ color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
                .status-badge {{ font-size: 18px; font-weight: bold; color: white; background-color: {status_color}; padding: 12px 20px; border-radius: 6px; display: inline-block; margin-bottom: 20px; }}
                .metadata {{ background: #f9f9f9; padding: 15px; border-left: 4px solid #3498db; margin-bottom: 25px; border-radius: 4px; }}
                .yggdra-table {{ width: 100%; border-collapse: collapse; margin-bottom: 25px; font-size: 14px; }}
                .yggdra-table th, .yggdra-table td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                .yggdra-table th {{ background-color: #34495e; color: white; font-weight: normal; }}
                .error-box {{ background-color: #fff3f3; color: #d63031; border: 1px solid #fab1a0; padding: 20px; border-radius: 6px; margin-top: 20px; box-shadow: inset 0 0 5px rgba(214,48,49,0.1); }}
                .error-box ul {{ margin-bottom: 0; line-height: 1.6; }}
                .footer {{ margin-top: 40px; font-size: 12px; color: #95a5a6; text-align: center; border-top: 1px solid #ecf0f1; padding-top: 15px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h2>🛡️ Heimdall - Relatório de Pre-Flight Checks</h2>
                
                <div class="status-badge">{status_label}</div>
                
                <div class="metadata">
                    <b>🎯 Destino:</b> <code>{self.job_args.get('db')}.{self.job_args.get('table_name')}</code><br>
                    <b>⏰ Data da Verificação:</b> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}<br>
                    <b>👤 Owner:</b> {self.job_args.get('owner', 'N/A')}
                </div>

                {tabela_html}
                {erros_html}

                <div class="footer">
                    Gerado automaticamente por <b>Yggdra Data Platform</b><br>
                    <i>Módulo de Segurança e Observabilidade (Heimdall)</i>
                </div>
            </div>
        </body>
        </html>
        """

        # =====================================================================
        # 4. Gravação Física no S3
        # =====================================================================
        status_folder = "error" if has_errors else "success"
        prefix = f"{self.project_path}/reports/heimdall/{status_folder}"
        filename = f"heimdall_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            s3_uri = self.s3.write_text_file(
                bucket=self.bucket_name,
                prefix=prefix,
                filename=filename,
                content=html_content,
                extension="html"
            )
            self.logger.info(f"✅ Relatório Heimdall gerado e salvo em: {s3_uri}")
            self.report_s3_path = s3_uri # Opcional: Guarda o path caso precise usar na Exception depois
            
        except Exception as e:
            self.logger.error(f"❌ Falha ao tentar salvar o relatório HTML do Heimdall no S3: {e}")

        return self

