from datetime import datetime
from ..aws.S3Manager import S3Manager 
from ..aws.GlueManager import GlueManager 
from ..aws.AthenaManager import AthenaManager 
from ..core.GenericLogger import GenericLogger
from ..core.Clock import Clock
from ..core.DataUtils import DataUtils
from ..build.S3Arquiteture import S3Arquiteture
from ..observability.MetadataManager import MetadataManager
from ..observability.ReportManager import ReportManager
from ..build.SourceGuardian import SourceGuardian

import concurrent.futures
import json

class DataFactory:
    """
    Orquestrador principal do pipeline de dados (Yggdra Data Factory).
    
    Responsável por gerenciar o ciclo de vida completo de uma carga ETL/ELT na AWS.
    Ele automatiza a infraestrutura no S3, extrai a linhagem de dados, calcula
    janelas temporais de partição, executa processamento paralelo no Athena,
    e gera relatórios de observabilidade e metadados (Gêmeo Digital).

    Args:
        job_args (dict): Dicionário contendo as configurações de infraestrutura, 
                         regras de negócio e janelas temporais do pipeline.

    Workflow (Método .run()):
        1. Setup: Inicializa conexões (S3, Glue, Athena) e estrutura de pastas S3.
        2. Linhagem: Inspeciona a query SQL via SourceGuardian e mapeia origens.
        3. Backup: Salva o estado do script Glue e o config (job_args) em JSON.
        4. Particionamento: Calcula os dias/meses a processar via DataUtils.
        5. Execução: 
            - Se tabela não existe: Executa First Load (CTAS) + Salva DDL.
            - Se tabela existe: Executa em paralelo (UNLOAD) as partições.
        6. Encerramento: Gera relatório HTML rico e salva na pasta success/error.
        7. Retorno: Devolve um dicionário com o status global e URIs do S3.
    """

    def __init__(self, job_args: dict):
        self.job_args = job_args
        self.PRODUCT_NAME = "YGGDRA"

        # Core
        self.logger = GenericLogger(name=self.PRODUCT_NAME, level=job_args.get('log_level', 'INFO'))
        self.execution_timer = Clock()
        self.execution_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Managers
        self.s3 = None
        self.athena = None
        self.glue = None
        self.report = None

        # Runtime
        self.data_setup = {}
        self.partitions = []

        self.report_s3_path = None

    # ================================
    # 🚀 Setup Inicial
    # ================================
    def _initialize_managers(self):
        self.logger.info("Inicializando managers...")

        self.s3 = S3Manager(logger_name=self.PRODUCT_NAME)
        # CORREÇÃO: Usando self.job_args.get()
        self.athena = AthenaManager(
            region_name=self.job_args.get('region_name'),
            logger_name=self.PRODUCT_NAME
        )
        self.glue = GlueManager(
            region_name=self.job_args.get('region_name'),
            logger_name=self.PRODUCT_NAME
        )
        self.report = ReportManager(self.job_args)


    def _setup_environment(self):
        self.logger.info("Executando setup do ambiente (Bootstrap S3/SQL)...")
        arq = S3Arquiteture(self.job_args).run()
        self.data_setup = arq
        guardian = SourceGuardian(region_name=self.job_args.get('region_name'))
        lineage = guardian.map_upstream_lineage(self.data_setup['query'])
        
        # 2. 💡 INJETA A LINHAGEM NO RELATÓRIO
        self.report.set_lineage(lineage)

        # =================================================================
        # 📸 NOVO: Acionando o backup do código fonte e propriedades do Job
        # =================================================================
        job_name = self.job_args.get('job_name')
        if job_name:
            self.glue.backup_running_job_state(
                job_name=job_name,
                s3_manager=self.s3,
                target_bucket=self.data_setup['bucket'],
                target_project_prefix=self.data_setup['project_path']
            )
        else:
            self.logger.warning("Parâmetro 'job_name' não informado no job_args. Backup do script ignorado.")

    def _generate_partitions(self):
        self.logger.info("Calculando janela de partições...")

        # 🧠 INTEGRAÇÃO DA NOVA LÓGICA:
        # Puxa o 'partition_type' para a matemática. Se não existir (legado), usa o 'partition_name'.
        p_type = self.job_args.get('partition_type', self.job_args.get('partition_name'))
        
        self.partitions = DataUtils.generate_partitions(
            p_type=p_type,
            # Se não passados, assume o default do DataUtils (190001 - modo automático)
            dt_ini=self.job_args.get('dt_ini', 190001),
            dt_fim=self.job_args.get('dt_fim', 190001),
            reprocessamento=self.job_args.get('reprocessamento', False),
            range_reprocessamento=self.job_args.get('range_reprocessamento', 0),
            dia_corte=self.job_args.get('dia_corte'),
            defasagem=self.job_args.get('defasagem', 0),
            p_format=self.job_args.get('partition_format') # Injeção do nosso formato customizado
        )

        if not self.partitions:
            self.logger.warning("Nenhuma partição identificada. Encerrando pipeline.")
            return False

        self.logger.info(f"Total de {len(self.partitions)} partições identificadas para a fila.")
        self.logger.info(f"Fila: {self.partitions}")
        return True

    # ================================================================
    # 🧩 Operação Isolada (Incremental)
    # ================================================================
    def _process_single_partition(self, part: str) -> dict:
        """Processa o fluxo ETL de ponta a ponta para uma única partição (UNLOAD)."""
        self.logger.info(f" >>> Iniciando processamento da Partição: {part}")

        # 1. Limpeza preventiva no S3 (Usa estritamente o nome físico da coluna)
        self.s3.clean_partition(
            s3_uri=self.data_setup['structure']['data'],
            partition_names=self.job_args['partition_name'],
            partition_values=part
        )

        # 2. Expansão Inteligente de Datas
        parametros_sql = DataUtils.expand_date_variables(part)
        parametros_sql[self.job_args['partition_name']] = part

        # 3. Execução da Query (UNLOAD)
        resp = self.athena.unload_to_s3(
            sql=self.data_setup['query'],
            target_s3_path=self.data_setup['structure']['data'],
            database=self.job_args['db'],
            temp_s3=self.data_setup['structure']['temp'],
            partition_names=self.job_args['partition_name'],
            sql_params=parametros_sql
        )

        # 4. Atualização no Glue Catalog
        self.athena.manage_partition(
            db=self.job_args['db'],
            table=self.job_args['table_name'],
            partition_val=part
        )

        return resp

    # ================================================================
    # 🔁 Orquestrador Incremental Paralelo
    # ================================================================
    def _process_incremental(self, max_workers: int = 4):
        """Distribui as partições restantes em threads paralelas."""
        self.logger.info(f"🚀 MODO INCREMENTAL PARALELO: {len(self.partitions)} Partições a Processar. (Workers: {max_workers})")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_part = {
                executor.submit(self._process_single_partition, part): part 
                for part in self.partitions
            }

            for future in concurrent.futures.as_completed(future_to_part):
                part = future_to_part[future]
                try:
                    resp = future.result()
                    self.report.add_partition_result(
                        part, "Success", resp['elapsed_sec'], resp['query_id']
                    )
                    self.logger.info(f"✅ Partição {part} processada com sucesso em {resp['elapsed_sec']}s.")
                except Exception as e:
                    self.logger.error(f"❌ Falha na partição {part}: {e}")
                    self.report.add_error(f"Partição {part}", str(e))

    # ================================
    # 🧱 First Load (CTAS Híbrido)
    # ================================
    def _process_first_load(self):
        self.logger.info("MODO FIRST LOAD: Executando CTAS para criação da infraestrutura.")

        # 1. Extrai (Remove) a PRIMEIRA partição da lista
        p_inicial = self.partitions.pop(0)
        self.logger.info(f"Partição âncora selecionada para o CTAS: {p_inicial}")

        self.s3.clean_partition(
            s3_uri=self.data_setup['structure']['data'],
            partition_names=self.job_args['partition_name'],
            partition_values=p_inicial
        )

        # Expansão de variáveis para o CTAS
        parametros_sql_ctas = DataUtils.expand_date_variables(p_inicial)
        parametros_sql_ctas[self.job_args['partition_name']] = p_inicial

        resp = self.athena.create_table_as_select(
            sql=self.data_setup['query'],
            target_db=self.job_args['db'],
            target_table=self.job_args['table_name'],
            s3_path_target=self.data_setup['structure']['data'],
            temp_s3=self.data_setup['structure']['temp'],
            partition_names=self.job_args['partition_name'],
            sql_params=parametros_sql_ctas
        )

        self.report.add_partition_result(
            p_inicial, "First Load (CTAS)", resp['elapsed_sec'], resp['query_id']
        )

        # 2. Registra metadados e DDL da tabela recém-criada
        self._handle_metadata()
        self.logger.info("Tabela construída com sucesso no AWS Glue Catalog.")

        # 3. 🧠 O GATILHO: Se sobraram partições, manda para o processamento paralelo
        if self.partitions:
            self.logger.info(f"Redirecionando as {len(self.partitions)} partições restantes para processamento incremental.")
            self._process_incremental()

    # ================================
    # ⚙️ Configuração (Backup)
    # ================================
    def _save_job_configuration(self):
        """
        Salva o dicionário de configuração (job_args) como JSON no S3.
        Garante total rastreabilidade e reprodutibilidade do pipeline.
        """
        self.logger.info("💾 Realizando backup da configuração do Job no S3...")

        try:
            # 1. Formata o dicionário para JSON (default=str protege contra falhas de conversão de datas)
            config_payload = json.dumps(
                self.job_args, 
                indent=4, 
                ensure_ascii=False, 
                default=str
            )

            # 2. Extrai os caminhos padronizados gerados pelo S3Arquiteture
            bucket = self.data_setup['bucket']
            # Cria a sub-pasta /config dentro do diretório do projeto
            prefix = f"{self.data_setup['project_path']}/config"
            
            # Usamos o timestamp para não sobrescrever configurações de execuções anteriores
            filename = f"config_{self.execution_timestamp}"

            # 3. Grava fisicamente usando nosso wrapper S3Manager
            s3_path = self.s3.write_text_file(
                bucket_name=bucket,
                prefix=prefix,
                filename=filename,
                content=config_payload,
                extension="json"
            )

            self.logger.info(f"✅ Arquivo de configuração salvo com sucesso em: {s3_path}")

        except Exception as e:
            self.logger.error(f"❌ Falha ao salvar backup de configuração: {e}")
            # Dica de Sênior: Se falhar ao salvar log/config, apenas avise, mas NÃO quebre o ETL principal.


    def _save_execution_report(self) -> str:
        """
        Gera e salva o relatório final de execução no S3.
        Roteia para a sub-pasta /success ou /error dependendo do status do job.
        Salva o caminho na instância e o retorna.
        """
        
        self.logger.info("📊 Gerando e roteando relatório de execução...")

        if not getattr(self, 'report', None):
            self.logger.warning("Nenhum ReportManager instanciado. Pulando geração de relatório.")
            return ""

        try:
            # 1. Avalia o status (Com erro ou Sucesso)
            has_errors = len(self.report.errors) > 0
            status_folder = "error" if has_errors else "success"

            # 2. Gera o conteúdo HTML enriquecido
            html_content = self.report.generate_html()

            # 3. Define os caminhos de destino no S3
            bucket = self.data_setup.get('bucket')
            if not bucket:
                self.logger.error("Bucket não encontrado no data_setup. Impossível salvar relatório.")
                return ""

            prefix = f"{self.data_setup['project_path']}/reports/{status_folder}"
            filename = f"report_{self.execution_timestamp}"

            # 4. Salva fisicamente e captura a URI retornada pelo S3Manager
            s3_uri = self.s3.write_text_file(
                bucket_name=bucket,
                prefix=prefix,
                filename=filename,
                content=html_content,
                extension="html"
            )

            # 5. Salva na instância para uso posterior (ex: anexar em e-mail)
            self.report_s3_path = s3_uri

            icon = "🚨" if has_errors else "✅"
            self.logger.info(f"{icon} Relatório salvo com sucesso em: {s3_uri}")
            
            return s3_uri

        except Exception as e:
            self.logger.error(f"❌ Falha ao salvar o relatório de execução no S3: {e}")
            return ""
        

    def _handle_metadata(self):
        self.logger.info("Gerando metadata (Gêmeo Digital)...")

        ddl_info = self.athena.get_table_ddl(
            self.job_args['db'],
            self.job_args['table_name'],
            self.data_setup['structure']['temp']
        )

        meta_manager = MetadataManager(self.job_args)
        meta_manager.register_sql(self.data_setup['query'])
        meta_manager.artifacts["original_ddl"] = ddl_info["ddl"]
        meta_manager.register_artifacts(structure=self.data_setup['structure'])

        metadata_json = meta_manager.to_json()

        self.s3.write_text_file(
            bucket_name=self.data_setup['bucket'],
            prefix=f"{self.data_setup['project_path']}/metadata",
            filename="metadata",
            content=metadata_json,
            extension="json"
        )

    # ================================
    # 📊 Finalização
    # ================================
    def _finalize(self):
        # 1. Dispara a nova função inteligente de salvar o report HTML
        self._save_execution_report()
        self.logger.info(f"🏁 Job finalizado. Tempo total: {self.execution_timer.formatted}")


    def _backup_table_ddl(self):
        self.logger.info("Salvando backup do DDL da tabela...")

        # 1. Pede ao AthenaManager a string formatada
        ddl_sql_content = self.athena.get_formatted_ddl(
            db=self.job_args['db'],
            table=self.job_args['table_name'],
            temp_s3=self.data_setup['structure']['temp']
        )

        # 2. Usa o S3Manager para gravar o texto diretamente como um arquivo .sql
        s3_uri = self.s3.write_text_file(
            bucket_name=self.data_setup['bucket'],
            prefix=f"{self.data_setup['project_path']}/sql",
            filename=f"backup_ddl_{self.job_args['table_name']}",
            content=ddl_sql_content,
            extension="sql"
        )
        
        self.logger.info(f"✅ Arquivo SQL salvo com sucesso em: {s3_uri}")

    def _generate_run_summary(self, status: str, msg: str) -> dict:
        """Helper para construir o dicionário de retorno da execução."""
        # Tenta extrair a linhagem se ela existir no report
        lineage_data = []
        if getattr(self, 'report', None) and getattr(self.report, 'lineage', None):
            lineage_data = self.report.lineage

        return {
            "status": status,
            "message": msg,
            "execution_duration": self.execution_timer.formatted,
            "database": self.job_args.get('db'),
            "table_name": self.job_args.get('table_name'),
            "partitions_processed": len(self.partitions) if self.partitions else 0,
            "report_s3_path": self.report_s3_path, 
            "data_s3_path": self.data_setup.get('structure', {}).get('data'),
            "lineage": lineage_data,
            "errors": getattr(self.report, 'errors', []) if getattr(self, 'report', None) else []
        }

     # ================================
    # 🎬 Orquestrador Principal
    # ================================
    def run(self) -> dict:
        """
        Ponto de entrada do orquestrador.
        Executa todas as validações, prepara o ambiente e processa os dados.
        Retorna um dicionário com todos os metadados de resultado da execução.
        """
        self.logger.info(f"🚀 INICIANDO PRODUTO: {self.PRODUCT_NAME} DATA FACTORY")

        # 1. Variáveis de estado global (Garante que existam no escopo final)
        final_status = "SUCCESS"
        final_msg = "Processamento concluído."

        try:
            # 2. Setup Inicial
            self._initialize_managers()
            self._setup_environment()
            
            # 3. Backup de Configuração (Salva job_args como json no S3)
            self._save_job_configuration()

            # 4. Geração da Janela de Processamento
            if not self._generate_partitions():
                # Altera o status, mas NÃO retorna ainda. Deixa fluir para o finally.
                final_status = "SKIPPED"
                final_msg = "Nenhuma partição identificada."
            
            else:
                # 5. Só entra aqui se existirem partições
                self.execution_timer.start()

                # Roteamento Lógico (Incremental ou Full)
                if self.glue.table_exists(db=self.job_args['db'], table=self.job_args['table_name']):
                    self._process_incremental()
                else:
                    self._process_first_load()
                    self._backup_table_ddl()
                    
                self.execution_timer.stop()
                self.logger.info("Lógica de execução processada com sucesso.")
                
                # Avaliação do Status Global
                has_errors = getattr(self.report, 'errors', [])
                if has_errors:
                    final_status = "ERROR"

        except Exception as e:
            # Captura de erros graves que quebrem o orquestrador
            self.logger.critical(f"Falha crítica no DataFactory: {e}", exc_info=True)
            if self.report:
                self.report.add_error("GlobalOrchestrator", str(e))
                
            final_status = "CRITICAL_FAILURE"
            final_msg = str(e)

        finally:
            self._finalize()

       
        return self._generate_run_summary(status=final_status, msg=final_msg)
    