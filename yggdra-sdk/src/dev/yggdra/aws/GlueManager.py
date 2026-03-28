import boto3
import time
import copy
from typing import Optional, Dict, List, Any , Callable
from .AwsClient import AWSClient
from ..core.GenericLogger import GenericLogger
from ..core.Clock import Clock
from ..core.Utils import Utils 
from botocore.exceptions import ClientError
import concurrent.futures
import json

class GlueManager(AWSClient):
    """
    Classe responsável por abstrair operações no AWS Glue Data Catalog.
    Agora monitora a performance de busca de metadados e partições.
    """

    def __init__(self, region_name: str = "us-east-2", logger_name: str = "YGGDRA"):
        super().__init__(service_name="glue", region_name=region_name)
        self.logger = GenericLogger(name=f'{logger_name}.Glue', propagate=True)
        self.logger.info(f"GlueManager inicializado na região: {region_name}")

    # --- MÉTODOS DE APOIO ---

    def _sanitize_name(self, name: str) -> str:
        return name.strip().lower() if name else ""

    def parse_partition_value(self, value: str, data_type: str) -> Any:
        data_type = data_type.lower()
        try:
            if not value or value.lower() == 'null':
                return ""
            if 'int' in data_type or 'bigint' in data_type:
                return int(value)
            if 'decimal' in data_type or 'float' in data_type or 'double' in data_type:
                return float(value)
            return value
        except ValueError:
            return value

    # --- VERIFICAÇÕES (Com Medição) ---

    def table_exists(self, db: str, table: str) -> bool:
        """Verifica existência e loga o tempo de resposta do Catálogo."""
        cronometro = Clock()
        cronometro.start()
        
        db = self._sanitize_name(db)
        table = self._sanitize_name(table)
        
        try:
            self.client.get_table(DatabaseName=db, Name=table)
            self.logger.debug(f"Verificação de tabela {db}.{table} em {cronometro.stop()}s")
            return True
        except self.client.exceptions.EntityNotFoundException:
            return False
        except Exception as e:
            self.logger.error(f"Erro ao verificar tabela {db}.{table}: {e}")
            raise

    # --- METADADOS (Com Medição) ---

    def get_description_table(self, db: str, table: str) -> Dict:
        """Busca metadados completos da tabela."""
        cronometro = Clock()
        cronometro.start()
        
        db = self._sanitize_name(db)
        table = self._sanitize_name(table)
        
        try:
            resp = self.client.get_table(DatabaseName=db, Name=table)
            self.logger.info(f"Descrição de {db}.{table} obtida em {cronometro.stop()}s")
            return resp["Table"]
        except Exception as e:
            self.logger.error(f"Erro ao obter metadados de {db}.{table}: {e}")
            raise

    def get_partition_values(self, db: str, table: str) -> List[Dict]:
        """Varre o catálogo em busca de todas as partições (Suporta Paginação)."""
        cronometro = Clock()
        cronometro.start()
        
        db = self._sanitize_name(db)
        table = self._sanitize_name(table)
        
        partitions = []
        try:
            paginator = self.client.get_paginator('get_partitions')
            for page in paginator.paginate(DatabaseName=db, TableName=table):
                partitions.extend(page.get('Partitions', []))
            
            self.logger.info(f"Total de {len(partitions)} partições obtidas para {db}.{table} em {cronometro.formatted}")
            return partitions
        except Exception as e:
            self.logger.error(f"Erro ao listar partições em {cronometro.stop()}s: {e}")
            raise

    def get_last_n_partitions(self, db: str, table: str, limit: int = 3, partition_keys: Optional[List[str]] = None) -> List[str]:
        """
        Busca as últimas N partições registradas no AWS Glue Catalog para uma tabela.
        Trata nativamente partições compostas (ex: ['2026', '03'] -> '2026/03').
        
        :param db: Nome do banco de dados no Glue.
        :param table: Nome da tabela.
        :param limit: Quantidade de partições para retornar (Padrão: 3).
        :param partition_keys: (Opcional) Lista de chaves para log/filtro avançado.
        :return: Lista com os valores das últimas N partições formatadas.
        """
        self.logger.debug(f"🔍 Buscando as últimas {limit} partições para {db}.{table}...")
        
        try:
            # 1. Instancia o paginador nativo do boto3
            paginator = self.client.get_paginator('get_partitions')
            
            # 2. Performance: ExcludeColumnSchema=True evita baixar todo o esquema da tabela a cada página
            page_iterator = paginator.paginate(
                DatabaseName=db,
                TableName=table,
                ExcludeColumnSchema=True
            )
            
            all_partitions = []
            
            # 3. Varre as páginas retornadas pela AWS
            for page in page_iterator:
                for partition in page.get('Partitions', []):
                    # O Glue retorna uma lista (ex: ['2024', '03'] ou ['2024-03-01'])
                    vals = partition.get('Values', [])
                    
                    if vals:
                        # 💡 A MÁGICA DA INTEGRAÇÃO: Normaliza partições múltiplas e simples
                        # Transforma listas do Glue em strings padronizadas para o Heimdall/DataUtils
                        joined_val = "/".join([str(v) for v in vals])
                        all_partitions.append(joined_val)
            
            # 4. Ordenação Lexicográfica no Client-Side
            # O Glue não garante a ordem no get_partitions, então ordenamos a lista localmente.
            # reverse=True garante que as mais recentes (ex: 2026/12) fiquem no topo.
            all_partitions.sort(reverse=True)
            
            # 5. Fatiamento (Slice)
            ultimas_particoes = all_partitions[:limit]
            
            self.logger.debug(f"✅ Partições encontradas: {ultimas_particoes}")
            return ultimas_particoes

        except self.client.exceptions.EntityNotFoundException:
            self.logger.warning(f"⚠️ Tabela {db}.{table} não encontrada no Glue Catalog (Pode ser o First Load).")
            return []
        except Exception as e:
            self.logger.error(f"❌ Erro ao buscar partições de {db}.{table}: {e}")
            return []
            
    def get_last_partition(self, db: str, table: str, **kwargs) -> Optional[str]:
        """Wrapper de retrocompatibilidade. Retorna apenas a partição mais recente."""
        ultimas = self.get_last_n_partitions(db, table, limit=1)
        return ultimas[0] if ultimas else None

        

    # --- INTEGRAÇÃO SQL/LINHAGEM (O Coração do Vigia) ---

    def extract_tables_info(self, sql: str) -> List[Dict]:
        """Extrai linhagem e metadados de todas as origens detectadas no SQL."""
        cronometro_total = Clock()
        cronometro_total.start()
        
        self.logger.info("Iniciando varredura de linhagem e metadados...")
        try:
            origens = Utils.get_origens_sql(sql)
            resultados = []

            for origem in origens:
                # Medição individual por tabela de origem
                cron_tab = Clock()
                cron_tab.start()
                
                table = self._sanitize_name(origem['name'])
                db = self._sanitize_name(origem['db'])

                if not self.table_exists(db, table):
                    self.logger.warning(f"Origem {db}.{table} não encontrada no Data Catalog.")
                    continue

                tb_desc = self.get_description_table(db, table)
                p_keys = [p.get("Name") for p in tb_desc.get("PartitionKeys", [])]
                p_types = [p.get("Type") for p in tb_desc.get("PartitionKeys", [])]

                last_part = None
                if p_keys:
                    last_part = self.get_last_partition(db, table, p_keys, p_types)

                resultados.append({
                    "name": table,
                    "db": db,
                    "path": origem.get('path'),
                    "partition_keys": p_keys,
                    "partition_types": p_types,
                    "last_update_partition": last_part,
                    "fetch_time": cron_tab.stop()
                })

            self.logger.info(f"Processo de Linhagem concluído para {len(resultados)} tabelas em {cronometro_total.formatted}")
            return resultados
        except Exception as e:
            self.logger.error(f"Falha na linhagem após {cronometro_total.stop()}s: {e}", exc_info=True)
            raise

    def execute_in_parallel(self, task_func: Callable, items: List[Any], max_workers: int = 5) -> List[Dict[str, Any]]:
        """
        Motor genérico de paralelização para a Yggdra.
        Recebe uma função e uma lista de itens, distribuindo o processamento em múltiplas threads.
        
        :param task_func: A função que será executada para cada item.
        :param items: Lista de inputs (pode ser lista de dicionários, strings, etc).
        :param max_workers: Número máximo de threads simultâneas (Cuidado com throttling da AWS).
        :return: Lista com o status e resultado de cada execução.
        """
        self.logger.info(f"Iniciando processamento paralelo com {max_workers} workers para {len(items)} itens.")
        
        results = []
        
        # Usamos ThreadPoolExecutor pois as chamadas do Boto3 são I/O Bound
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            
            # Mapeia cada futuro (thread) para o seu respectivo item original
            future_to_item = {executor.submit(task_func, item): item for item in items}
            
            # as_completed garante que vamos processando os resultados assim que cada thread termina
            for future in concurrent.futures.as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    # Tenta capturar o retorno da função task_func
                    data = future.result()
                    results.append({
                        "item": item,
                        "status": "SUCCESS",
                        "data": data
                    })
                    self.logger.debug(f"Item processado com sucesso em paralelo: {item}")
                    
                except Exception as exc:
                    self.logger.error(f"❌ Falha no processamento paralelo do item {item}: {exc}")
                    results.append({
                        "item": item,
                        "status": "FAILED",
                        "error": str(exc)
                    })
                    
        self.logger.info("Processamento paralelo concluído.")
        return results
    
    def backup_running_job_state(self, job_name: str, s3_manager, target_bucket: str, target_project_prefix: str) -> dict:
        """
        Captura as propriedades atuais do Glue Job e o código fonte (Script),
        salvando ambos como artefatos de auditoria no S3.
        
        :param job_name: Nome do Glue Job na AWS.
        :param s3_manager: Instância da classe S3Manager para operações de cópia/escrita.
        :param target_bucket: Bucket de destino do seu projeto.
        :param target_project_prefix: Caminho base do projeto (ex: workspace_db/tb_cliente).
        :return: Dicionário com os metadados capturados do Job.
        """
        self.logger.info(f"📸 Capturando snapshot (código e propriedades) do Glue Job: {job_name}")
        
        try:
            # 1. Pega os atributos completos do Job na AWS
            response = self.client.get_job(JobName=job_name)
            job_details = response.get('Job', {})
            
            # Pasta de destino padronizada para os scripts
            target_script_folder = f"s3://{target_bucket}/{target_project_prefix}/script"
            
            # 2. Salva as propriedades como JSON no S3
            properties_json = json.dumps(job_details, default=str, indent=2, ensure_ascii=False)
            
            s3_manager.write_text_file(
                bucket=target_bucket,
                prefix=f"{target_project_prefix}/script",
                filename=f"properties_{job_name}",
                content=properties_json,
                extension="json"
            )
            self.logger.debug("Propriedades do Job salvas com sucesso em JSON.")
            
            # 3. Captura o caminho do Script Python original e faz a cópia
            script_location = job_details.get('Command', {}).get('ScriptLocation')
            
            if script_location:
                s3_manager.copy_file(
                    source_file_uri=script_location,
                    target_folder_uri=target_script_folder
                )
                self.logger.info(f"✅ Script fonte copiado com sucesso para: {target_script_folder}")
            else:
                self.logger.warning(f"⚠️ O job '{job_name}' não possui um 'ScriptLocation' definido no Command.")
                
            return job_details
            
        except Exception as e:
            self.logger.error(f"❌ Falha ao fazer backup dos artefatos do job '{job_name}': {e}")
            # Não damos um 'raise' aqui para não derrubar o pipeline caso seja apenas um erro de permissão.
            return {}