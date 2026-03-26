import boto3
import time
import copy
from typing import Optional, Dict, List, Any
from .AwsClient import AWSClient
from .GlueManager import GlueManager
from ..core.GenericLogger import GenericLogger
from ..core.Clock import Clock

class AthenaManager(AWSClient):
    """
    Classe responsável por orquestrar execuções no Amazon Athena.
    Gerencia consultas assíncronas, operações de UNLOAD e CTAS com medição de performance.
    """

    def __init__(self, region_name: str = "us-east-2",logger_name = 'YGGDRA'):
        super().__init__(service_name="athena", region_name=region_name)
        self.logger = GenericLogger(name=f'{logger_name}.Athena', propagate=True)
        self.glue = GlueManager(region_name=region_name)
        self.region_name = region_name
        self.logger.info(f"AthenaManager inicializado na região: {self.region_name}")

    # --- MÉTODOS PRIVADOS DE SUPORTE ---

    def _wait_for_query(self, query_id: str, timeout: int = 300) -> str:
        """Aguarda a conclusão da query com polling inteligente e cronômetro."""
        cronometro = Clock()
        cronometro.start()
        
        while cronometro.elapsed_seconds < timeout:
            response = self.client.get_query_execution(QueryExecutionId=query_id)
            status = response["QueryExecution"]["Status"]["State"]
            
            if status == "SUCCEEDED":
                self.logger.info(f"Query {query_id} finalizada em {cronometro.formatted}.")
                return status
            
            if status in ["FAILED", "CANCELLED"]:
                reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Sem motivo informado.")
                # Mantemos o log de erro/aviso, mas retornamos o status em vez de dar raise.
                # Isso permite que quem chamou a função trate a falha de forma estruturada.
                self.logger.error(f"Query {query_id} encerrou com {status} após {cronometro.formatted}: {reason}")
                return status
            
            time.sleep(5)
        
        # O Timeout continua dando raise, pois é uma falha de sistema (loop infinito) 
        # e não um status retornado pelo motor do Athena.
        raise TimeoutError(f"A query {query_id} excedeu o limite de {timeout}s.")

    # --- EXECUÇÃO DE QUERIES (DML / DDL) ---

    def execute_query(self, sql: str, database: str, output_s3: str, workgroup: str = "primary") -> Dict:
        """Executa uma query e retorna um dicionário com ID, status, motivo e tempo decorrido."""
        cronometro = Clock()
        cronometro.start()
        
        clean_output = output_s3 if output_s3.startswith("s3://") else f"s3://{output_s3}"
        
        try:
            resp = self.client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={"OutputLocation": clean_output},
                WorkGroup=workgroup
            )
            query_id = resp["QueryExecutionId"]
            
            # Aguarda a query sair dos estados de processamento (QUEUED / RUNNING)
            self._wait_for_query(query_id)
            cronometro.stop()

            # Busca o status oficial de finalização diretamente do Athena
            exec_info = self.client.get_query_execution(QueryExecutionId=query_id)
            status_info = exec_info['QueryExecution']['Status']
            
            final_status = status_info['State'] # Retorna SUCCEEDED, FAILED ou CANCELLED
            motivo = status_info.get('StateChangeReason', '') # Captura a mensagem de erro, se houver

            return {
                "status": final_status,
                "reason": motivo,
                "query_id": query_id,
                "elapsed_sec": cronometro.elapsed_seconds,
                "formatted_time": cronometro.formatted
            }

        except Exception as e:
            cronometro.stop()
            self.logger.error(f"Erro ao disparar/aguardar query no Athena após {cronometro.formatted}: {e}")
            raise

    def unload_to_s3(
        self, 
        sql: str, 
        target_s3_path: str, 
        database: str, 
        temp_s3: str, 
        partition_names: list | str, 
        sql_params: Dict[str, Any]
    ) -> Dict:
        """Executa UNLOAD direto na pasta da partição para eficiência incremental."""
        names = [partition_names] if isinstance(partition_names, str) else partition_names
        
        try:
            partition_path = "/".join([f"{n}={sql_params[n]}" for n in names])
        except KeyError as e:
            self.logger.error(f"Parâmetro de partição {e} não encontrado em sql_params.")
            raise

        final_target = f"{target_s3_path.rstrip('/')}/{partition_path}/"
        formatted_inner_sql = sql.format(**sql_params).strip().rstrip(";")

        self.logger.info(f"SQL formatado para UNLOAD:\n{formatted_inner_sql}")
        unload_query = f"""
            UNLOAD ({formatted_inner_sql}) 
            TO '{final_target}' 
            WITH (format = 'PARQUET', compression = 'GZIP')
        """.strip()

        self.logger.info(f"Disparando UNLOAD para: {final_target}")
        # Retorna o dicionário completo do execute_query
        return self.execute_query(unload_query, database, temp_s3)
    
    def create_table_as_select(
        self, 
        target_db: str, 
        target_table: str, 
        sql: str, 
        s3_path_target: str, 
        temp_s3: str, 
        partition_names: list | str, 
        sql_params: Dict[str, Any],
        overwrite: bool = True
    ) -> Dict:
        """Cria tabela via CTAS com limpeza automática e cronometragem."""
        cronometro = Clock()
        cronometro.start()

        names = [partition_names] if isinstance(partition_names, str) else partition_names
        cols_array = ", ".join([f"'{name}'" for name in names])

        try:
            formatted_sql = sql.format(**sql_params).strip().rstrip(";")
            self.logger.info(f"SQL formatado para CTAS:\n{formatted_sql}")
            
        except KeyError as e:
            self.logger.error(f"Parâmetro {e} ausente para o CTAS.")
            raise

        clean_target = f"{s3_path_target.rstrip('/')}/"
        if overwrite:
            self.execute_query(f"DROP TABLE IF EXISTS {target_db}.{target_table}", target_db, temp_s3)
            
            from .S3Manager import S3Manager
            s3_client = S3Manager(region_name=self.region_name)
            s3_client.delete_prefix(bucket=self._extract_bucket(clean_target), 
                                  prefix=self._extract_prefix(clean_target))

        ctas_query = f"""
            CREATE TABLE {target_db}.{target_table}
            WITH (
                format = 'PARQUET',
                external_location = '{clean_target}',
                partitioned_by = ARRAY[{cols_array}]
            )
            AS {formatted_sql}
        """

        try:
            resp = self.execute_query(ctas_query, target_db, temp_s3)
            cronometro.stop()
            
            self.logger.info(f"CTAS {target_table} concluído em {cronometro.formatted}.")
            
            return {
                "status": "Success",
                "table": f"{target_db}.{target_table}",
                "elapsed_sec": cronometro.elapsed_seconds,
                "formatted_time": cronometro.formatted,
                "query_id": resp["query_id"]
            }
        except Exception as e:
            self.logger.error(f"Falha no CTAS: {e}")
            raise

    # --- MÉTODOS DE UTILITÁRIOS ---

    def _extract_bucket(self, s3_uri: str) -> str:
        return s3_uri.replace("s3://", "").split("/")[0]

    def _extract_prefix(self, s3_uri: str) -> str:
        parts = s3_uri.replace("s3://", "").split("/", 1)
        return parts[1] if len(parts) > 1 else ""

    def manage_partition(self, db: str, table: str, partition_val: Any) -> Dict:
        """Gerencia partições no Glue com medição de tempo de resposta do SDK."""
        cronometro = Clock()
        cronometro.start()
        
        desc = self.glue.get_description_table(db, table)
        new_sd = copy.deepcopy(desc["StorageDescriptor"])
        base_location = new_sd["Location"].rstrip("/")
        new_sd["Location"] = f"{base_location}/anomes={partition_val}/"

        try:
            self.glue.client.create_partition(
                DatabaseName=db,
                TableName=table,
                PartitionInput={"Values": [str(partition_val)], "StorageDescriptor": new_sd}
            )
            status = "Created"
        except self.glue.client.exceptions.AlreadyExistsException:
            self.glue.client.update_partition(
                DatabaseName=db,
                TableName=table,
                PartitionValueList=[str(partition_val)],
                PartitionInput={"Values": [str(partition_val)], "StorageDescriptor": new_sd}
            )
            status = "Updated"

        cronometro.stop()
        return {
            "partition": partition_val, 
            "status": status, 
            "elapsed_sec": cronometro.elapsed_seconds
        }

    def get_table_ddl(self, database: str, table: str, temp_s3: str) -> Dict[str, Any]:
        """
        Extrai o DDL (CREATE TABLE statement) de uma tabela existente no Athena.
        Útil para persistência de metadados e linhagem no YGGDRA.
        """
        cronometro = Clock()
        cronometro.start()
        
        query_sql = f"SHOW CREATE TABLE {database}.{table}"
        self.logger.info(f"Extraindo DDL da tabela {database}.{table}")

        try:
            # 1. Executa a query usando o motor padronizado da classe
            # Isso já garante o wait_for_query e o registro do tempo
            exec_resp = self.execute_query(query_sql, database, temp_s3)
            query_id = exec_resp["query_id"]

            # 2. Coleta os resultados da execução
            results = self.client.get_query_results(QueryExecutionId=query_id)
            
            # 3. Processa as linhas do Athena (o DDL vem fragmentado em linhas)
            # Cada linha do 'SHOW CREATE TABLE' vem como uma VarCharValue única
            ddl_lines = [
                row['Data'][0].get('VarCharValue', '') 
                for row in results['ResultSet']['Rows']
            ]
            ddl_final = "\n".join(ddl_lines)

            cronometro.stop()
            self.logger.info(f"DDL extraído com sucesso em {cronometro.formatted}")

            return {
                "status": "Success",
                "database": database,
                "table": table,
                "ddl": ddl_final,
                "query_id": query_id,
                "elapsed_sec": cronometro.elapsed_seconds,
                "formatted_time": cronometro.formatted
            }

        except Exception as e:
            cronometro.stop()
            self.logger.error(f"Falha ao extrair DDL de {database}.{table}: {str(e)}")
            raise

    def obter_ddl_tabela_athena(database: str, tabela: str, workgroup: str = 'primary') -> str:
        """
        Executa SHOW CREATE TABLE no Athena e retorna o DDL como string.
        """
        client = boto3.client('athena')
        query = f"SHOW CREATE TABLE {database}.{tabela}"

        # 1. Inicia a execução da query
        try:
            start_response = client.start_query_execution(
                QueryString=query,
                WorkGroup=workgroup
                # Se não usar Workgroup configurado com bucket de output, 
                # adicione: ResultConfiguration={'OutputLocation': 's3://seu-bucket-de-logs/'}
            )
            query_execution_id = start_response['QueryExecutionId']
        except Exception as e:
            raise RuntimeError(f"Erro ao iniciar a query no Athena: {e}")

        # 2. Loop de espera (Polling) para a query finalizar
        while True:
            status_response = client.get_query_execution(QueryExecutionId=query_execution_id)
            status = status_response['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                reason = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Motivo desconhecido')
                raise RuntimeError(f"A query falhou com status {status}. Motivo: {reason}")
            
            # Aguarda 1 segundo antes de checar novamente para não estourar o limite da API
            time.sleep(1)

        # 3. Busca os resultados da query
        results_response = client.get_query_results(QueryExecutionId=query_execution_id)

        # 4. Processa o JSON para extrair o texto do DDL
        linhas_ddl = []
        
        # O Athena retorna os dados dentro de ['ResultSet']['Rows']
        for row in results_response['ResultSet']['Rows']:
            # Cada linha tem um array 'Data'. Pegamos a primeira coluna [0].
            # Usamos .get('VarCharValue') para evitar erros caso a linha venha vazia
            valor_coluna = row['Data'][0].get('VarCharValue', '')
            
            # O Athena costuma retornar o nome da coluna ('createtab_stmt') na primeira linha. 
            # Ignoramos essa linha de cabeçalho.
            if valor_coluna and valor_coluna != 'createtab_stmt':
                linhas_ddl.append(valor_coluna)

        # 5. Junta todas as linhas com quebra de linha para formar a string final
        ddl_completo = '\n'.join(linhas_ddl)
        
        return ddl_completo
    
    def fetch_scalar(self, sql: str, database: str, temp_s3: str) -> str:
        """
        Executa uma query no Athena e retorna o primeiro valor da primeira coluna (escalar).
        Ideal para queries de agregação rápida como COUNT(*), MAX() ou MIN().
        
        :param sql: A query a ser executada.
        :param database: O banco de dados de contexto.
        :param temp_s3: Caminho temporário no S3 para o Athena gravar os metadados.
        :return: O valor retornado como string (ex: '1540'). Retorna '0' se vier vazio.
        """
        self.logger.info("⚡ [AthenaManager] Executando query escalar (Scalar Fetch)...")
        
        try:
            # 1. Aciona o seu método padrão que executa a query e aguarda o status SUCCEEDED
            resp = self.execute_query(sql=sql, database=database, output_s3=temp_s3)
            query_id = resp.get('query_id')
            
            if not query_id:
                raise RuntimeError("Falha na execução: Nenhum Query ID retornado pelo Athena.")
                
            # 2. Pede à API da AWS os resultados reais daquela execução
            response = self.client.get_query_results(
                QueryExecutionId=query_id,
                MaxResults=5 # Limitamos a 5 porque só queremos a primeira linha mesmo, economiza rede!
            )
            
            # 3. Navega no labirinto do JSON do Boto3
            rows = response.get('ResultSet', {}).get('Rows', [])
            
            # Se tiver mais de 1 linha (Cabeçalho + Dados)
            if len(rows) > 1:
                # Extrai o bloco de dados da Linha 1 (índice 1)
                data_columns = rows[1].get('Data', [])
                if data_columns:
                    # Pega o 'VarCharValue' da Coluna 0 (índice 0)
                    scalar_value = data_columns[0].get('VarCharValue', '0')
                    return scalar_value
            
            # Retorno seguro caso a query não devolva linhas
            self.logger.warning(f"⚠️ Query escalar retornou VAZIO. Query ID: {query_id}")
            return "0"

        except Exception as e:
            self.logger.error(f"❌ Erro ao tentar extrair resultado escalar da query no Athena: {e}")
            raise
    
    
    def count_linhas_particao(self, database: str, tabela: str, particao: Dict[str, str], output_s3: str, workgroup: str = "primary") -> Dict[str, Any]:
        """
        Executa um COUNT(*) em uma partição específica e retorna o valor junto com as métricas.
        
        Args:
            database: Nome do banco de dados no Athena.
            tabela: Nome da tabela.
            particao: Dicionário com as chaves e valores da partição. Ex: {'anomesdia': '20240309'}
            output_s3: Caminho do S3 para salvar os resultados temporários do Athena.
            workgroup: Workgroup do Athena.
        """
        cronometro = Clock()
        cronometro.start()
        
        # 1. Monta a cláusula WHERE dinamicamente com base no dicionário
        clausulas_where = [f"{coluna} = '{valor}'" for coluna, valor in particao.items()]
        where_sql = " AND ".join(clausulas_where)
        
        sql = f"SELECT COUNT(*) AS total_linhas FROM {database}.{tabela} WHERE {where_sql}"
        
        clean_output = output_s3 if output_s3.startswith("s3://") else f"s3://{output_s3}"
        
        try:
            # 2. Inicia a query
            resp = self.client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={"OutputLocation": clean_output},
                WorkGroup=workgroup
            )
            query_id = resp["QueryExecutionId"]
            
            # 3. Aguarda a execução usando o método ajustado
            status = self._wait_for_query(query_id)
            cronometro.stop()
            
            # 4. Busca as estatísticas de execução (mesmo se falhar, o Athena devolve o tempo gasto na fila)
            exec_info = self.client.get_query_execution(QueryExecutionId=query_id)
            stats = exec_info['QueryExecution'].get('Statistics', {})
            
            dados_lidos_bytes = stats.get('DataScannedInBytes', 0)
            tempo_motor_ms = stats.get('EngineExecutionTimeInMillis', 0)
            
            # Monta a estrutura base do retorno
            resultado = {
                "status": status,
                "query_id": query_id,
                "elapsed_sec": cronometro.elapsed_seconds,
                "formatted_time": cronometro.formatted,
                "metrics": {
                    "data_scanned_bytes": dados_lidos_bytes,
                    "data_scanned_mb": round(dados_lidos_bytes / (1024 * 1024), 4),
                    "engine_execution_time_ms": tempo_motor_ms
                },
                "count": None,
                "reason": exec_info['QueryExecution']['Status'].get('StateChangeReason', '')
            }
            
            # 5. Se a query teve sucesso, busca o resultado numérico do COUNT
            if status == "SUCCEEDED":
                results = self.client.get_query_results(QueryExecutionId=query_id)
                
                # Navegação no JSON de resposta do Athena:
                # results['ResultSet']['Rows'][0] -> Cabeçalho (ex: 'total_linhas')
                # results['ResultSet']['Rows'][1] -> Primeira linha de dados com o valor
                valor_str = results['ResultSet']['Rows'][1]['Data'][0].get('VarCharValue', '0')
                resultado["count"] = int(valor_str)
                
                if hasattr(self, 'logger'):
                    self.logger.info(f"Partição {particao} da tabela {tabela} possui {resultado['count']} linhas. Lidos: {resultado['metrics']['data_scanned_mb']} MB.")
                
            return resultado

        except Exception as e:
            cronometro.stop()
            if hasattr(self, 'logger'):
                self.logger.error(f"Erro ao contar linhas na particao após {cronometro.formatted}: {e}")
            raise

    def get_formatted_ddl(self, db: str, table: str, temp_s3: str) -> str:
        """
        Executa um SHOW CREATE TABLE no Athena, captura as linhas do ResultSet,
        e formata o output para ser salvo nativamente como um script .sql.
        
        :param db: Nome do banco de dados no Glue Catalog.
        :param table: Nome da tabela.
        :param temp_s3: Caminho temporário no S3 para o Athena salvar o resultado da query.
        :return: String do DDL formatado e pronto para uso.
        """
        self.logger.info(f"🔍 Extraindo DDL reverso da tabela {db}.{table}...")
        
        query_sql = f"SHOW CREATE TABLE `{db}`.`{table}`;"
        
        try:
            # 1. Executa a query usando o nosso método padrão
            resp = self.execute_query(
                sql=query_sql, 
                database=db, 
                output_s3=temp_s3
            )
            
            # 2. Busca o resultado cru da API do Athena
            result_payload = self.client.get_query_results(
                QueryExecutionId=resp['query_id']
            )
            
            # 3. Lógica de concatenação: O Athena retorna cada quebra de linha do DDL como uma Row
            ddl_lines = []
            rows = result_payload.get('ResultSet', {}).get('Rows', [])
            
            for row in rows:
                # Extrai o valor texto da primeira coluna de cada linha
                col_data = row.get('Data', [{}])[0].get('VarCharValue', '')
                if col_data:
                    # Remove espaços em branco excessivos nas pontas, mantendo a indentação interna
                    ddl_lines.append(col_data.rstrip())

            # 4. Formatação Final
            # Junta tudo com quebra de linha
            formatted_ddl = "\n".join(ddl_lines).strip()
            
            # Garante que termine com ponto e vírgula (padrão SQL)
            if not formatted_ddl.endswith(';'):
                formatted_ddl += ';'

            self.logger.debug("DDL formatado com sucesso.")
            return formatted_ddl

        except Exception as e:
            self.logger.error(f"❌ Falha ao extrair DDL de {db}.{table}: {e}")
            raise RuntimeError(f"Erro ao extrair DDL: {e}")
    
    def validate_query(self, sql: str, database: str, temp_s3: str) -> Dict[str, Any]:
        """
        Executa um EXPLAIN na query para validar sintaxe, semântica e existência
        das tabelas/colunas subjacentes sem processar os dados reais.
        
        Padrão 'Fail Fast' para evitar custos no Athena.
        
        :param sql: A query SQL original que será testada.
        :param database: O banco de dados de contexto.
        :param temp_s3: Caminho S3 para o output temporário do Athena.
        :return: Dicionário contendo o status de validação e a mensagem de erro (se houver).
        """
        self.logger.info("🧪 Validando a estrutura da query no Athena (EXPLAIN)...")
        
        # 1. Limpeza preventiva: Removemos espaços e o ';' final para não quebrar o EXPLAIN
        clean_sql = sql.strip().rstrip(';')
        
        # 2. Monta a query de validação
        explain_sql = f"EXPLAIN {clean_sql};"
        
        try:
            # 3. Executa a query usando o nosso motor padrão
            resp = self.execute_query(
                sql=explain_sql, 
                database=database, 
                output_s3=temp_s3
            )
            
            self.logger.info("✅ Query validada com sucesso! Estrutura e origens estão corretas.")
            return {
                "is_valid": True,
                "error": None,
                "query_id": resp.get('query_id')
            }

        except Exception as e:
            # O execute_query já lança uma exceção se a AWS retornar FAILED.
            error_msg = str(e)
            self.logger.error(f"❌ Falha na validação da Query. O Athena recusou a execução.")
            
            return {
                "is_valid": False,
                "error": error_msg,
                "query_id": None
            }