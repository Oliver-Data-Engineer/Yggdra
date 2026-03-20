import boto3
from typing import Optional, Dict, List, Any
from .AwsClient import AWSClient
from botocore.exceptions import ClientError
from ..core.GenericLogger import GenericLogger
from ..core.Clock import Clock
from ..core.Utils import Utils 

class S3Manager(AWSClient):
    """
    Classe responsável por abstrair operações no Amazon S3.
    Centraliza lógica de bootstrapping de projetos e manipulação de objetos/partições.
    """

    def __init__(self, region_name: str = "us-east-2", logger_name: str = "YGGDRA"):
        super().__init__(service_name="s3", region_name=region_name)
        self.logger = GenericLogger(name=f'{logger_name}.S3', propagate=True)
        self.region = region_name
        self.logger.info(f"S3Manager inicializado na região: {self.region}")

    # --- MÉTODOS DE APOIO / INTERNOS ---

    def _sanitize_bucket_name(self, bucket_name: str) -> str:
        """
        Limpador Universal para nomes de Bucket:
        Remove barras, espaços e converte para minúsculo.
        Essencial para evitar botocore.exceptions.ParamValidationError.
        """
        if not bucket_name:
            return ""
        # S3 Buckets NÃO podem ter "/" e devem ser minúsculos para compatibilidade DNS
        return bucket_name.strip().strip("/").lower()

    def _normalize_path(self, prefix: str) -> str:
        """
        Normalizador Universal para Prefixos/Pastas:
        Garante que o prefixo termine com '/' e não comece com '/'.
        """
        if not prefix:
            return ""
        # Remove espaços e barra inicial (S3 paths não devem começar com /)
        path = prefix.strip().lstrip("/")
        # Garante a barra final para representar um diretório/prefixo
        if not path.endswith("/"):
            path += "/"
        # Remove possíveis barras duplas acidentais
        while "//" in path:
            path = path.replace("//", "/")
        return path

    def get_bucket_default(self) -> str:
        """Retorna o nome do bucket seguindo o padrão da organização."""
        account_id = self.account_id
        bucket = f"itau-self-wkp-{self.region}-{account_id}"
        # Como o padrão é gerado via código, aplicamos a sanitização por segurança
        return self._sanitize_bucket_name(bucket)

    # --- CRIAÇÃO DE INFRAESTRUTURA ---

    def create_bucket(self, bucket_name: str) -> bool:
        """Cria um bucket S3, se não existir, com nome sanitizado."""
        clean_name = self._sanitize_bucket_name(bucket_name)
        
        if self.bucket_exists(clean_name):
            self.logger.info(f"Bucket já existe: {clean_name}")
            return False
        
        try:
            self.client.create_bucket(
                Bucket=clean_name,
                CreateBucketConfiguration={"LocationConstraint": self.region}
            )
            self.logger.info(f"Bucket criado com sucesso: {clean_name}")
            return True
        except ClientError as e:
            self.logger.error(f"Erro ao criar bucket {clean_name}: {e}")
            raise

    # --- VERIFICAÇÕES ---

    def bucket_exists(self, bucket_name: str) -> Optional[bool]:
        """Verifica existência e permissão de um bucket com nome sanitizado."""
        clean_name = self._sanitize_bucket_name(bucket_name)
        
        try:
            self.client.head_bucket(Bucket=clean_name)
            return True
        except ClientError as e:
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 404:
                self.logger.debug(f"Bucket não existe: {clean_name}")
                return False
            if status == 403:
                self.logger.warning(f"Sem permissão (403) para o bucket: {clean_name}")
                return None
            raise
    
    def prefix_exists(self, bucket: str, prefix: str) -> bool:
        """Verifica se um prefixo (pasta ou objeto) existe."""
        clean_bucket = self._sanitize_bucket_name(bucket)
        clean_prefix = self._normalize_path(prefix)
        try:
            resp = self.client.list_objects_v2(Bucket=clean_bucket, Prefix=clean_prefix, MaxKeys=1)
            return "Contents" in resp
        except ClientError as e:
            self.logger.error(f"Erro ao listar prefixo s3://{clean_bucket}/{clean_prefix}: {e}")
            return False

    # --- CRIAÇÃO DE ESTRUTURA LÓGICA ---

    def create_s3_folder(self, prefix: str, bucket: str = None) -> str:


        """Cria um marcador de pasta (objeto vazio terminado em '/') no S3."""
        target_bucket = self._sanitize_bucket_name(bucket or self.get_bucket_default())
        target_prefix = self._normalize_path(prefix)

        if not self.prefix_exists(target_bucket, target_prefix):
            self.client.put_object(Bucket=target_bucket, Key=target_prefix)
            self.logger.info(f"Pasta criada: s3://{target_bucket}/{target_prefix}")

        
        return f"s3://{target_bucket}/{target_prefix}"

    def setup_project(self, project_name: str, bucket_name: Optional[str] = None) -> Dict[str, str]:
        """
        Orquestrador de ambiente: 
        1. Resolve e sanitiza o bucket.
        2. Garante que o bucket raiz exista.
        3. Invoca o bootstrap para criar pastas internas.
        """
        # CORREÇÃO: Usamos sanitize para buckets, nunca normalize.
        target_bucket = self._sanitize_bucket_name(bucket_name or self.get_bucket_default())
        
        self.logger.info(f"Configurando ambiente no bucket: {target_bucket}")

        status = self.bucket_exists(target_bucket)
        
        if status is False:
            self.logger.info(f"Bucket {target_bucket} não encontrado. Tentando criar...")
            self.create_bucket(target_bucket)
        elif status is None:
            raise PermissionError(f"Acesso negado ao bucket: {target_bucket}")

        return self._initialize_bootstrap(project_name, target_bucket)

    def _initialize_bootstrap(self, project_name: str, bucket: str) -> Dict[str, str]:
        """Cria subpastas padrão para um projeto."""
        clean_project = self._normalize_path(project_name)
        subdirs = ["scripts", "data", "sql", "temp", "logs","reports","metadata",'audits','config','reports/error','reports/success']
        
        structure = {"root": self.create_s3_folder(clean_project, bucket=bucket)}

        for sub in subdirs:
            # Concatena o nome do projeto com a subpasta
            full_path = f"{clean_project}{sub}"
            structure[sub] = self.create_s3_folder(full_path, bucket=bucket)

        self.logger.info(f"Bootstrap finalizado para '{clean_project}' em s3://{bucket}/")
        return structure

    # --- I/O DE ARQUIVOS ---

    def get_content_sql(self, bucket: str, prefix: str, filename: str) -> str:
        """
        Lê o conteúdo de um arquivo SQL no S3 de forma robusta.
        Suporta normalização de caminhos e tratamento de erros de encoding.
        """
        clean_bucket = self._sanitize_bucket_name(bucket)
        
        # Garante a extensão .sql
        if not filename.lower().endswith(".sql"):
            filename += ".sql"
        
        key = f"{self._normalize_path(prefix)}{filename}"
        s3_uri = f"s3://{clean_bucket}/{key}"

        self.logger.info(f"Buscando script SQL em: {s3_uri}")

        try:
            response = self.client.get_object(Bucket=clean_bucket, Key=key)
            
            # Captura metadados para o log (ajuda a debugar se o arquivo está vazio)
            content_length = response.get('ContentLength', 0)
            last_modified = response.get('LastModified', 'Desconhecido')
            
            # Leitura robusta: tenta UTF-8, mas permite fallback se necessário
            body_content = response["Body"].read()
            
            try:
                content = body_content.decode("utf-8")
            except UnicodeDecodeError:
                self.logger.warning(f"Falha ao decodificar {filename} em UTF-8. Tentando latin-1.")
                content = body_content.decode("latin-1")

            self.logger.info(
                f"SQL carregado com sucesso. Tamanho: {content_length} bytes. "
                f"Última modificação: {last_modified}"
            )
            self.logger.info(f"Conteúdo do SQL:\n{content}...")  # Loga os primeiros 500 caracteres para verificação
            return content

        except self.client.exceptions.NoSuchKey:
            error_msg = f"Script SQL não encontrado no S3: {s3_uri}"
            self.logger.error(error_msg)
            raise FileNotFoundError(error_msg)
            
        except ClientError as e:
            # Captura erros de permissão (403) ou outros erros de API
            error_code = e.response.get("Error", {}).get("Code")
            self.logger.error(f"Erro AWS ({error_code}) ao acessar {s3_uri}: {e}", exc_info=True)
            raise

        except Exception as e:
            self.logger.critical(f"Erro inesperado ao ler SQL em {s3_uri}: {e}", exc_info=True)
            raise

    def write_text_file(self, bucket_name: Optional[str], prefix: str, filename: str, content: str, extension: str) -> str:
        """Escreve arquivo de texto no S3 com caminhos normalizados e logging de execução."""
        
        bucket = bucket_name or self.get_bucket_default()
       
        clean_bucket = self._sanitize_bucket_name(bucket)
        
        # Normalização de extensão e nome do arquivo
        ext = extension if extension.startswith(".") else f".{extension}"
        if not filename.endswith(ext):
            filename += ext
        
        # Composição da Key final
        normalized_prefix = self._normalize_path(prefix)
        key = f"{normalized_prefix}{filename}"
        s3_uri = f"s3://{clean_bucket}/{key}"

        self.logger.info(f"Iniciando escrita de arquivo: {s3_uri}")
        
        try:
            # Execução do upload via boto3
            self.client.put_object(
                Bucket=clean_bucket,
                Key=key,
                Body=content.encode("utf-8"),
                ContentType="text/plain"
            )
            
            self.logger.info(f"Arquivo gravado com sucesso. Tamanho: {len(content)} caracteres.")
            return s3_uri

        except Exception as e:
            # O logger.error com exc_info=True anexa o traceback completo no log,
            # o que é vital para debug em produção (ex: erro de permissão ou bucket inexistente).
            self.logger.error(f"Falha ao gravar arquivo em {s3_uri}: {str(e)}", exc_info=True)
            raise

    # --- LIMPEZA ---

    def clean_partition(
        self, 
        s3_uri: str, 
        partition_names: list | str, 
        partition_values: list | Any
    ) -> int:
        """
        Realiza a limpeza cirúrgica de uma ou múltiplas partições no S3.
        Suporta o padrão: s3://bucket/prefix/key1=val1/key2=val2/
        """
        if not s3_uri.startswith("s3://"):
            self.logger.error(f"URI Inválida fornecida para limpeza: {s3_uri}")
            raise ValueError(f"URI Inválida: {s3_uri}")

        # 1. Parsing da URI Base
        path_parts = s3_uri.replace("s3://", "").split("/", 1)
        bucket = self._sanitize_bucket_name(path_parts[0])
        base_prefix = self._normalize_path(path_parts[1]) if len(path_parts) > 1 else ""

        # 2. Normalização de nomes e valores para listas
        names = [partition_names] if isinstance(partition_names, str) else partition_names
        values = [partition_values] if not isinstance(partition_values, list) else partition_values

        if len(names) != len(values):
            self.logger.error("Divergência entre número de nomes e valores de partição.")
            raise ValueError("A quantidade de nomes de partições deve ser igual à de valores.")

        # 3. Construção do Prefixo Especializado (Hierarquia Hive)
        # Ex: ['ano=2026', 'mes=02', 'dia=26']
        partition_path_parts = [f"{n}={v}" for n, v in zip(names, values)]
        
        # Junta tudo garantindo a estrutura de pastas e a barra final
        target_prefix = base_prefix + "/".join(partition_path_parts) + "/"
        
        # 4. Limpeza de barras duplas acidentais (Double Slash Prevention)
        while "//" in target_prefix:
            target_prefix = target_prefix.replace("//", "/")

        self.logger.info(f"Iniciando limpeza especializada em: s3://{bucket}/{target_prefix}")

        # 5. Execução da Deleção
        # Utiliza o método de deleção por prefixo que já temos na classe
        deleted_count = self._delete_objects_by_prefix(bucket, target_prefix)
        
        if deleted_count > 0:
            self.logger.info(f"Limpeza concluída. {deleted_count} objetos removidos de s3://{bucket}/{target_prefix}")
        else:
            self.logger.info(f"Nenhum objeto encontrado para remoção no prefixo informado.")

        return deleted_count

    def _delete_objects_by_prefix(self, bucket: str, prefix: str) -> int:
        """Método privado para deleção em massa."""
        objects = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in objects:
            return 0
        
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
        self.client.delete_objects(Bucket=bucket, Delete=delete_keys)
        return len(objects['Contents'])
    
    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Exclui um bucket do S3.
        
        Args:
            bucket_name (str): Nome do bucket a ser excluído.
            force (bool): Se True, esvazia o bucket antes de tentar excluir.
                          CUIDADO: Isso apagará todos os dados permanentemente.
        """
        clean_name = self._sanitize_bucket_name(bucket_name)
        
        try:
            if not self.bucket_exists(clean_name):
                self.logger.warning(f"Tentativa de excluir bucket inexistente: {clean_name}")
                return False

            if force:
                self.logger.info(f"Esvaziando bucket '{clean_name}' antes da exclusão...")
                self._empty_bucket(clean_name)

            self.client.delete_bucket(Bucket=clean_name)
            self.logger.info(f"Bucket '{clean_name}' excluído com sucesso.")
            return True

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "BucketNotEmpty":
                self.logger.error(f"Não foi possível excluir '{clean_name}': O bucket não está vazio. Use force=True.")
            else:
                self.logger.error(f"Erro ao excluir bucket '{clean_name}': {e}")
            raise

    def _empty_bucket(self, bucket_name: str):
        """Método auxiliar para remover todos os objetos e versões (objetos excluídos)."""
        # Remove todas as versões de objetos (necessário se o bucket tiver versionamento)
        paginator = self.client.get_paginator('list_object_versions')
        for page in paginator.paginate(Bucket=bucket_name):
            delete_list = []
            
            # Versões de objetos
            for version in page.get('Versions', []):
                delete_list.append({'Key': version['Key'], 'VersionId': version['VersionId']})
            
            # Marcadores de exclusão (Delete Markers)
            for marker in page.get('DeleteMarkers', []):
                delete_list.append({'Key': marker['Key'], 'VersionId': marker['VersionId']})
            
            if delete_list:
                self.client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_list})

        # Garante que objetos simples também sejam removidos (caso o versionamento esteja off)
        self._delete_objects_by_prefix(bucket_name, "")

    def move_file(self, source_file_uri: str, target_folder_uri: str) -> str:
        """
        Move um arquivo de um path/URI completo para uma pasta/URI de destino.
        
        Args:
            source_file_uri (str): URI completa do arquivo ou path relativo.
            target_folder_uri (str): URI completa da pasta ou path relativo.
        """
        # 1. Resolver Origem (Bucket e Key)
        if source_file_uri.startswith("s3://"):
            s_parts = source_file_uri.replace("s3://", "").split("/", 1)
            s_bucket = self._sanitize_bucket_name(s_parts[0])
            s_key = s_parts[1]
        else:
            s_bucket = self.get_bucket_default()
            s_key = source_file_uri.lstrip("/")

        # 2. Resolver Destino (Bucket e Prefixo)
        if target_folder_uri.startswith("s3://"):
            t_parts = target_folder_uri.replace("s3://", "").split("/", 1)
            t_bucket = self._sanitize_bucket_name(t_parts[0])
            t_prefix = self._normalize_path(t_parts[1]) if len(t_parts) > 1 else ""
        else:
            t_bucket = s_bucket # Se não passar URI no destino, assume o mesmo bucket da origem
            t_prefix = self._normalize_path(target_folder_uri)

        # 3. Extrair nome do arquivo usando o utilitário
        filename = self.get_filename_from_uri(s_key)
        if not filename:
            raise ValueError(f"Não foi possível extrair o nome do arquivo da origem: {source_file_uri}")

        # 4. Compor a Key final de destino
        t_key = f"{t_prefix}{filename}"

        source_path = f"s3://{s_bucket}/{s_key}"
        target_path = f"s3://{t_bucket}/{t_key}"

        self.logger.info(f"Iniciando movimento: {source_path} -> {target_path}")

        try:
            # 5. Cópia entre Buckets/Pastas
            self.client.copy_object(
                CopySource={'Bucket': s_bucket, 'Key': s_key},
                Bucket=t_bucket,
                Key=t_key
            )
            
            # 6. Deleção na Origem
            self.client.delete_object(Bucket=s_bucket, Key=s_key)
            
            self.logger.info(f"Movimento finalizado com sucesso.")
            return target_path

        except Exception as e:
            self.logger.error(f"Erro ao mover de {source_path} para {target_path}: {e}", exc_info=True)
            raise
    
    @staticmethod
    def get_filename_from_uri(uri: str) -> str:
        """
        Extrai apenas o nome do arquivo de uma URI S3 ou path completo.
        Ex: 's3://bucket/folder/file.sql' -> 'file.sql'
        """
        if not uri or not isinstance(uri, str):
            return ""
            
        # Remove espaços e possíveis barras residuais à direita
        clean_uri = uri.strip().rstrip("/")
        
        # O nome do arquivo é sempre a última parte após a barra
        filename = clean_uri.split("/")[-1]
        
        return filename
    
    def file_exists(self, file_uri: str) -> bool:
        """
        Verifica se um arquivo específico existe no S3 via URI ou path relativo.
        
        Args:
            file_uri (str): URI completa (ex: s3://bucket/path/file.txt)
                            ou path relativo ao bucket padrão.
        """
        # 1. Resolver Bucket e Key
        if file_uri.startswith("s3://"):
            parts = file_uri.replace("s3://", "").split("/", 1)
            bucket = self._sanitize_bucket_name(parts[0])
            key = parts[1] if len(parts) > 1 else ""
        else:
            bucket = self.get_bucket_default()
            key = file_uri.lstrip("/")

        # Se a key estiver vazia (ex: passou apenas s3://bucket/), não é um arquivo
        if not key:
            self.logger.warning(f"URI de arquivo inválida (sem key): {file_uri}")
            return False

        self.logger.debug(f"Verificando existência do arquivo: s3://{bucket}/{key}")

        try:
            # head_object é a forma mais barata e rápida de verificar um arquivo
            # (retorna apenas metadados, sem baixar o corpo)
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        
        except ClientError as e:
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 404:
                self.logger.debug(f"Arquivo não encontrado: s3://{bucket}/{key}")
                return False
            
            # Caso seja erro de permissão (403) ou outro, logamos o erro
            self.logger.error(f"Erro ao verificar arquivo {file_uri}: {e}")
            return False
    
    def copy_file(self, source_file_uri: str, target_folder_uri: str,target_file_name: Optional[str]) -> str:
        """
        Copia um arquivo de um path/URI completo para uma pasta/URI de destino.
        
        Args:
            source_file_uri (str): URI completa do arquivo ou path relativo.
            target_folder_uri (str): URI completa da pasta de destino ou path relativo.
        """
        # 1. Resolver Origem (Bucket e Key)
        if source_file_uri.startswith("s3://"):
            s_parts = source_file_uri.replace("s3://", "").split("/", 1)
            s_bucket = self._sanitize_bucket_name(s_parts[0])
            s_key = s_parts[1]
        else:
            s_bucket = self.get_bucket_default()
            s_key = source_file_uri.lstrip("/")

        # 2. Resolver Destino (Bucket e Prefixo)
        if target_folder_uri.startswith("s3://"):
            t_parts = target_folder_uri.replace("s3://", "").split("/", 1)
            t_bucket = self._sanitize_bucket_name(t_parts[0])
            t_prefix = self._normalize_path(t_parts[1]) if len(t_parts) > 1 else ""
        else:
            t_bucket = s_bucket
            t_prefix = self._normalize_path(target_folder_uri)

        # 3. Extrair nome do arquivo
        filename = self.get_filename_from_uri(s_key)
        if not filename:
            error_msg = f"Falha ao extrair nome do arquivo de origem: {source_file_uri}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 4. Compor a Key final de destino

        filename_target = target_file_name or filename
        t_key = f"{t_prefix}{filename_target}"

        source_path = f"s3://{s_bucket}/{s_key}"
        target_path = f"s3://{t_bucket}/{t_key}"

        self.logger.info(f"Iniciando cópia: {source_path} -> {target_path}")

        try:
            # 5. Operação de Cópia via Boto3
            # Nota: O CopySource deve ser um dicionário ou string 'bucket/key'
            self.client.copy_object(
                CopySource={'Bucket': s_bucket, 'Key': s_key},
                Bucket=t_bucket,
                Key=t_key
            )
            
            self.logger.info(f"Cópia finalizada com sucesso: {target_path}")
            return target_path

        except Exception as e:
            self.logger.error(f"Erro ao copiar de {source_path} para {target_path}: {e}", exc_info=True)
            raise
    
    def delete_prefix(self, bucket: str, prefix: str) -> int:
        """
        Deleta recursivamente todos os objetos sob um prefixo específico.
        
        Args:
            bucket (str): Nome do bucket.
            prefix (str): O prefixo (pasta) a ser limpo.
            
        Returns:
            int: Total de objetos deletados.
        """
        clean_bucket = self._sanitize_bucket_name(bucket)
        clean_prefix = self._normalize_path(prefix)

        # Proteção Crítica: Evita deletar o bucket inteiro se o prefixo vier vazio
        if not clean_prefix or clean_prefix == "/":
            self.logger.error(f"Tentativa de deleção em prefixo raiz abortada por segurança no bucket: {clean_bucket}")
            raise ValueError("O prefixo de deleção não pode ser vazio ou a raiz.")

        self.logger.info(f"Iniciando limpeza do prefixo: s3://{clean_bucket}/{clean_prefix}")

        total_deleted = 0
        try:
            # O paginator é essencial: o S3 limita a listagem a 1000 objetos por chamada.
            paginator = self.client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=clean_bucket, Prefix=clean_prefix):
                if 'Contents' in page:
                    # Prepara o lote de objetos (Máximo 1000 por requisição de deleção)
                    delete_list = [{'Key': obj['Key']} for obj in page['Contents']]
                    
                    self.client.delete_objects(
                        Bucket=clean_bucket,
                        Delete={'Objects': delete_list, 'Quiet': True}
                    )
                    
                    batch_count = len(delete_list)
                    total_deleted += batch_count
                    self.logger.debug(f"Lote de {batch_count} objetos removido.")

            if total_deleted > 0:
                self.logger.info(f"Sucesso: {total_deleted} objetos removidos de {clean_prefix}.")
            else:
                self.logger.info(f"O prefixo {clean_prefix} já estava limpo.")

            return total_deleted

        except Exception as e:
            self.logger.error(f"Erro crítico ao deletar prefixo {clean_prefix}: {e}", exc_info=True)
            raise