import re
from typing import Dict, Any, List
from ..aws.S3Manager import S3Manager
from ..aws.GlueManager import GlueManager
from ..core.Utils import Utils

class SourceGuardian:
    """
    Produto da Yggdra responsável por inspecionar o código SQL, 
    identificar tabelas upstream (origens) e capturar o estado exato das 
    partições no momento da execução, com inferência inteligente de formatos.
    """

    def __init__(self, region_name: str, logger_name: str = 'YGGDRA.Guardian'):
        self.glue = GlueManager(logger_name=logger_name, region_name=region_name)
        self.s3 = S3Manager(logger_name=logger_name, region_name=region_name) 

    def get_query_from_s3(self, s3_uri: str, bucket_name: str, project_prefix: str) -> str:
        """Busca a query no S3 caso ela não seja passada diretamente."""
        filename = self.s3.get_filename_from_uri(s3_uri)
        return self.s3.get_content_sql(bucket=bucket_name, prefix=f"{project_prefix}/sql", filename=filename)

    def _infer_partition_format(self, sample_partitions: List[str], p_types: List[str]) -> str:
        """
        Analisa uma amostra das últimas partições para inferir formatos de data (comuns ou especiais).
        """
        # Se a amostra estiver vazia ou for uma partição múltipla (ex: year=, month=), 
        # ignoramos a inferência de string única.
        if not sample_partitions or len(p_types) > 1:
            return None

        p_type = p_types[0].lower()
        
        if p_type not in ['string', 'date', 'timestamp']:
            return None

        # Função auxiliar para não repetirmos o loop for toda hora
        def all_match(pattern: str) -> bool:
            return all(isinstance(p, str) and re.match(pattern, p) for p in sample_partitions)

        # =====================================================================
        # 1. CASOS ESPECIAIS (Prioridade Alta: Mensal disfarçado de diário)
        # =====================================================================
        if all_match(r'^\d{4}-\d{2}-01( 00:00:00)?$'):
            return "%Y-%m-01"

        if all_match(r'^\d{4}\d{2}01$'):
            return "%Y%m01"

        # =====================================================================
        # 2. CASOS PADRÕES (Infeção Comum de Datas)
        # =====================================================================
        # Data com traço (Ex: 2024-03-10)
        if all_match(r'^\d{4}-\d{2}-\d{2}( 00:00:00)?$'):
            return "%Y-%m-%d"

        # Anomesdia contínuo (Ex: 20240310)
        if all_match(r'^\d{8}$'):
            return "%Y%m%d"

        # Anomes contínuo (Ex: 202403)
        if all_match(r'^\d{6}$'):
            return "%Y%m"

        # Apenas Ano (Ex: 2024)
        if all_match(r'^\d{4}$'):
            return "%Y"

        return None

    def map_upstream_lineage(self, query: str) -> List[Dict[str, Any]]:
        """
        Inspeciona a query e retorna a linhagem estruturada com metadados do Glue,
        incluindo a inferência inteligente de formato de partição.
        """
        origens = Utils.get_origens_sql(query, dialect='presto')
        lineage_details = []

        for origem in origens:
            db = origem.get('db')
            table = origem.get('table')
            path_complete = origem.get('path')

            try:
                tb_desc = self.glue.get_description_table(db=db, table=table)
                
                partition_keys = [p.get("Name") for p in tb_desc.get("PartitionKeys", [])]
                partition_types = [p.get("Type") for p in tb_desc.get("PartitionKeys", [])]
                
                last_partition_value = None
                inferred_format = None
                
                if partition_keys:
                    # ⚠️ ATENÇÃO ARQUITETURAL: 
                    # Ajuste o seu GlueManager para ter um método que traga as N últimas partições.
                    # Exemplo: get_last_n_partitions(..., limit=3)
                    last_3_partitions = self.glue.get_last_n_partitions(
                        db=db, 
                        table=table,
                        partition_keys=partition_keys,
                        limit=3
                    )
                    
                    if last_3_partitions:
                        # Pega a mais recente para manter a compatibilidade com a chave antiga
                        last_partition_value = last_3_partitions[0]
                        
                        # Aciona a inteligência para inferir o formato da amostra
                        inferred_format = self._infer_partition_format(
                            sample_partitions=last_3_partitions, 
                            p_types=partition_types
                        )

                # Estrutura limpa, validada e com o novo metadado
                source_info = {
                    "path_reference": path_complete,
                    "database": db,
                    "table": table,
                    "partition_keys": partition_keys,
                    "partition_types": partition_types,
                    "last_partition_value": last_partition_value,
                    "inferred_format": inferred_format  # 💡 O NOVO ATRIBUTO
                }
                
                lineage_details.append(source_info)
                
            except Exception as e:
                print(f"[Guardiao] Aviso: Não foi possível obter metadados do Glue para {db}.{table}. Erro: {e}")
                
        return lineage_details