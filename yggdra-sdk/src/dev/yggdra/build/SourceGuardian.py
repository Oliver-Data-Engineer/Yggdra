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

    def _infer_partition_format(self, sample_partitions: List[Any], p_types: List[str], p_names: List[str] = None) -> str:
        """
        Analisa a amostra de partições (e suas chaves) para inferir o formato da data.
        Agora suporta partições compostas (ex: ['year', 'month', 'day'] -> '%Y/%m/%d').
        
        :param sample_partitions: Lista de partições extraídas do Glue (strings ou listas).
        :param p_types: Lista dos tipos de dados de cada chave de partição.
        :param p_names: Lista com os nomes das chaves de partição (opcional, mas vital para compostas).
        """
        if not sample_partitions:
            return None

        # =====================================================================
        # 1. TRATAMENTO PARA PARTIÇÕES COMPOSTAS (Ex: year=2024, month=03)
        # =====================================================================
        if len(p_types) > 1:
            if not p_names:
                return None # Sem o nome da coluna, não conseguimos ter certeza
            
            # Converte os nomes das colunas para letras minúsculas para padronizar
            p_names_lower = [name.lower() for name in p_names]
            
            # Cria a string de formato baseada na ordem das colunas
            format_parts = []
            for name in p_names_lower:
                if 'ano' in name or 'year' in name:
                    format_parts.append('%Y')
                elif 'mes' in name or 'month' in name:
                    format_parts.append('%m')
                elif 'dia' in name or 'day' in name:
                    format_parts.append('%d')
                else:
                    return None # Tem uma coluna desconhecida no meio (ex: 'id_pais'), aborta inferência
                
            # Retorna o formato exato que o Heimdall vai usar após dar o ".join()"
            # Ex: Se as colunas forem ['ano', 'mes', 'dia'], retorna '%Y/%m/%d'
            if format_parts:
                return "/".join(format_parts)
            return None

        # =====================================================================
        # 2. TRATAMENTO PARA PARTIÇÕES SIMPLES (String)
        # =====================================================================
        p_type = p_types[0].lower()
        if p_type not in ['string', 'date', 'timestamp']:
            return None

        def all_match(pattern: str) -> bool:
            return all(isinstance(p, str) and re.match(pattern, p) for p in sample_partitions)

        # CASOS ESPECIAIS (Mensal disfarçado de diário)
        if all_match(r'^\d{4}-\d{2}-01( 00:00:00)?$'): return "%Y-%m-01"
        if all_match(r'^\d{4}\d{2}01$'): return "%Y%m01"

        # CASOS PADRÕES
        if all_match(r'^\d{4}-\d{2}-\d{2}( 00:00:00)?$'): return "%Y-%m-%d"
        if all_match(r'^\d{8}$'): return "%Y%m%d"
        if all_match(r'^\d{6}$'): return "%Y%m"
        if all_match(r'^\d{4}$'): return "%Y"

        return None

    def map_upstream_lineage(self, query: str) -> List[Dict[str, Any]]:
                """
                Inspeciona a query e retorna a linhagem estruturada com metadados do Glue,
                incluindo a inferência inteligente de formato de partição e defasagem esperada.
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
                        expected_defasagem = 0  # 💡 Inicia assumindo 0 (Mensal/Default)
                        
                        if partition_keys:
                            
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
                                # 💡 NOVO: Passando a lista de partition_keys para suportar compostas!
                                inferred_format = self._infer_partition_format(
                                    sample_partitions=last_3_partitions, 
                                    p_types=partition_types,
                                    p_names=partition_keys 
                                )

                            # =========================================================
                            # 🧠 LÓGICA DE INFERÊNCIA DA DEFASAGEM ESPERADA (LAG)
                            # =========================================================
                            if inferred_format:
                                # 💡 NOVO: O formato diário composto '%Y/%m/%d' entra na lista de Lag = 1
                                if inferred_format in ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]:
                                    expected_defasagem = 1
                            else:
                                # Fallback: Se não detectou formato, tenta inferir pelo nome da coluna (Apenas simples)
                                if len(partition_keys) == 1:
                                    p_name_lower = partition_keys[0].lower()
                                    if any(keyword in p_name_lower for keyword in ['data', 'dia', 'dt', 'anomesdia']):
                                        expected_defasagem = 1

                        # Estrutura limpa, validada e com os novos metadados
                        source_info = {
                            "path_reference": path_complete,
                            "database": db,
                            "table": table,
                            "partition_keys": partition_keys,
                            "partition_types": partition_types,
                            "last_partition_value": last_partition_value,
                            "inferred_format": inferred_format,
                            "expected_defasagem": expected_defasagem  # 💡 NOVO ATRIBUTO ALOCADO
                        }
                        
                        lineage_details.append(source_info)
                        
                    except Exception as e:
                        print(f"[Guardiao] Aviso: Não foi possível obter metadados do Glue para {db}.{table}. Erro: {e}")
                        
                return lineage_details