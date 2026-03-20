import json
from datetime import datetime, timezone
from typing import Dict, Any, List

class MetadataManager:
    """
    Responsável por consolidar a 'Digital Twin' da execução.
    Captura caminhos, parâmetros, SQL e métricas para consumo externo.
    """
    def __init__(self, job_args: Dict[str, Any]):
        self.start_time = datetime.now(timezone.utc)
        self.job_args = job_args
        self.artifacts = {}
        self.metrics = {
            "partitions_processed": 0,
            "success_count": 0,
            "failed_count": 0,
            "total_duration_sec": 0.0
        }
        self.sql_context = ""
        self.sources = []  # <--- Nova lista para armazenar as origens
        self.execution_id = f"exec_{self.start_time.strftime('%Y%m%d_%H%M%S')}"

    def register_sql(self, sql_content: str):
        """Armazena o código SQL utilizado (versão final)."""
        self.sql_context = sql_content

    
    def add_source(self, db: str, table: str, partition_type: str, partition_name: str, partition_value: Any, defasagem: int = 0):
        """
        Adiciona uma tabela de origem ao lineage da execução, incluindo seu SLA/Defasagem.
        """
        source_entry = {
            "database": db,
            "table": table,
            "partition_type": partition_type,
            "partition_name": partition_name,
            "partition_value": partition_value,
            "defasagem": defasagem 
        }
        self.sources.append(source_entry)

    def register_artifacts(self, structure: Dict[str, str], report_uri: str = None, log_uri: str = None):
        """Mapeia todos os caminhos físicos envolvidos na execução."""
        self.artifacts = {
            "s3_structure": structure,
            "execution_report_uri": report_uri,
            "execution_log_uri": log_uri,
            "target_table": f"{self.job_args.get('db')}.{self.job_args.get('tabel_name')}"
        }

    def update_metrics(self, success: int, failures: int, duration: float):
        """Atualiza os KPIs da rodada."""
        self.metrics["success_count"] = success
        self.metrics["failed_count"] = failures
        self.metrics["partitions_processed"] = success + failures
        self.metrics["total_duration_sec"] = round(duration, 2)

    def generate_metadata_dict(self) -> Dict[str, Any]:
        """Gera o dicionário final para persistência ou retorno de API."""
        end_time = datetime.now(timezone.utc)
        
        metadata = {
            "execution_id": self.execution_id,
            "product": "YGGRA - Data Factory",
            "environment": {
                "region": self.job_args.get('region_name'),
                "start_at": self.start_time.isoformat(),
                "end_at": end_time.isoformat(),
            },
            "parameters": self.job_args,
            "artifacts": self.artifacts,
            "metrics": self.metrics,
            "lineage": {
                "sql_applied": self.sql_context,
                "upstream_sources": self.sources 
            }
        }
        return metadata

    def to_json(self) -> str:
        return json.dumps(self.generate_metadata_dict(), indent=2, ensure_ascii=False)