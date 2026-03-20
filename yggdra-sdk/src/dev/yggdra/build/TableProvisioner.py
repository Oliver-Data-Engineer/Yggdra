from datetime import datetime
from ..aws.S3Manager import S3Manager
from ..aws.AthenaManager import AthenaManager
from ..observability.MetadataManager import MetadataManager
from ..core.GenericLogger import GenericLogger

class TableProvisioner:
    """
    Produto da Yggdra responsável por provisionar a infraestrutura de uma nova tabela (SOT).
    Cria os caminhos físicos no S3 (Bootstrap), monta e executa o DDL no Athena, e gera o Metadata inicial.
    """
    def __init__(self, region_name: str, logger_name: str = 'TableProvisioner'):
        self.region_name = region_name
        self.logger = GenericLogger(name=logger_name, level="INFO")
        self.s3 = S3Manager(region_name=self.region_name, logger_name=logger_name)
        self.athena = AthenaManager(region_name=self.region_name, logger_name=logger_name)

    def generate_ddl(self, db: str, table: str, columns: dict, partitions: dict, location_uri: str) -> str:
        """Monta a string do DDL em formato Presto/Athena."""
        cols_formatted = ",\n    ".join([f"`{col}` {dtype}" for col, dtype in columns.items()])
        parts_formatted = ",\n    ".join([f"`{col}` {dtype}" for col, dtype in partitions.items()])
        
        ddl = f"""CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table}` ({cols_formatted}
                )
                PARTITIONED BY (
                    {parts_formatted}
                )
                STORED AS PARQUET
                LOCATION '{location_uri}'
                TBLPROPERTIES ('classification'='parquet');"""
                        
        return ddl

    def provision(self, args: dict):
        self.logger.info(f"Iniciando provisionamento para {args['db']}.{args['table_name']}")
        
        # 1. Resolve Parâmetros Opcionais e Defaults
        bucket = args.get('bucket') or self.s3.get_bucket_default()
        prefix = args.get('prefix') or f"{args['db']}/{args['table_name']}"
        workgroup = args.get('workgroup', 'primary') # Extrai o workgroup se existir, senão usa 'primary'
        
        # 2. Bootstrap S3 (Cria /data, /metadata, /logs, /sql, /reports)
        self.logger.info(f"Configurando estrutura S3 em s3://{bucket}/{prefix}")
        structure = self.s3.setup_project(bucket_name=bucket, project_name=prefix)
        
        # 3. Geração do DDL
        ddl_query = self.generate_ddl(
            db=args['db'],
            table=args['table_name'],
            columns=args['columns'],
            partitions=args['partition_columns'],
            location_uri=structure['data']
        )
        
        self.logger.debug(f"DDL Gerado:\n{ddl_query}")
        
        # 4. Execução no Athena (Criação no Glue Catalog)
        self.logger.info("Executando DDL no Athena...")
        
        # ========================================================
        # AQUI ESTÁ A CORREÇÃO DA CHAMADA
        # ========================================================
        resp = self.athena.execute_query(
            sql=ddl_query, 
            database=args['db'], 
            output_s3=structure['temp'], # Passa diretamente a string da URI do S3
            workgroup=workgroup          # Repassa o workgroup opcional
        )
        
        # 5. Registro de Metadados (O Gêmeo Digital)
        self.logger.info("Gerando artefatos de Metadados...")
        meta_manager = MetadataManager(job_args=args)
        meta_manager.register_sql(ddl_query)
        meta_manager.register_artifacts(structure=structure)
        meta_manager.artifacts["original_ddl"] = ddl_query
        meta_manager.metrics["total_duration_sec"] = resp.get('elapsed_sec', 0.0)
        
        metadata_json = meta_manager.to_json()
        
        # 6. Salvar Metadado no S3
        meta_uri = self.s3.write_text_file(
            bucket=bucket,
            prefix=f"{prefix}/metadata",
            filename=f"bootstrap_meta_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            content=metadata_json,
            extension="json"
        )
        self.logger.info(f"Provisionamento concluído. Metadados salvos em: {meta_uri}")
        
        return {"status": "success", "structure": structure, "metadata_uri": meta_uri}