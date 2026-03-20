
import re
from ..aws.S3Manager import S3Manager 
from time import sleep

class S3Arquiteture:
    def __init__(self, args: dict, logger_name: str = 'YGGDRA'):
        """
        Orquestrador de arquitetura S3 para criação de estrutura de projetos e preparação de SQL para execução de tabelas (ex: Athena/Glue).

        -----------------------------
        📥 Parâmetros (args: dict)
        -----------------------------

        region_name : str, opcional
            Região AWS utilizada na inicialização do S3Manager.
            Default: 'sa-east-1'

        bucket_name : str, opcional
            Nome do bucket S3 onde a arquitetura será criada.
            Caso não informado, será utilizado o padrão:
            'itau-self-wkp-{region}-{account_id}'.
            Se um bucket customizado for informado, toda a estrutura será criada nele.

        db : str, obrigatório
            Nome do database.
            Define:
            - o database da tabela
            - a primeira camada do diretório no S3

        table_name : str, obrigatório
            Nome da tabela.
            Define:
            - o nome da tabela
            - a segunda camada do diretório no S3

        owner : str, obrigatório
            Email do responsável pela tabela/processo.
            Utilizado para governança e envio de erros.
            Deve ser um email válido e com domínio permitido.

        partition_name : str, obrigatório
            Nome da partição da tabela.

        partition_type : str, obrigatório
            Tipo da partição (ex: string, int, date).
            Define o comportamento do gerador de partições.

        path_sql_origem : str, obrigatório parcial
            Caminho S3 do arquivo SQL de origem.

        query : str, obrigatório parcial
            Query SQL (CTAS ou DDL) em formato string.
            Caso informado, o parâmetro 'path_sql_origem' será ignorado.

        ⚠️ Regra:
            É obrigatório informar pelo menos um dos parâmetros:
            - query
            - path_sql_origem

        -----------------------------
        ⚙️ Fluxo de execução
        -----------------------------

        1. Validação dos parâmetros obrigatórios
        2. Validação do email do owner (formato + domínio permitido)
        3. Criação da estrutura de diretórios no S3
        4. Tratamento do SQL:
            - Se 'path_sql_origem' for informado:
                copia o arquivo para a pasta /sql do projeto
            - Se 'query' for informada:
                utiliza diretamente o conteúdo
        5. Leitura do conteúdo SQL
        6. Retorno dos metadados do projeto

        -----------------------------
        📂 Estrutura S3 gerada
        -----------------------------

        {
            "root": "s3://bucket/db/table/",
            "scripts": "s3://bucket/db/table/scripts/",
            "data": "s3://bucket/db/table/data/",
            "sql": "s3://bucket/db/table/sql/",
            "temp": "s3://bucket/db/table/temp/",
            "logs": "s3://bucket/db/table/logs/",
            "reports": "s3://bucket/db/table/reports/",
            "metadata": "s3://bucket/db/table/metadata/"
        }

        -----------------------------
        📤 Retorno (build / run)
        -----------------------------

        dict:
        {
            "structure": list,
                Dicionário contendo os caminhos das pastas no S3

            "query": str,
                Conteúdo da query SQL a ser executada

            "bucket": str,
                Nome do bucket utilizado no projeto

            "project_path": str
                Caminho raiz do projeto (db/table)
        }

        -----------------------------
        ❌ Exceções
        -----------------------------

        ValueError:
            - Parâmetros obrigatórios não informados
            - Email inválido
            - Domínio de email não permitido
            - Ausência de 'query' e 'path_sql_origem'

        TypeError:
            - Tipos inválidos para 'query' ou 'path_sql_origem'

        -----------------------------
        💡 Observações
        -----------------------------

        - Caso os parâmetros obrigatórios não sejam informados, o processo será interrompido imediatamente.
        - A validação é executada automaticamente na inicialização e na execução.
        - A estrutura criada segue padrão de organização para projetos de dados (data lake).
        """

        self.args = args
        self.logger_name = logger_name
        
        self._validate()  
        
        self.s3 = S3Manager(
            region_name=args['region_name'],
            logger_name=logger_name
        )
        
        self.bucket_name = args.get('bucket_name') or self.s3.get_bucket_default()
        self.project_name = f"{args['db']}/{args['table_name']}"
        self.db = args['db']
        self.table_name = args['table_name']
        self.structure = None
        self.query = None
        self.primeria_execucao = 0 


   
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
        
        self._validate_owner_email()

    # 🔹 Step 1 - Setup estrutura
    def setup_structure(self):
        prefix = f"{self.project_name}/"

        # 🔍 verifica se já existe
        if self.s3.prefix_exists(self.bucket_name, prefix):
            self.s3.logger.info(
                f"Estrutura já existe em s3://{self.bucket_name}/{prefix}."
            )

            self.structure = self.s3.setup_project(
                bucket_name=self.bucket_name,
                project_name=self.project_name
            )
            return self
        else:
            # 🚀 cria normalmente
            self.structure = self.s3.setup_project(
                bucket_name=self.bucket_name,
                project_name=self.project_name
            )
            self.primeria_execucao = 1


        return self

    def prepare_sql(self):
        prefix = f"{self.project_name}/"
        filename = f"{self.args['table_name']}"


        sql_prefix = f"{self.project_name}/sql"

        # 🚀 PRIMEIRA EXECUÇÃO
        if self.primeria_execucao == 1:
            if self.args.get('query'):
                self.s3.logger.info("Salvando query no diretório do projeto")

                self.s3.write_text_file(
                    content=self.args['query'],
                    extension='.sql',
                    filename=filename,
                    bucket_name=self.bucket_name,
                    prefix=sql_prefix
                )

            else:
                self.s3.logger.info("Copiando SQL de origem para o projeto")

                self.s3.copy_file(
                    source_file_uri=self.args['path_sql_origem'],
                    target_folder_uri=self.structure['sql'],
                    target_file_name=f'{filename}.sql'
                )
            print('sleep')
            sleep(2)
        # 🔁 EXECUÇÕES POSTERIORES
        else:
            self.s3.logger.info(
                "Utilizando SQL armazenado no diretório do projeto"
            )

        # 📥 SEMPRE lê do diretório do projeto
        self.query = self.s3.get_content_sql(
            bucket=self.bucket_name,
            prefix=sql_prefix,
            filename=f'{filename}.sql'
        )

        return self

    # 🔹 Step final
    def build(self) -> dict:
        return {
            "structure": self.structure,
            "query": self.query,
            "bucket": self.bucket_name,
            "project_path": self.project_name
        }
    
    def run(self) -> dict:
        return (
            self
            .setup_structure()
            .prepare_sql()
            .build()
        )
    

