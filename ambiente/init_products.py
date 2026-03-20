import os
from pathlib import Path

def create_data_products_structure():
    root_dir = "yggdra-data-products"
    print(f"🚀 Iniciando a construção do ecossistema de Produtos em: {root_dir}\n")

    # 1. Definição da Estrutura de Diretórios
    directories = [
        "products/sot_vendas",           # Exemplo de um produto real
        "products/sot_logistica",        # Outro produto
        "config/dev",                    # Parâmetros para ambiente DEV
        "config/prod",                   # Parâmetros para ambiente PROD
        "notebooks/exploracao",          # Jupyter Notebooks para prototipação
        "notebooks/analises_ad_hoc",     # Notebooks de negócio
        "tests/unit",                    # Testes de funções específicas
        "tests/integration",             # Testes do pipeline completo (Local)
        "local_data/mock_input",         # Arquivos Parquet/CSV fake para testar local
        "local_data/mock_output",        # Onde o job local vai salvar os resultados
        "local_data/mock_metadata",      # Onde os JSONs de Gêmeo Digital locais cairão
        "docs/arquitetura",              # Desenhos de solução (Mermaid, Draw.io)
        "docs/dicionarios_de_dados",     # Contratos de dados e schemas
        "presentations/kickoff",         # PPTXs ou Decks de apresentação pro negócio
        "presentations/resultados",      # Apresentações de entrega de valor
        "scripts/ci_cd",                 # Scripts de deploy do produto pro Glue
        "scripts/local_runners"          # Scripts para rodar o job do Glue na sua máquina
    ]

    # 2. Criar as pastas
    for dir_path in directories:
        full_path = Path(root_dir) / dir_path
        full_path.mkdir(parents=True, exist_ok=True)
        # Cria um .gitkeep para garantir que pastas vazias subam pro Git
        (full_path / ".gitkeep").touch()

    # 3. Criar Arquivos Base de um Produto Exemplo (SOT Vendas)
    sot_vendas_path = Path(root_dir) / "products" / "sot_vendas"
    
    # O Script principal do Glue
    with open(sot_vendas_path / "glue_job.py", "w", encoding="utf-8") as f:
        f.write('"""\nScript principal do AWS Glue para o produto SOT Vendas.\nImporta a Yggdra (via .zip no Glue) e executa o pipeline.\n"""\n')
        f.write("import sys\n# from yggdra.factory.orchestrator import main as yggdra_main\n\n")
        f.write("def run_job():\n    print('Iniciando ETL de Vendas...')\n\nif __name__ == '__main__':\n    run_job()\n")

    # A Regra de Negócio (SQL isolado do Python)
    with open(sot_vendas_path / "transform_rule.sql", "w", encoding="utf-8") as f:
        f.write("SELECT * FROM raw_vendas WHERE data = '{anomesdia}';\n")

    # 4. Criar Arquivos de Configuração (Desacoplados)
    with open(Path(root_dir) / "config" / "dev" / "sot_vendas_args.yaml", "w", encoding="utf-8") as f:
        f.write("DB: 'workspace_db_dev'\nTABLE_NAME: 'tb_vendas_consolidadas'\nREGION_NAME: 'us-east-2'\nPARTITION_NAME: 'anomesdia'\nLOG_LEVEL: 'DEBUG'\n")

    # 5. Criar Arquivos de Teste Local Base
    with open(Path(root_dir) / "tests" / "conftest.py", "w", encoding="utf-8") as f:
        f.write('"""\nArquivo de configuração do Pytest.\nAqui você cria fixtures (ex: mock do S3, mock do Athena) para os testes locais.\n"""\n')

    # 6. Criar Arquivos Raiz
    root_files = {
        ".gitignore": "venv/\n__pycache__/\n.pytest_cache/\nlocal_data/\n*.parquet\n.env\n",
        "requirements.txt": "pytest\npyaml\nboto3\nmoto\n# Adicione a Yggdra aqui futuramente para teste local\n",
        "README.md": "# 📊 Yggdra Data Products\nRepositório contendo os pipelines do AWS Glue, regras de negócio e testes.\n",
        ".env.example": "AWS_PROFILE=default\nENVIRONMENT=DEV\n"
    }

    for filename, content in root_files.items():
        with open(Path(root_dir) / filename, "w", encoding="utf-8") as f:
            f.write(content)

    print("✅ Estrutura criada com sucesso!")
    print(f"📂 Navegue para a pasta usando: cd {root_dir}")

if __name__ == "__main__":
    create_data_products_structure()