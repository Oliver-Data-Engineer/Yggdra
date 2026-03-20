import os
from pathlib import Path

def create_yggdra_platform_structure():
    root_dir = "yggdra-data-platform"
    print(f"🏗️ Inicializando a Plataforma de Dados: {root_dir}\n")

    # 1. Definição da Estrutura de Diretórios
    directories = {
        # A Biblioteca Core (O cérebro)
        "src/yggdra/core": ["clock.py", "utils.py", "data_utils.py", "__init__.py"],
        "src/yggdra/aws": ["s3_manager.py", "athena_manager.py", "glue_manager.py", "__init__.py"],
        "src/yggdra/observability": ["generic_logger.py", "report_manager.py", "metadata_manager.py", "__init__.py"],
        "src/yggdra/governance": ["source_guardian.py", "__init__.py"],
        "src/yggdra/factory": ["table_provisioner.py", "orchestrator.py", "__init__.py"],
        
        # Os Motores / Pipelines (Os Jobs do AWS Glue)
        "pipelines/sot_factory": ["job_incremental.py", "job_first_load.py", "config.json"],
        "pipelines/data_quality": ["job_vigia.py"],
        
        # Infraestrutura como Código (Terraform)
        "terraform/environments/dev": ["main.tf", "variables.tf", "terraform.tfvars"],
        "terraform/environments/prod": ["main.tf", "variables.tf", "terraform.tfvars"],
        "terraform/modules/glue_job": ["main.tf", "variables.tf", "outputs.tf"],
        "terraform/modules/s3_bucket": ["main.tf", "variables.tf", "outputs.tf"],
        
        # Testes Locais Robustos
        "tests/unit": ["test_data_utils.py", "test_metadata_manager.py", "__init__.py"],
        "tests/integration": ["test_glue_pipeline.py", "__init__.py"],
        "tests/mocks": ["mock_s3_events.json", "mock_athena_responses.json"], # Para testes sem custo AWS
        
        # Documentação e Apresentações
        "docs/architecture": ["diagrama_c4.md", "fluxo_dados.md"],
        "docs/presentations": ["onboarding_engenharia.pptx", "pitch_negocios.pdf"],
        "docs/api": ["yggdra_reference.md"],
        
        # Scripts de Automação (CI/CD e Setup)
        "scripts": ["deploy_glue_jobs.sh", "package_yggdra.py", "run_local_tests.sh"]
    }

    # 2. Criação Física das Pastas e Arquivos
    base_root = Path(root_dir)
    base_root.mkdir(parents=True, exist_ok=True)

    for folder_path, files in directories.items():
        full_folder_path = base_root / folder_path
        full_folder_path.mkdir(parents=True, exist_ok=True)
        
        for file in files:
            file_path = full_folder_path / file
            # Só cria o arquivo se ele não existir (evita sobrescrever se rodar o script 2x)
            if not file_path.exists():
                file_path.touch()

    # 3. Arquivos Raiz do Repositório
    root_files = {
        "pyproject.toml": "# Configuração do pacote Python e dependências (ex: pytest, boto3)\n",
        "requirements.txt": "boto3\npytest\nmoto\n",
        ".gitignore": "venv/\n.terraform/\n*.tfstate\n__pycache__/\n*.zip\n.env\n",
        "README.md": "# 🌳 Yggdra Data Platform\n\nRepositório central de engenharia de dados.\n",
        "conftest.py": "# Configurações globais do Pytest e fixtures do Moto (AWS Mock)\n",
        ".env.example": "AWS_DEFAULT_REGION=us-east-2\nENV=dev\n"
    }

    for file_name, content in root_files.items():
        file_path = base_root / file_name
        if not file_path.exists():
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)

    print("✅ Estrutura de Plataforma construída com sucesso!")
    print(f"👉 Acesse o diretório usando: cd {root_dir}")

if __name__ == "__main__":
    create_yggdra_platform_structure()