import os
from pathlib import Path

def create_yggdra_structure():
    root_dir = "yggdra-sdk_2"
    
    # 🧠 Nomenclaturas melhoradas com base no ecossistema Yggdra
    sub_packages = {
        "core": ["clock.py", "utils.py", "data_utils.py"],
        "aws": ["s3_manager.py", "athena_manager.py", "glue_manager.py"],
        "observability": ["generic_logger.py", "report_manager.py", "metadata_manager.py"],
        "governance": ["source_guardian.py"],
        "factory": ["table_provisioner.py", "orchestrator.py"] 
    }

    print(f"🏗️ Criando estrutura profissional para: {root_dir}\n")

    # 1. Criar a estrutura única e limpa do SDK em src/
    base_path = Path(root_dir) / "src" / "yggdra"
    base_path.mkdir(parents=True, exist_ok=True)
    (base_path / "__init__.py").touch()
    
    for pkg, files in sub_packages.items():
        pkg_path = base_path / pkg
        pkg_path.mkdir(parents=True, exist_ok=True)
        (pkg_path / "__init__.py").touch()
        
        # Cria os arquivos placeholder das classes
        for file in files:
            (pkg_path / file).touch()
            
        print(f"✅ Módulo '{pkg}' criado.")

    # 2. Criar Diretório de CI/CD e Scripts
    scripts_path = Path(root_dir) / "scripts"
    scripts_path.mkdir(exist_ok=True)
    
    # O script de deploy agora empacota a pasta src/yggdra e manda para o S3 de DEV ou PROD
    (scripts_path / "deploy_aws.py").touch() 
    (scripts_path / "setup_env.py").touch()
    (scripts_path / "local_test_runner.py").touch()
    
    # 3. Criar Diretório de Testes (Unitários)
    tests_path = Path(root_dir) / "tests"
    tests_path.mkdir(exist_ok=True)
    (tests_path / "__init__.py").touch()

    # 4. Arquivos de Raiz
    root_files = ["pyproject.toml", ".gitignore", "README.md", ".env.example"]
    for file in root_files:
        (Path(root_dir) / file).touch()

    print(f"\n🚀 Tudo pronto! A estrutura da Yggdra foi inicializada.")

if __name__ == "__main__":
    create_yggdra_structure()