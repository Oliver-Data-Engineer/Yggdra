import os
from pathlib import Path

def create_yggdra_dev_prod_structure():
    root_dir = "yggdra-sdk"
    
    # Estrutura base que se repete em DEV e PROD
    # Isso garante que a importação seja idêntica em ambos
    sub_packages = [
        "core",      # logger, utils, clock
        "aws",       # athena, s3, glue, client
        "security",  # guardian, source_guardian
        "metadata"   # metadata, table_prodisioner
    ]
    
    # Arquivos base para cada sub-pacote
    base_files = ["__init__.py"]

    print(f"🏗️ Criando estrutura espelhada (DEV/PROD) para: {root_dir}\n")

    # 1. Criar pastas de Ambiente (DEV e PROD)
    for env in ["dev", "prod"]:
        env_path = Path(root_dir) / "src" / env / "yggdra"
        
        for pkg in sub_packages:
            pkg_path = env_path / pkg
            pkg_path.mkdir(parents=True, exist_ok=True)
            
            # Criar arquivos iniciais em cada sub-pasta
            for file in base_files:
                (pkg_path / file).touch()
        
        # Init principal da yggdra por ambiente
        (env_path / "__init__.py").touch()
        print(f"✅ Estrutura de {env.upper()} criada em: {env_path}")

    # 2. Criar Diretório de Scripts (Setup e Deploy)
    scripts_path = Path(root_dir) / "scripts"
    scripts_path.mkdir(exist_ok=True)
    
    # O script que você pediu para enviar de dev -> prod
    deploy_script = scripts_path / "deploy_to_prod.py"
    deploy_script.touch()
    
    # Outros utilitários de setup
    (scripts_path / "setup_env.py").touch()
    (scripts_path / "local_test_runner.py").touch()

    # 3. Arquivos de Raiz
    root_files = ["pyproject.toml", ".gitignore", "README.md", ".env"]
    for file in root_files:
        (Path(root_dir) / file).touch()

    print(f"\n🚀 Tudo pronto! A pasta 'scripts/deploy_to_prod.py' foi criada.")

if __name__ == "__main__":
    create_yggdra_dev_prod_structure()