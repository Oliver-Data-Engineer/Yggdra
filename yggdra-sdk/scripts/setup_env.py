import os
import subprocess
import sys
from pathlib import Path
import venv

def get_venv_python_path(venv_dir: Path) -> Path:
    """
    Retorna o caminho correto para o executável do Python dentro do ambiente virtual,
    lidando com a diferença de diretórios entre Windows e Linux/Mac.
    """
    if os.name == 'nt':  # Windows
        return venv_dir / "Scripts" / "python.exe"
    else:                # Linux / MacOS
        return venv_dir / "bin" / "python"

def create_default_requirements(root_dir: Path):
    """
    Cria um arquivo requirements.txt padrão para a plataforma Yggdra, 
    caso ele não exista, contendo todas as dependências core e de observabilidade.
    """
    req_path = root_dir / "requirements.txt"
    if not req_path.exists():
        print("📝 Arquivo requirements.txt não encontrado. Criando lista padrão Yggdra...")
        default_packages = [
            "boto3>=1.28.0",         # Core de comunicação com a AWS (S3, Athena, Glue)
            "pytest>=7.4.0",         # Framework robusto para testes locais
            "moto>=4.2.0",           # Mock para simular S3/Glue localmente sem custo AWS
            "sqlglot>=18.0.0",       # Motor de parsing de SQL para o SourceGuardian
            "python-dotenv>=1.0.0",  # Gerenciamento de variáveis de ambiente (.env)
            "tabulate",              # Formatação de tabelas (ideal para o ReportManager)
            "pandas"                 # Manipulação de dados estruturados e validação em testes locais
        ]
        with open(req_path, "w", encoding="utf-8") as f:
            f.write("\n".join(default_packages))
        print("✅ requirements.txt criado com sucesso com as novas dependências (pandas e tabulate).")
    else:
        print("ℹ️ Arquivo requirements.txt já existe. Nenhuma alteração feita neste arquivo.")

def setup_environment():
    """
    Função principal que orquestra a criação do ambiente e instalação de dependências.
    """
    print("🌳 Iniciando o Setup da Plataforma Yggdra...")
    
    # Define a raiz do projeto (voltando um nível a partir da pasta 'scripts')
    current_dir = Path(__file__).resolve().parent
    root_dir = current_dir.parent
    venv_dir = root_dir / "venv"

    # 1. Cria o Ambiente Virtual
    if not venv_dir.exists():
        print(f"📦 Criando ambiente virtual em: {venv_dir} ...")
        venv.create(venv_dir, with_pip=True)
        print("✅ Ambiente virtual criado.")
    else:
        print("ℹ️ O ambiente virtual já existe. Pulando criação.")

    # 2. Prepara as dependências
    create_default_requirements(root_dir)

    # 3. Descobre o caminho do Python do VENV
    venv_python = get_venv_python_path(venv_dir)

    if not venv_python.exists():
        print(f"❌ Erro Crítico: Executável do Python não encontrado em {venv_python}")
        sys.exit(1)

    # 4. Atualiza o PIP e Instala os pacotes
    print("\n⏳ Atualizando pip e instalando bibliotecas... (Isso pode levar alguns minutos)")
    try:
        # Atualiza o pip primeiro
        subprocess.check_call([str(venv_python), "-m", "pip", "install", "--upgrade", "pip"])
        
        # Instala as libs do requirements.txt
        req_file = root_dir / "requirements.txt"
        subprocess.check_call([str(venv_python), "-m", "pip", "install", "-r", str(req_file)])
        
        print("\n🚀 Setup concluído com sucesso! Todas as dependências da Yggdra estão instaladas.")
        
        # Dica de ouro para o usuário no terminal
        print("-" * 50)
        print("Para ativar o ambiente virtual, rode no seu terminal:")
        if os.name == 'nt':
            print(f">>> {venv_dir.name}\\Scripts\\activate")
        else:
            print(f">>> source {venv_dir.name}/bin/activate")
        print("-" * 50)

    except subprocess.CalledProcessError as e:
        print(f"\n❌ Falha durante a instalação via pip. Erro: {e}")
        sys.exit(1)

if __name__ == "__main__":
    setup_environment()