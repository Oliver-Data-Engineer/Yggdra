# Importa as classes dos arquivos locais
from .AthenaManager import AthenaManager
from .S3Manager import S3Manager
from .GlueManager import GlueManager
from .AwsClient import AWSClient

# Define a API pública do pacote 'aws'
__all__ = [
    "AthenaManager",
    "S3Manager",
    "GlueManager",
    "AWSClient"
]