import logging
import sys
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional

class MaxLevelFilter(logging.Filter):
    """
    Filtro customizado para garantir que níveis de erro não vazem 
    para o handler de sucesso (stdout).
    """
    def __init__(self, max_level: int):
        super().__init__()
        self.max_level = max_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= self.max_level


class InMemoryHandler(logging.Handler):
    """Handler customizado para armazenar logs em memória como dicionários Python."""
    
    def __init__(self):
        super().__init__()
        self.records: List[Dict] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(timespec='seconds'),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "module": record.module
            }
            self.records.append(entry)
        except Exception:
            self.handleError(record)

    def to_json(self, indent: Optional[int] = 2) -> str:
        return json.dumps(self.records, ensure_ascii=False, indent=indent)

    def clear(self) -> None:
        self.records.clear()


class GenericLogger:
    """
    Abstração de log com suporte a múltiplas saídas e persistência em memória.
    Perfeito para auditoria de processos ETL.
    """

    _LEVELS = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    def __init__(
        self, 
        name: str = "YGGDRA", 
        level: str = "INFO", 
        propagate: bool = True,
        to_file: Optional[str] = None
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self._LEVELS.get(level.upper(), logging.INFO))
        self.logger.propagate = propagate 

        self.memory_handler = self._setup_handlers(to_file)

    def _setup_handlers(self, to_file: Optional[str] = None) -> InMemoryHandler:
        """Configura handlers de forma inteligente e roteia para o Output/Error logs do Glue."""
        
        # 1. Idempotência: Se já existir o handler de memória, não recria
        for handler in self.logger.handlers:
            if isinstance(handler, InMemoryHandler):
                return handler

        fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        formatter = logging.Formatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S")

        is_root = "." not in self.logger.name
        
        if is_root or not self.logger.propagate:
            
            
            
            # A. Handler de Sucesso (Output Logs - CloudWatch)
            # Imprime apenas níveis menores ou iguais a WARNING.
            stdout_h = logging.StreamHandler(sys.stdout)
            stdout_h.setFormatter(formatter)
            stdout_h.addFilter(MaxLevelFilter(logging.WARNING))
            self.logger.addHandler(stdout_h)

            # B. Handler de Erro (Error Logs - CloudWatch)
            # Imprime do ERROR em diante (vai direto pro sys.stderr por padrão).
            stderr_h = logging.StreamHandler(sys.stderr)
            stderr_h.setFormatter(formatter)
            stderr_h.setLevel(logging.ERROR) # O StreamHandler já ignora levels menores
            self.logger.addHandler(stderr_h)

            # -----------------------------------------------------------------

            # C. File Handler (Opcional, para rodar localmente salvando em .txt)
            if to_file:
                file_h = logging.FileHandler(to_file, encoding="utf-8")
                file_h.setFormatter(formatter)
                self.logger.addHandler(file_h)

        # 2. Memory Handler (Captura TUDO, independente do nível)
        mem_h = InMemoryHandler()
        self.logger.addHandler(mem_h)
        
        return mem_h

    # --- Atalhos para os métodos nativos ---
    def debug(self, msg, *args, **kwargs): self.logger.debug(msg, *args, **kwargs)
    def info(self, msg, *args, **kwargs): self.logger.info(msg, *args, **kwargs)
    def warning(self, msg, *args, **kwargs): self.logger.warning(msg, *args, **kwargs)
    def error(self, msg, *args, **kwargs): self.logger.error(msg, *args, **kwargs)
    def critical(self, msg, *args, **kwargs): self.logger.critical(msg, *args, **kwargs)

    # --- Gestão dos logs em memória ---
    def get_history(self) -> List[Dict]:
        return self.memory_handler.records

    def get_history_json(self, indent: int = 2) -> str:
        return self.memory_handler.to_json(indent=indent)

    def clear_history(self) -> None:
        self.memory_handler.clear()