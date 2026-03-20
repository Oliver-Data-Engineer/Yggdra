import time

class Clock:
    """
    Classe utilitária para medição de performance em pipelines de dados.
    Padroniza o cálculo de tempo decorrido.
    """
    def __init__(self) -> None:
        self._start_time = None
        self._elapsed_seconds = 0.0

    def start(self):
        """Inicia ou reinicia o cronômetro."""
        self._start_time = time.perf_counter()

    def stop(self) -> float:
        """
        Para o cronômetro e retorna o tempo em segundos.
        Também atualiza o estado interno em minutos conforme sua preferência.
        """
        if self._start_time is None:
            return 0.0
        
        duration = time.perf_counter() - self._start_time
        self._elapsed_seconds = duration
        return self._elapsed_seconds

    @property
    def elapsed_minutes(self) -> float:
        """Retorna o tempo decorrido em minutos (formatado)."""
        return round(self._elapsed_seconds / 60, 4)

    @property
    def elapsed_seconds(self) -> float:
        """Retorna o tempo decorrido em segundos (formatado)."""
        return round(self._elapsed_seconds, 2)

    @property
    def formatted(self) -> str:
        """Retorna uma string amigável para logs e relatórios."""
        if self._elapsed_seconds < 60:
            return f"{self.elapsed_seconds}s"
        return f"{self.elapsed_minutes} min"

    # Setters conforme solicitado
    @elapsed_seconds.setter
    def elapsed_seconds(self, value):
        self._elapsed_seconds = value