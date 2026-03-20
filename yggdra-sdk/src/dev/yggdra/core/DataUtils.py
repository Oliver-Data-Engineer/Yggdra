from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Union, Any, Optional
import re

class DataUtils:
    """
    Classe utilitária para orquestração de tempo e partições.
    Suporta defasagem (D-x), reprocessamento condicional e múltiplos formatos.
    """

    @staticmethod
    def format_partition(date_ref: Any, p_type: str, p_format: Optional[str] = None) -> str:
        """Fábrica de formatos de data. Prioriza p_format se existir."""
        if isinstance(date_ref, (int, str)):
            str_date = str(date_ref).strip()
            try:
                if "-" in str_date:
                    dt = datetime.strptime(str_date[:10], "%Y-%m-%d")
                elif len(str_date) == 6:
                    dt = datetime.strptime(str_date, "%Y%m")
                else:
                    dt = datetime.strptime(str_date[:8], "%Y%m%d")
            except ValueError:
                return str(date_ref)
        else:
            dt = date_ref

        # Se o usuário passou um formato explícito (ex: '%Y-%m-01'), usa ele
        if p_format:
            return dt.strftime(p_format)

        # Fallback para os formatos padrões baseados no tipo
        formats = {
            "ano": "%Y",
            "mes": "%m",
            "anomes": "%Y%m",
            "anomesdia": "%Y%m%d",
            "data": "%Y-%m-%d"
        }
        return dt.strftime(formats.get(p_type.lower(), "%Y-%m-%d"))

    @staticmethod
    def _get_base_dates(
        p_type: str,
        dt_ini: Union[int, str], 
        dt_fim: Union[int, str], 
        reprocessamento: bool, 
        range_reprocessamento: int, 
        dia_corte: Optional[int] = None,
        defasagem: int = 0,
        p_format: Optional[str] = None
    ) -> List[datetime]: 
        """
        Calcula a lista base de datas considerando defasagem, formato e reprocessamento.
        """
        hoje = datetime.combine(date.today(), datetime.min.time())
        hoje_dia = date.today().day
        
        # Define se o grão é diário ou mensal
        is_daily = p_type.lower() in ["data", "anomesdia"]
        
        # REGRA DE EXCEÇÃO: Se for sempre o primeiro dia do mês, o salto deve ser mensal
        if p_format == "%Y-%m-01":
            is_daily = False

        # 1. Definição do Salto (Delta)
        step = timedelta(days=1) if is_daily else relativedelta(months=1)

        # 2. Cálculo Automático (Âncora e Defasagem)
        if int(dt_ini) == 190001 and int(dt_fim) == 190001:
            # Define o ponto de referência (hoje ou 1º do mês)
            anchor = hoje if is_daily else hoje.replace(day=1)
            
            # APLICA A DEFASAGEM: Recua o ponto final antes de qualquer outra lógica
            dt_fim_dt = anchor - (step * int(defasagem))
            
            # Padrão: Apenas o dia/mês defasado
            dt_ini_dt = dt_fim_dt 

            # 3. Lógica de Reprocessamento Condicional
            if reprocessamento:
                deve_reprocessar = (dia_corte is None) or (hoje_dia == dia_corte)
                if deve_reprocessar:
                    dt_ini_dt = dt_fim_dt - (step * int(range_reprocessamento))
        
        else:
            # Caso de Datas Manuais (sempre obedece o range informado)
            str_ini, str_fim = str(dt_ini), str(dt_fim)
            fmt_ini = "%Y%m%d" if len(str_ini) > 6 else "%Y%m"
            fmt_fim = "%Y%m%d" if len(str_fim) > 6 else "%Y%m"
            dt_ini_dt = datetime.strptime(str_ini, fmt_ini)
            dt_fim_dt = datetime.strptime(str_fim, fmt_fim)

        if dt_fim_dt < dt_ini_dt:
            return []

        # 4. Geração da Lista
        dates = []
        current = dt_ini_dt
        while current <= dt_fim_dt:
            dates.append(current)
            current += step
            
        return dates

    @staticmethod
    def generate_partitions(
        p_type: str,
        dt_ini: Union[int, str] = 190001,
        dt_fim: Union[int, str] = 190001,
        reprocessamento: bool = False,
        range_reprocessamento: int = 0,
        dia_corte: Optional[int] = None,
        defasagem: int = 0,
        p_format: Optional[str] = None
    ) -> List[str]:
        """Gera a lista final de strings formatadas para o loop do ETL."""
        base_dates = DataUtils._get_base_dates(
            p_type, dt_ini, dt_fim, reprocessamento, range_reprocessamento, dia_corte, defasagem, p_format
        )
        return [DataUtils.format_partition(d, p_type, p_format) for d in base_dates]
    
    @staticmethod
    def expand_date_variables(partition_value: str) -> dict:
        """
        Recebe um valor de partição genérico (ex: '20231027', '2023-10-27', '2023/10/27')
        e retorna um dicionário com múltiplas variações formatadas para interpolação SQL.
        """
        clean_val = re.sub(r'\D', '', str(partition_value))
        
        try:
            if len(clean_val) == 8:    # YYYYMMDD
                dt = datetime.strptime(clean_val, '%Y%m%d')
            elif len(clean_val) == 6:  # YYYYMM
                dt = datetime.strptime(clean_val, '%Y%m')
            elif len(clean_val) == 4:  # YYYY
                dt = datetime.strptime(clean_val, '%Y')
            else:
                raise ValueError("Tamanho de data não mapeado")

            return {
                "anomesdia": dt.strftime('%Y%m%d'),      
                "anomes": dt.strftime('%Y%m'),           
                "data": dt.strftime('%Y-%m-%d'),         
                "year": dt.strftime('%Y'),               
                "month": dt.strftime('%m'),              
                "day": dt.strftime('%d')                 
            }
            
        except Exception as e:
            return {
                "anomesdia": partition_value,
                "anomes": partition_value,
                "data": partition_value,
                "year": partition_value,
                "month": partition_value,
                "day": partition_value
            }
    
    @staticmethod
    def calcular_defasagem(
        partition_value: Union[str, dict], 
        partition_type: str = "", 
        defasagem: int = 0,
        partition_format: Optional[str] = None
    ) -> Union[str, dict]:
        """
        Calcula a partição de defasagem (lag).
        Respeita formatos específicos como '%Y-%m-01' tratando-os como grão mensal.
        """
        if defasagem == 0:
            return partition_value

        # ==========================================================
        # 1. LÓGICA PARA PARTIÇÕES MÚLTIPLAS (Dict: year, month, day)
        # ==========================================================
        if isinstance(partition_value, dict):
            keys = {k.lower(): k for k in partition_value.keys()}
            
            try:
                # Cenário A: Tem year, month e day (Grão: Dia)
                if 'day' in keys and 'month' in keys and 'year' in keys:
                    dt = datetime(
                        int(partition_value[keys['year']]),
                        int(partition_value[keys['month']]),
                        int(partition_value[keys['day']])
                    )
                    nova_dt = dt - relativedelta(days=defasagem)
                    
                    return {
                        keys['year']: nova_dt.strftime('%Y'),
                        keys['month']: nova_dt.strftime('%m'),
                        keys['day']: nova_dt.strftime('%d')
                    }
                
                # Cenário B: Tem apenas year e month (Grão: Mês)
                elif 'month' in keys and 'year' in keys:
                    dt = datetime(
                        int(partition_value[keys['year']]),
                        int(partition_value[keys['month']]),
                        1
                    )
                    nova_dt = dt - relativedelta(months=defasagem)
                    
                    return {
                        keys['year']: nova_dt.strftime('%Y'),
                        keys['month']: nova_dt.strftime('%m')
                    }
                
                # Cenário C: Tem apenas year (Grão: Ano)
                elif 'year' in keys:
                    dt = datetime(int(partition_value[keys['year']]), 1, 1)
                    nova_dt = dt - relativedelta(years=defasagem)
                    
                    return {
                        keys['year']: nova_dt.strftime('%Y')
                    }
            except Exception as e:
                print(f"[DataUtils] Erro na defasagem de dicionário: {e}")
                return partition_value

        # ==========================================================
        # 2. LÓGICA PARA PARTIÇÕES SIMPLES (String)
        # ==========================================================
        str_val = str(partition_value)
        part_type_lower = partition_type.lower()
        has_dash = '-' in str_val

        try:
            # REGRA DE NEGÓCIO: Tratamento especial para o formato %Y-%m-01
            if partition_format == '%Y-%m-01':
                dt = datetime.strptime(str_val, partition_format)
                # Defasagem opera em meses
                nova_dt = dt - relativedelta(months=defasagem)
                return nova_dt.strftime(partition_format)

            elif 'anomesdia' in part_type_lower or 'data' in part_type_lower:
                dt_format = '%Y-%m-%d' if has_dash else '%Y%m%d'
                dt = datetime.strptime(str_val, dt_format)
                nova_dt = dt - relativedelta(days=defasagem)
                return nova_dt.strftime(dt_format)

            elif 'anomes' in part_type_lower:
                dt_format = '%Y-%m' if has_dash else '%Y%m'
                dt = datetime.strptime(str_val, dt_format)
                nova_dt = dt - relativedelta(months=defasagem)
                return nova_dt.strftime(dt_format)

            elif 'ano' in part_type_lower or 'year' in part_type_lower:
                dt = datetime.strptime(str_val, '%Y')
                nova_dt = dt - relativedelta(years=defasagem)
                return nova_dt.strftime('%Y')

            # Se o usuário informou um formato customizado, usa ele para o parse e defasa como dia
            elif partition_format:
                dt = datetime.strptime(str_val, partition_format)
                nova_dt = dt - relativedelta(days=defasagem)
                return nova_dt.strftime(partition_format)

            return str_val

        except Exception as e:
            print(f"[DataUtils] Erro ao calcular defasagem de string para {partition_value}: {e}")
            return str_val