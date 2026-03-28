from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Union, Any, Optional
import re

class DataUtils:
    """
    Classe utilitária para orquestração de tempo e partições.
    Suporta defasagem (D-x), reprocessamento condicional, múltiplos formatos
    e partições compostas nativas [year, month, day].
    """

    @staticmethod
    def format_partition(date_ref: Any, p_type: str, p_format: Optional[str] = None) -> str:
        """Fábrica de formatos de data. Prioriza p_format se existir."""
        if isinstance(date_ref, (int, str)):
            str_date = str(date_ref).strip()
            try:
                if "-" in str_date:
                    dt = datetime.strptime(str_date[:10], "%Y-%m-%d")
                # 💡 NOVO: Suporte a strings separadas por barras (Padrão de Partição Composta)
                elif "/" in str_date:
                    if len(str_date) == 10:
                        dt = datetime.strptime(str_date, "%Y/%m/%d")
                    elif len(str_date) == 7:
                        dt = datetime.strptime(str_date, "%Y/%m")
                    else:
                        dt = datetime.strptime(re.sub(r'\D', '', str_date)[:8], "%Y%m%d")
                elif len(str_date) == 6:
                    dt = datetime.strptime(str_date, "%Y%m")
                else:
                    dt = datetime.strptime(str_date[:8], "%Y%m%d")
            except ValueError:
                return str(date_ref)
        else:
            dt = date_ref

        # Se o usuário passou um formato explícito (ex: '%Y/%m/%d'), usa ele
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
        """Calcula a lista base de datas considerando defasagem, formato e reprocessamento."""
        hoje = datetime.combine(date.today(), datetime.min.time())
        hoje_dia = date.today().day
        
        is_daily = p_type.lower() in ["data", "anomesdia"]
        if p_format == "%Y-%m-01":
            is_daily = False

        step = timedelta(days=1) if is_daily else relativedelta(months=1)

        if int(dt_ini) == 190001 and int(dt_fim) == 190001:
            anchor = hoje if is_daily else hoje.replace(day=1)
            dt_fim_dt = anchor - (step * int(defasagem))
            dt_ini_dt = dt_fim_dt 

            if reprocessamento:
                deve_reprocessar = (dia_corte is None) or (hoje_dia == dia_corte)
                if deve_reprocessar:
                    dt_ini_dt = dt_fim_dt - (step * int(range_reprocessamento))
        else:
            str_ini, str_fim = str(dt_ini), str(dt_fim)
            fmt_ini = "%Y%m%d" if len(str_ini) > 6 else "%Y%m"
            fmt_fim = "%Y%m%d" if len(str_fim) > 6 else "%Y%m"
            dt_ini_dt = datetime.strptime(str_ini, fmt_ini)
            dt_fim_dt = datetime.strptime(str_fim, fmt_fim)

        if dt_fim_dt < dt_ini_dt:
            return []

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
    def expand_date_variables(partition_value: Union[str, list, dict]) -> dict:
        """
        Gera um dicionário com variações de datas para interpolação SQL.
        Suporta nativamente listas ['2026', '03', '06'] e converte sem erros.
        """
        # 💡 NOVO: Se receber lista (AWS Glue), normaliza para string unida
        if isinstance(partition_value, list):
            partition_value = "/".join([str(x) for x in partition_value])
            
        clean_val = re.sub(r'\D', '', str(partition_value))
        
        try:
            if len(clean_val) >= 8:    # YYYYMMDD
                dt = datetime.strptime(clean_val[:8], '%Y%m%d')
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
                "day": dt.strftime('%d'),
                "path_date": dt.strftime('%Y/%m/%d') if len(clean_val) >= 8 else dt.strftime('%Y/%m') # 💡 NOVO
            }
            
        except Exception:
            return {
                "anomesdia": partition_value, "anomes": partition_value, 
                "data": partition_value, "year": partition_value, 
                "month": partition_value, "day": partition_value, "path_date": partition_value
            }
    
    @staticmethod
    def calcular_defasagem(
        partition_value: Union[str, list, dict], 
        partition_type: str = "", 
        defasagem: int = 0,
        partition_format: Optional[str] = None
    ) -> Union[str, dict]:
        """Calcula a partição de defasagem com inteligência de grão (Dias vs Meses)."""
        if defasagem == 0:
            return partition_value
            
        # 💡 NOVO: Normaliza lista ['2026', '03', '06'] para '2026/03/06'
        if isinstance(partition_value, list):
            partition_value = "/".join([str(x) for x in partition_value])

        # 1. LÓGICA PARA DICIONÁRIOS MÚLTIPLOS
        if isinstance(partition_value, dict):
            # ... (Lógica de dict intocada, já estava perfeita) ...
            keys = {k.lower(): k for k in partition_value.keys()}
            try:
                if 'day' in keys and 'month' in keys and 'year' in keys:
                    dt = datetime(int(partition_value[keys['year']]), int(partition_value[keys['month']]), int(partition_value[keys['day']]))
                    nova_dt = dt - relativedelta(days=defasagem)
                    return {keys['year']: nova_dt.strftime('%Y'), keys['month']: nova_dt.strftime('%m'), keys['day']: nova_dt.strftime('%d')}
                elif 'month' in keys and 'year' in keys:
                    dt = datetime(int(partition_value[keys['year']]), int(partition_value[keys['month']]), 1)
                    nova_dt = dt - relativedelta(months=defasagem)
                    return {keys['year']: nova_dt.strftime('%Y'), keys['month']: nova_dt.strftime('%m')}
                elif 'year' in keys:
                    dt = datetime(int(partition_value[keys['year']]), 1, 1)
                    nova_dt = dt - relativedelta(years=defasagem)
                    return {keys['year']: nova_dt.strftime('%Y')}
            except Exception:
                return partition_value

        # 2. LÓGICA PARA STRINGS (COM OU SEM BARRAS)
        str_val = str(partition_value)
        part_type_lower = partition_type.lower()
        has_dash = '-' in str_val

        try:
            # 💡 NOVO: Inteligência em cima do Formato Customizado
            if partition_format:
                dt = datetime.strptime(str_val, partition_format)
                if '%d' in partition_format: # Se o formato exige dia, defasa dias!
                    nova_dt = dt - relativedelta(days=defasagem)
                elif '%m' in partition_format: # Se exige só mês, defasa meses!
                    nova_dt = dt - relativedelta(months=defasagem)
                else:
                    nova_dt = dt - relativedelta(years=defasagem)
                return nova_dt.strftime(partition_format)

            # 💡 NOVO: Regra Nativa para Strings separadas por '/' (Partições Compostas Glue)
            elif '/' in str_val:
                if len(str_val) == 10: # YYYY/MM/DD
                    dt = datetime.strptime(str_val, '%Y/%m/%d')
                    return (dt - relativedelta(days=defasagem)).strftime('%Y/%m/%d')
                elif len(str_val) == 7: # YYYY/MM
                    dt = datetime.strptime(str_val, '%Y/%m')
                    return (dt - relativedelta(months=defasagem)).strftime('%Y/%m')

            # Regras Antigas...
            elif 'anomesdia' in part_type_lower or 'data' in part_type_lower:
                dt_format = '%Y-%m-%d' if has_dash else '%Y%m%d'
                dt = datetime.strptime(str_val, dt_format)
                return (dt - relativedelta(days=defasagem)).strftime(dt_format)

            elif 'anomes' in part_type_lower:
                dt_format = '%Y-%m' if has_dash else '%Y%m'
                dt = datetime.strptime(str_val, dt_format)
                return (dt - relativedelta(months=defasagem)).strftime(dt_format)

            elif 'ano' in part_type_lower or 'year' in part_type_lower:
                dt = datetime.strptime(str_val, '%Y')
                return (dt - relativedelta(years=defasagem)).strftime('%Y')

            return str_val

        except Exception as e:
            print(f"[DataUtils] Erro ao calcular defasagem de string para {partition_value}: {e}")
            return str_val