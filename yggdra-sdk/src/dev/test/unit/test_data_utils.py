import pytest
from datetime import datetime, date
from unittest.mock import patch
from ..yggdra.core.DataUtils import DataUtils

class TestDataUtils:

    # =========================================================================
    # 1. TESTES DE FORMATAÇÃO (format_partition)
    # =========================================================================
    @pytest.mark.parametrize("date_input, p_type, p_format, expected", [
        ("20231027", "data", None, "2023-10-27"),
        ("2023-10-27", "anomesdia", None, "20231027"),
        ("202310", "anomes", None, "202310"),
        ("202310", "mes", None, "10"),
        ("20231027", "ano", None, "2023"),
        # Testando a sobreposição pelo p_format
        ("20231027", "data", "%Y-%m-01", "2023-10-01"),
        (datetime(2024, 5, 15), "qualquer", "%Y/%m/%d", "2024/05/15"),
        # Fallback de erro
        ("invalido", "data", None, "invalido")
    ])
    def test_format_partition(self, date_input, p_type, p_format, expected):
        """Valida se o cast de strings e objetos datetime respeita o tipo e formato."""
        result = DataUtils.format_partition(date_input, p_type, p_format)
        assert result == expected


    # =========================================================================
    # 2. TESTES DE GERAÇÃO DE PARTIÇÕES (generate_partitions)
    # =========================================================================
    def test_generate_partitions_daily_range(self):
        """Garante que o salto diário funciona perfeitamente."""
        result = DataUtils.generate_partitions(
            p_type="data", dt_ini="20240101", dt_fim="20240103"
        )
        assert result == ["2024-01-01", "2024-01-02", "2024-01-03"]

    def test_generate_partitions_monthly_range(self):
        """Garante que o salto mensal (anomes) funciona perfeitamente."""
        result = DataUtils.generate_partitions(
            p_type="anomes", dt_ini="202311", dt_fim="202401"
        )
        assert result == ["202311", "202312", "202401"]

    def test_generate_partitions_special_first_day_format(self):
        """
        🔥 EDGE CASE: Mesmo passando p_type='data', se o p_format for '%Y-%m-01',
        ele deve dar saltos mensais (is_daily=False).
        """
        result = DataUtils.generate_partitions(
            p_type="data", 
            dt_ini="20231101", 
            dt_fim="20240101",
            p_format="%Y-%m-01"
        )
        # Deve pular os meses, não os dias!
        assert result == ["2023-11-01", "2023-12-01", "2024-01-01"]


    # =========================================================================
    # 3. TESTES DE EXPANSÃO DE VARIÁVEIS (expand_date_variables)
    # =========================================================================
    def test_expand_date_variables_success(self):
        """Valida a explosão da data em múltiplos formatos úteis para SQL."""
        result = DataUtils.expand_date_variables("2024-03-15")
        
        assert result["anomesdia"] == "20240315"
        assert result["anomes"] == "202403"
        assert result["data"] == "2024-03-15"
        assert result["year"] == "2024"
        assert result["month"] == "03"
        assert result["day"] == "15"

    def test_expand_date_variables_fallback(self):
        """Se o input não for uma data (ex: partição textual), retorna a própria string."""
        result = DataUtils.expand_date_variables("categoria_vip")
        
        assert result["year"] == "categoria_vip"
        assert result["anomes"] == "categoria_vip"


    # =========================================================================
    # 4. TESTES DE DEFASAGEM (calcular_defasagem)
    # =========================================================================
    @pytest.mark.parametrize("part_dict, defasagem, expected", [
        # Grão Diário
        ({"year": "2024", "month": "03", "day": "02"}, 1, {"year": "2024", "month": "03", "day": "01"}),
        # Grão Mensal (Vira o ano de janeiro para dezembro)
        ({"year": "2024", "month": "01"}, 1, {"year": "2023", "month": "12"}),
        # Grão Anual
        ({"year": "2024"}, 2, {"year": "2022"})
    ])
    def test_calcular_defasagem_dict(self, part_dict, defasagem, expected):
        """Garante recuo correto do tempo ao lidar com dicionários Hive Style."""
        result = DataUtils.calcular_defasagem(part_dict, defasagem=defasagem)
        assert result == expected

    @pytest.mark.parametrize("part_val, p_type, defasagem, p_format, expected", [
        # Strings Padrão Diário
        ("2024-03-02", "data", 1, None, "2024-03-01"),
        ("20240302", "anomesdia", 2, None, "20240229"), # Testa ano bissexto!
        
        # Strings Padrão Mensal e Anual
        ("202401", "anomes", 1, None, "202312"),
        ("2024", "ano", 5, None, "2019"),
        
        # 🔥 EDGE CASE: Formato Especial %Y-%m-01 (Deve recuar MESES, não dias)
        ("2024-03-01", "data", 2, "%Y-%m-01", "2024-01-01") 
    ])
    def test_calcular_defasagem_string(self, part_val, p_type, defasagem, p_format, expected):
        """Garante recuo correto usando strings e a regra especial de formatos mensais."""
        result = DataUtils.calcular_defasagem(
            partition_value=part_val,
            partition_type=p_type,
            defasagem=defasagem,
            partition_format=p_format
        )
        assert result == expected