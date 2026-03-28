
import sys
import json
from pathlib import Path

# 1. 🛠️ TRUQUE DE ENGENHARIA: Adiciona o SDK ao PATH local
current_dir = Path(__file__).resolve().parent
sdk_path = current_dir.parent / "yggdra-sdk" / "src" / "dev"
if str(sdk_path) not in sys.path:
    sys.path.append(str(sdk_path))


from yggdra.aws.AthenaManager import AthenaManager

athena = AthenaManager()

response = athena.client.get_query_results(
                QueryExecutionId='79e8796f-19c7-45f4-9e9c-3942376f9b38',
                MaxResults=5 # Limitamos a 5 porque só queremos a primeira linha mesmo, economiza rede!
            )
            
            # 3. Navega no labirinto do JSON do Boto3
rows = response.get('ResultSet', {}).get('Rows', [])

print(rows)
print(type(rows))
print(len(rows) > 1)

data_columns = rows[1].get('Data', [])
if data_columns:
    # Pega o 'VarCharValue' da Coluna 0 (índice 0)
    scalar_value = data_columns[0].get('VarCharValue', '0')
    print(scalar_value)
    print(type(scalar_value))

qtd_linhas = int(scalar_value) if scalar_value else 0

print(qtd_linhas)
print(type(qtd_linhas))

#