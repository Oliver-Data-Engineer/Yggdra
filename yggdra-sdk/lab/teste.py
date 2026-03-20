import sys
import os

sys.path.append(r"C:\Users\guilh\Documents\GitHub\YGGDRA\yggdra-sdk\src")

from dev.yggdra.observability.ReportManager import ReportManager

job_args = {
    'db': 'workspace_db',
    'table_name': 'tb_visao_cliente_completa',
    'path_sql_origem': '', # Ignorado pois vamos passar a query direta
    'region_name': 'us-east-2',
    'partition_name': 'anomesdia',
    'log_level': 'INFO',
    'job_name': 'ETL_Visao_Cliente',
    'owner': 'Guilherme'}


report = ReportManager(job_args)
# ... loop de partições ...
report.add_partition_result("2023-10-01", "Success", 12.5, "abc-123-uuid")

# Para enviar por e-mail:
corpo_html = report.generate_html()

# Exemplo de salvamento local para conferência:
with open("report.html", "w", encoding="utf-8") as f:
    f.write(corpo_html)