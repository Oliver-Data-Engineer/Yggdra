import sys
import json
from pathlib import Path

# 1. 🛠️ TRUQUE DE ENGENHARIA: Adiciona o SDK ao PATH local
current_dir = Path(__file__).resolve().parent
sdk_path = current_dir.parent / "yggdra-sdk" / "src" / "dev"
sys.path.append(str(sdk_path))

# 2. 📦 Import do nosso motor
# (Ajuste o caminho exato do import dependendo de onde a DataFactory foi salva)
from yggdra.products.DataFactory import DataFactory 

def main():
    print("Iniciando Motor Yggdra...\n")


    # 3. O Dicionário de Configuração (O "Coração" do seu pipeline)
    args = {
        'db': 'workspace_db',
        'table_name': 'tb_visao_cliente_completa_join',
        'path_sql_origem': '', # Ignorado pois vamos passar a query direta
        'region_name': 'us-east-2',
        'dt_ini': '20260306',
        'dt_fim': '20260306',
        'partition_name': 'anomesdia',
        'partition_type': 'anomesdia',
        'log_level': 'INFO',
        'owner': 'Guilherme@itau-unibanco.com.br',

        'query': '''
            with base as (
                    SELECT 
                        p.pedido_id,
                        p.cliente_id,
                        p.valor_total,
                        f.status_pagamento,
                        l.transportadora,
                        l.status_entrega,
                        c.pagina_acessada,
                        p.anomesdia
                    FROM workspace_db.tb_pedidos p
                    LEFT JOIN workspace_db.tb_faturamento_cliente f 
                        ON p.cliente_id = f.cliente_id
                    LEFT JOIN workspace_db.tb_logistica l 
                        ON p.pedido_id = l.pedido_id
                    LEFT JOIN workspace_db.tb_clickstream c 
                        ON p.cliente_id = c.cliente_id
                    WHERE 
                        -- Forçando o partition pruning em todas as tabelas
                        cast(p.anomesdia as varchar) = cast('{anomesdia}' as varchar)
                        AND cast(f.anomes as varchar) = cast('{anomes}' as varchar)
                    )
                    select * from base
            '''
    }

    # 4. Instancia e Roda a Fábrica de Dados
    factory = DataFactory(job_args=args)
    execution_summary = factory.run()

    # =========================================================================
    # 5. CONSUMINDO O RESULTADO (Tomada de Decisão)
    # =========================================================================
    
    print("\n" + "="*60)
    print("🎯 PAYLOAD DE RETORNO DO DATA FACTORY")
    print("="*60)
    print(json.dumps(execution_summary, indent=4, ensure_ascii=False))
    print("="*60 + "\n")

    # Lógica de negócio baseada no status retornado
    status = execution_summary.get('status')
    
    if status == 'SUCCESS':
        print(f"🚀 SUCESSO! Dados atualizados no Data Lake.")
        print(f"📂 Caminho dos Dados: {execution_summary.get('data_s3_path')}")
        print(f"📊 Relatório Gerencial: {execution_summary.get('report_s3_path')}")
        
        # Exemplo: Aqui você chamaria uma função send_success_email(execution_summary)
        
    elif status in ['ERROR', 'CRITICAL_FAILURE']:
        print(f"❌ FALHA NO PIPELINE! Status: {status}")
        print(f"🚨 Analise o erro acessando o relatório HTML:")
        print(f"➡️ {execution_summary.get('report_s3_path')}")
        
        # Exemplo: Aqui você chamaria um alerta para o PagerDuty/Slack
    else:
        print(f"⚠️ Pipeline pulado ou com status desconhecido: {status}")

if __name__ == "__main__":
    main()