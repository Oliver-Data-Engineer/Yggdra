import sys
import json
from pathlib import Path

# 1. 🛠️ TRUQUE DE ENGENHARIA: Adiciona o SDK ao PATH local
current_dir = Path(__file__).resolve().parent
sdk_path = current_dir.parent / "yggdra-sdk" / "src" / "dev"
if str(sdk_path) not in sys.path:
    sys.path.append(str(sdk_path))

# 2. 📦 Imports do nosso motor
from yggdra.products.DataFactory import DataFactory
from yggdra.products.Heimdall import Heimdall
from yggdra.aws.GlueManager import GlueManager

def main():
    print("🌳 Iniciando Motor Yggdra...\n")

    # 3. O Dicionário de Configuração (O "Coração" do seu pipeline)
    args = {
        'db': 'workspace_db',
        'table_name': 'tb_visao_cliente_heimdall_all_2',
        'path_sql_origem': '', # Ignorado pois vamos passar a query direta
        'region_name': 'us-east-2',
        'dt_ini': '20240303',
        'dt_fim': '20240306',
        'partition_name': 'anomesdia',
        'partition_type': 'anomesdia', # Alterado para 'data' para bater com o formato de tempo
        'defasagem': 1, # Exigindo D-1
        'reprocessamento': False,
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
                    )
                    select * from base
            '''
    }

    # =========================================================================
    # 4. 🧭 ROTEAMENTO LÓGICO (Verifica a existência da tabela)
    # =========================================================================
    glue = GlueManager(region_name=args['region_name'])
    tabela_existe = glue.table_exists(db=args['db'], table=args['table_name'])

    if tabela_existe:
        print(f"🔄 Tabela '{args['table_name']}' já existe. Iniciando Pre-Flight Checks (Heimdall)...")
        try:
            (
                Heimdall(job_args=args)
                # 1. Valida a saúde sintática do SQL antes de rodar qualquer coisa
                .check_query_health()
                
                # 2. Avalia a prontidão de todas as origens (D-1/M-0) mapeadas na query
                .evaluate_upstream_readiness()
                
                # 3. Gera e salva o relatório visual de segurança no S3
                .save_report()
                
                # 4. Autoriza ou levanta a exceção bloqueando o fluxo
                .authorize()
            )
            print("🟢 Heimdall: Portões Abertos! Pipeline autorizado para processamento incremental.")
            
        except PermissionError as e:
            print("\n🛑 " + "="*56)
            print("PIPELINE BLOQUEADO POR SEGURANÇA (HEIMDALL)")
            print("="*60)
            print(f"Motivo: {e}")
            print("Consulte o relatório HTML gerado no S3 para mais detalhes.")
            print("="*60 + "\n")
            sys.exit(1) # Aborta a execução do script com código de erro
            
        except Exception as e:
            print(f"\n❌ Erro crítico inesperado durante as validações de segurança: {e}")
            sys.exit(1)
            
    else:
        print(f"🏗️ Tabela '{args['table_name']}' NÃO existe. Modo FIRST LOAD ativado. Ignorando Heimdall.")


    # =========================================================================
    # 5. 🏭 EXECUÇÃO DA FÁBRICA DE DADOS (Após aprovação ou no First Load)
    # =========================================================================
    print(f"\n🚀 Acionando DataFactory para a tabela {args['table_name']}...")
    
    factory = DataFactory(job_args=args)
    execution_summary = factory.run()

    # =========================================================================
    # 6. CONSUMINDO O RESULTADO (Tomada de Decisão e Alertas)
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
        
    elif status in ['ERROR', 'CRITICAL_FAILURE']:
        print(f"❌ FALHA NO PIPELINE! Status: {status}")
        print(f"🚨 Analise o erro acessando o relatório HTML:")
        print(f"➡️ {execution_summary.get('report_s3_path')}")
        sys.exit(1) # Retorna erro para a ferramenta de orquestração (ex: Airflow)
        
    elif status == 'SKIPPED':
        print(f"⏭️ PIPELINE PULADO. Nenhuma partição precisou ser gerada. Status: {status}")
        
    else:
        print(f"⚠️ Pipeline com status desconhecido: {status}")

if __name__ == "__main__":
    main()