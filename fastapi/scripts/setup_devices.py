"""
Script de setup inicial para criar devices e enviar dados históricos.
Este script configura todo o ambiente ThingsBoard com devices e telemetria.
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Adicionar diretório fastapi ao path
fastapi_dir = Path(__file__).parent.parent
sys.path.insert(0, str(fastapi_dir))

# Diretório raiz do projeto
project_root = fastapi_dir.parent

# Carregar variáveis de ambiente
# Em Docker: /app/.env, localmente: fastapi/.env
if Path("/app/.env").exists():
    env_path = Path("/app/.env")
else:
    env_path = fastapi_dir / ".env"

if env_path.exists():
    load_dotenv(env_path)

from services.thingsboard_service import ThingsBoardService
from services.device_manager_service import create_device_manager
from services.csv_processor_service import create_csv_processor
from services.graph_metadata_service import create_graph_metadata_service

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Função principal do script de setup."""
    parser = argparse.ArgumentParser(
        description="Setup inicial de devices e telemetria no ThingsBoard"
    )
    parser.add_argument(
        "--tb-url",
        default=os.getenv("THINGSBOARD_URL", "http://localhost:9090"),
        help="URL do ThingsBoard (padrão: http://localhost:9090)",
    )
    parser.add_argument(
        "--tb-username",
        default=os.getenv("TB_USERNAME", "tenant@thingsboard.org"),
        help="Usuário do ThingsBoard (padrão: tenant@thingsboard.org)",
    )
    parser.add_argument(
        "--tb-password",
        default=os.getenv("TB_PASSWORD", "tenant"),
        help="Senha do ThingsBoard",
    )
    parser.add_argument(
        "--data-dir",
        default=str(project_root / "data"),
        help="Diretório com arquivos CSV",
    )
    parser.add_argument(
        "--notebooks-dir",
        default=str(project_root / "notebooks"),
        help="Diretório com notebooks e metadados",
    )
    parser.add_argument(
        "--skip-telemetry",
        action="store_true",
        help="Apenas criar devices, sem enviar telemetria",
    )
    parser.add_argument(
        "--skip-metadata", action="store_true", help="Não incluir metadados de gráficos"
    )
    parser.add_argument(
        "--anos",
        nargs="+",
        default=None,
        help="Anos específicos para enviar telemetria (ex: 2020 2021 2022)",
    )
    parser.add_argument(
        "--estacoes",
        nargs="+",
        default=None,
        help="Estações específicas para processar (ex: PETROLINA CARUARU)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Tamanho do lote para envio de telemetria (padrão: 1000)",
    )
    parser.add_argument(
        "--max-registros",
        type=int,
        default=None,
        help="Limite máximo de registros por estação (para testes rápidos)",
    )
    parser.add_argument(
        "--recriar-devices",
        action="store_true",
        help="Deletar e recriar devices existentes",
    )

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("SETUP DE DEVICES E TELEMETRIA - ESTAÇÕES METEOROLÓGICAS INMET")
    logger.info("=" * 80)

    # Passo 1: Autenticar no ThingsBoard
    logger.info("\n[1/4] Autenticando no ThingsBoard...")
    logger.info(f"URL: {args.tb_url}")
    logger.info(f"Usuário: {args.tb_username}")

    tb_service = ThingsBoardService(tb_url=args.tb_url)

    if not tb_service.authenticate(args.tb_username, args.tb_password):
        logger.error("❌ Falha na autenticação. Verifique as credenciais.")
        return 1

    logger.info("✅ Autenticação bem-sucedida!")

    # Passo 2: Criar devices
    logger.info("\n[2/4] Criando devices para estações meteorológicas...")

    device_manager = create_device_manager(tb_service)

    # Extrair metadados se não for para pular
    metadados_estacoes = None
    if not args.skip_metadata:
        logger.info("Extraindo metadados de gráficos...")
        try:
            metadata_service = create_graph_metadata_service(args.notebooks_dir)
            metadados_todas = metadata_service.extrair_metadados_todas_estacoes()

            # Formatar para atributos
            metadados_estacoes = {}
            for estacao, metadados in metadados_todas.items():
                metadados_estacoes[estacao] = (
                    metadata_service.formatar_para_atributos_device(metadados)
                )

            logger.info(
                f"✅ Metadados extraídos para {len(metadados_estacoes)} estações"
            )
        except Exception as e:
            logger.warning(f"⚠️ Erro ao extrair metadados: {e}")
            logger.warning("Continuando sem metadados...")

    # Criar devices
    resultado_devices = device_manager.criar_todos_devices(metadados_estacoes)

    logger.info(f"\n✅ Devices criados:")
    logger.info(f"   - Sucessos: {resultado_devices['sucesso']}")
    logger.info(f"   - Falhas: {resultado_devices['falhas']}")

    if resultado_devices["falhas"] > 0:
        logger.warning("⚠️ Algumas estações falharam na criação de devices")

    # Mostrar devices criados
    logger.info("\n📋 Devices criados:")
    for device in resultado_devices["devices"]:
        logger.info(
            f"   - {device['nome']}: {device['device_id'][:8]}... (token: {device['token'][:8]}...)"
        )

    # Passo 3: Enviar telemetria (se não for para pular)
    if not args.skip_telemetry:
        logger.info("\n[3/4] Enviando dados históricos de telemetria...")

        csv_processor = create_csv_processor(args.data_dir, tb_service)

        # Filtrar estações se especificado
        devices_to_process = resultado_devices["devices"]
        if args.estacoes:
            devices_to_process = [
                d for d in devices_to_process if d["nome"] in args.estacoes
            ]
            logger.info(f"Processando apenas estações: {', '.join(args.estacoes)}")

        # Filtrar anos se especificado
        anos_processar = args.anos
        if anos_processar:
            logger.info(f"Processando anos: {', '.join(anos_processar)}")
        else:
            logger.info("Processando todos os anos disponíveis (2020-2024)")

        # Processar cada estação
        resultados_telemetria = []
        for idx, device in enumerate(devices_to_process, 1):
            logger.info(
                f"\n[{idx}/{len(devices_to_process)}] Processando estação {device['nome']}..."
            )

            # Preparar colunas de telemetria (limitar se necessário)
            colunas_telemetria = None
            if args.max_registros:
                logger.info(
                    f"   Limitando a {args.max_registros} registros por arquivo"
                )

            resultado = csv_processor.enviar_telemetria_estacao(
                nome_estacao=device["nome"],
                device_token=device["token"],
                anos=anos_processar,
                batch_size=args.batch_size,
            )

            resultados_telemetria.append(resultado)

            if resultado["registros_enviados"] > 0:
                taxa_sucesso = (
                    resultado["sucesso"] / resultado["registros_enviados"] * 100
                )
                logger.info(
                    f"   ✅ {device['nome']}: "
                    f"{resultado['sucesso']} sucessos, "
                    f"{resultado['falhas']} falhas "
                    f"({taxa_sucesso:.1f}% sucesso)"
                )
            else:
                logger.warning(
                    f"   ⚠️ {device['nome']}: Nenhum arquivo encontrado ou processado"
                )

        # Resumo geral
        logger.info("\n" + "=" * 80)
        logger.info("RESUMO DO ENVIO DE TELEMETRIA")
        logger.info("=" * 80)

        total_registros = sum(r["registros_enviados"] for r in resultados_telemetria)
        total_sucesso = sum(r["sucesso"] for r in resultados_telemetria)
        total_falhas = sum(r["falhas"] for r in resultados_telemetria)
        taxa_sucesso_geral = (
            (total_sucesso / total_registros * 100) if total_registros > 0 else 0
        )

        logger.info(f"Estações processadas: {len(resultados_telemetria)}")
        logger.info(f"Total de registros enviados: {total_registros:,}")
        logger.info(f"Sucessos: {total_sucesso:,}")
        logger.info(f"Falhas: {total_falhas:,}")
        logger.info(f"Taxa de sucesso geral: {taxa_sucesso_geral:.2f}%")

    else:
        logger.info("\n[3/4] Envio de telemetria pulado (--skip-telemetry)")

    # Passo 4: Finalização
    logger.info("\n[4/4] Finalização")
    logger.info("=" * 80)
    logger.info("✅ SETUP CONCLUÍDO COM SUCESSO!")
    logger.info("=" * 80)

    logger.info("\n📊 Próximos passos:")
    logger.info("1. Acesse o ThingsBoard em: " + args.tb_url)
    logger.info("2. Navegue até 'Devices' para ver os devices criados")
    logger.info("3. Crie dashboards usando os dados de telemetria")
    logger.info("4. Use Trendz Analytics para visualizações avançadas")

    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("\n\n⚠️ Setup interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Erro fatal: {e}", exc_info=True)
        sys.exit(1)