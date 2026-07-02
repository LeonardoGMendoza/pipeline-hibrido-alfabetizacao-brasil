"""
setup_aws_s3.py
===============
Script para configurar o bucket S3 no AWS Academy Learner Lab.

COMO USAR:
1. Abra o AWS Academy Learner Lab
2. Clique em "Start Lab" e aguarde ficar verde
3. Clique em "AWS Details" → "Show" → copie as credenciais
4. Cole as credenciais abaixo OU defina como variáveis de ambiente
5. Execute: python setup_aws_s3.py
"""

import os
import sys
import json

# ============================================================
# COLE SUAS CREDENCIAIS DO AWS ACADEMY LEARNER LAB AQUI
# (Renovar a cada sessão — expiram em ~4 horas)
# ============================================================
AWS_ACCESS_KEY    = os.getenv("AWS_ACCESS_KEY_ID",     "COLE_AQUI")
AWS_SECRET_KEY    = os.getenv("AWS_SECRET_ACCESS_KEY", "COLE_AQUI")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN",     "COLE_AQUI")
AWS_REGION        = "us-east-1"
BUCKET_NAME       = "payflow-risk-lake"

PASTAS = [
    "bronze/payflow_credito/",
    "bronze/payflow_nps/",
    "silver/payflow_credito/",
    "silver/payflow_nps/",
    "gold/modelo_features/",
    "gold/metricas_risco/",
    "gold/metricas_nps/",
    "gold/dashboard_resumo/",
    "raw/",
]


def main():
    print("=" * 60)
    print("  PAYFLOW RISK LAKE — Setup AWS S3")
    print("  AWS Academy Learner Lab")
    print("=" * 60)

    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError
    except ImportError:
        print("\n❌ boto3 não instalado. Execute:")
        print("   pip install boto3")
        sys.exit(1)

    # Validar credenciais
    if "COLE_AQUI" in [AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_SESSION_TOKEN]:
        print("\n⚠️  ATENÇÃO: Configure as credenciais AWS!")
        print("   Opção 1 — Editar diretamente neste arquivo")
        print("   Opção 2 — Variáveis de ambiente:")
        print("     set AWS_ACCESS_KEY_ID=ASIA...")
        print("     set AWS_SECRET_ACCESS_KEY=...")
        print("     set AWS_SESSION_TOKEN=...")
        sys.exit(1)

    # Criar cliente S3
    s3 = boto3.client(
        "s3",
        region_name           = AWS_REGION,
        aws_access_key_id     = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY,
        aws_session_token     = AWS_SESSION_TOKEN,
    )

    # ──────────────────────────────────────────
    # PASSO 1: Criar o bucket
    # ──────────────────────────────────────────
    print(f"\n📦 Criando bucket: s3://{BUCKET_NAME}/")
    try:
        if AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket              = BUCKET_NAME,
                CreateBucketConfiguration = {"LocationConstraint": AWS_REGION}
            )
        print(f"  ✅ Bucket criado com sucesso!")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"]:
            print(f"  ℹ️  Bucket já existe — continuando.")
        else:
            print(f"  ❌ Erro ao criar bucket: {e}")
            sys.exit(1)

    # ──────────────────────────────────────────
    # PASSO 2: Criar estrutura de pastas (objetos vazios)
    # ──────────────────────────────────────────
    print(f"\n📁 Criando estrutura de pastas (Medallion):")
    for pasta in PASTAS:
        key = pasta + ".keep"
        s3.put_object(
            Bucket  = BUCKET_NAME,
            Key     = key,
            Body    = b"",
            Metadata = {"criado-por": "payflow-risk-setup", "camada": pasta.split("/")[0]}
        )
        print(f"  ✅ s3://{BUCKET_NAME}/{pasta}")

    # ──────────────────────────────────────────
    # PASSO 3: Upload do dataset original
    # ──────────────────────────────────────────
    print(f"\n📤 Upload dos datasets originais para s3://{BUCKET_NAME}/raw/:")

    datasets = {
        "Base de dados Tech Challenge/desafio_nps_fase_1.csv" : "raw/desafio_nps_fase_1.csv",
    }

    for local_path, s3_key in datasets.items():
        if os.path.exists(local_path):
            print(f"  ⬆️  Enviando {local_path}...")
            s3.upload_file(local_path, BUCKET_NAME, s3_key)
            print(f"     ✅ s3://{BUCKET_NAME}/{s3_key}")
        else:
            print(f"  ⚠️  Arquivo não encontrado localmente: {local_path}")

    # ──────────────────────────────────────────
    # PASSO 4: Criar arquivo de configuração
    # ──────────────────────────────────────────
    config = {
        "projeto"      : "PayFlow Risk Model",
        "bucket"       : BUCKET_NAME,
        "regiao"       : AWS_REGION,
        "camadas"      : ["bronze", "silver", "gold"],
        "databricks"   : {
            "notebooks" : [
                "databricks/01_bronze_ingestao.py",
                "databricks/02_silver_limpeza.py",
                "databricks/03_gold_analitico.py",
            ]
        },
        "status"       : "configurado"
    }

    with open("aws_config.json", "w") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    s3.put_object(
        Bucket = BUCKET_NAME,
        Key    = "config/payflow_config.json",
        Body   = json.dumps(config, indent=2, ensure_ascii=False).encode("utf-8"),
    )

    # ──────────────────────────────────────────
    # PASSO 5: Validação — listar objetos
    # ──────────────────────────────────────────
    print(f"\n📋 Estrutura final do bucket:")
    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=BUCKET_NAME)
    prefixes  = set()
    for page in pages:
        for obj in page.get("Contents", []):
            prefix = obj["Key"].split("/")[0]
            prefixes.add(prefix)

    for p in sorted(prefixes):
        print(f"  └── s3://{BUCKET_NAME}/{p}/")

    print(f"""
╔═══════════════════════════════════════════════════════╗
║   ✅  AWS S3 CONFIGURADO COM SUCESSO!                ║
║                                                       ║
║   Bucket: s3://{BUCKET_NAME:<32}   ║
║   Região: {AWS_REGION:<44}   ║
║                                                       ║
║   Próximo passo:                                      ║
║   → Importe os notebooks no Databricks                ║
║   → Configure as credenciais AWS nos notebooks        ║
║   → Execute: 01 → 02 → 03                            ║
╚═══════════════════════════════════════════════════════╝
""")


if __name__ == "__main__":
    main()
