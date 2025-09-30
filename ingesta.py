import os
import sys
import time
import csv
import re
from datetime import datetime
from typing import List, Optional
import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "")
PG_USER = os.getenv("PG_USER", "")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")

TABLES_ENV = os.getenv("TABLES", "")
TABLES: List[str] = [t.strip() for t in TABLES_ENV.split(",") if t.strip()]

CSV_SEP = os.getenv("CSV_SEP", ",")
CSV_QUOTE = os.getenv("CSV_QUOTE", "MINIMAL").upper()
CSV_LINE_TERMINATOR = os.getenv("CSV_LINE_TERMINATOR", "\n")
CHUNKSIZE = int(os.getenv("CHUNKSIZE", "100000"))
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/out")

S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION", "us-east-1")

TIMESTAMP = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def csv_quote_const(name: str):
    import csv as _csv
    return {
        "MINIMAL": _csv.QUOTE_MINIMAL,
        "ALL": _csv.QUOTE_ALL,
        "NONNUMERIC": _csv.QUOTE_NONNUMERIC,
        "NONE": _csv.QUOTE_NONE,
    }.get(name, _csv.QUOTE_MINIMAL)


def ensure_output_dir(path: str):
    os.makedirs(path, exist_ok=True)


def get_engine():
    uri = f"postgresql+psycopg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(
        uri,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={"options": f"-c search_path={PG_SCHEMA}"},
    )


VALID_IDENT = re.compile(r"^[A-Za-z0-9_]+$")

def safe_ident(name: str) -> str:
    """Evita inyección en identificadores: permite solo a-zA-Z0-9_."""
    if not VALID_IDENT.match(name):
        raise ValueError(f"Nombre inválido: {name!r}")
    return name


def table_exists(engine, table_name: str) -> bool:
    q = text("""
        SELECT COUNT(*) AS c
        FROM information_schema.tables
        WHERE table_schema = :schema AND table_name = :tbl
    """)
    with engine.connect() as conn:
        r = conn.execute(q, {"schema": PG_SCHEMA, "tbl": table_name}).scalar()
        return (r or 0) > 0


def export_table_to_csv(engine, table_name: str, out_dir: str) -> str:
    """Stream de la tabla a CSV por chunks."""
    table = safe_ident(table_name)
    schema = safe_ident(PG_SCHEMA)

    filename = f"{table}_{TIMESTAMP}.csv"
    out_path = os.path.join(out_dir, filename)

    quote = csv_quote_const(CSV_QUOTE)
    header_written = False
    row_count = 0

    query = f'SELECT * FROM "{schema}"."{table}"'
    for chunk in pd.read_sql(query, engine, chunksize=CHUNKSIZE):
        chunk.to_csv(
            out_path,
            mode="a",
            index=False,
            sep=CSV_SEP,
            quoting=quote,
            lineterminator=CSV_LINE_TERMINATOR,  
            header=not header_written,
        )
        row_count += len(chunk)
        header_written = True

    print(f"[OK] {table} -> {out_path} ({row_count} filas)")
    return out_path


def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def upload_to_s3(local_path: str, bucket: str, prefix: Optional[str]) -> str:
    key = os.path.basename(local_path)
    if prefix:
        key = f"{prefix.rstrip('/')}/{key}"

    cli = s3_client()
    try:
        cli.upload_file(local_path, bucket, key)
    except (NoCredentialsError, PartialCredentialsError):
        print("[ERROR] Credenciales de AWS no encontradas o incompletas.", file=sys.stderr)
        raise
    except ClientError as e:
        print(f"[ERROR] Fallo subiendo a S3: {e}", file=sys.stderr)
        raise

    print(f"[OK] Subido a s3://{bucket}/{key}")
    return key


def main():
    if not PG_DB or not PG_USER or not PG_PASSWORD or not TABLES:
        print(
            "Faltan variables de entorno obligatorias: PG_DB, PG_USER, PG_PASSWORD y TABLES.",
            file=sys.stderr,
        )
        sys.exit(1)

    if not S3_BUCKET:
        print("Falta S3_BUCKET.", file=sys.stderr)
        sys.exit(1)

    ensure_output_dir(OUTPUT_DIR)
    engine = get_engine()

    exported_files = []
    for tbl in TABLES:
        if not table_exists(engine, tbl):
            print(f"[WARN] La tabla '{tbl}' no existe en {PG_DB}.{PG_SCHEMA}. Se omite.")
            continue
        path = export_table_to_csv(engine, tbl, OUTPUT_DIR)
        exported_files.append(path)

    if not exported_files:
        print("[INFO] No se exportó ninguna tabla. Revisa nombres y permisos.", file=sys.stderr)
        sys.exit(2)

    for path in exported_files:
        upload_to_s3(path, S3_BUCKET, S3_PREFIX)

    print("[DONE] Ingesta completada.")


if __name__ == "__main__":
    start = time.time()
    try:
        main()
    finally:
        dur = time.time() - start
        print(f"Duración total: {dur:.1f}s")
