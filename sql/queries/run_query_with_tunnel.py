#!/usr/bin/env python
"""Ejecuta la consulta SQL `sql/flora_fauna_query.sql` via SSH tunnel.

Conecta a la base de datos remota a través del túnel SSH y ejecuta la consulta,
muestra un resumen por pantalla y guarda el resultado como Parquet comprimido.

Uso:
    uv run -m src.queries.run_query_with_tunnel
"""

# %%
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from sql.db_conns_with_tunnel import get_tunnel_engine

# Add the db/sea_crawler_tunnel directory to path to import the tunnel module
sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "db" / "sea_crawler_tunnel")
)

load_dotenv(override=True)


def read_sql() -> str:
    """Devuelve el contenido de sql/flora_fauna_query.sql."""
    sql_path = (
        Path(__file__).resolve().parents[2]  # -> proyecto raíz
        / "sql"
        / "queries"
        / "flora_fauna_query.sql"
    )
    return sql_path.read_text(encoding="utf-8")


def main():
    """Ejecuta la consulta usando conexión SSH tunnel."""
    query = read_sql()

    print("🔐 Estableciendo túnel SSH...")
    tunnel, engine = get_tunnel_engine()

    try:
        print("📊 Ejecutando consulta...")
        # Usar la conexión directamente para evitar problemas con pandas/SQLAlchemy
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            columns = result.keys()

        # Convertir a DataFrame
        df = pd.DataFrame(rows, columns=columns)

        print(f"\n📋 Resultados: {len(df)} filas")
        print("\nPrimeras 5 filas:")
        print(df.head())

        # Mostrar info de columnas
        print(f"\n📊 Columnas ({len(df.columns)}):")
        for col in df.columns:
            print(f"  - {col}: {df[col].dtype}")

        # Guardar como Parquet
        output_file = (
            Path(__file__).resolve().parents[1]
            / "metadata_table"
            / "flora_fauna_metadata.parquet"
        )
        df.to_parquet(output_file, index=False, compression="gzip")
        print(f"\n✅ Resultado Parquet (comprimido) guardado en {output_file}")
        print(f"   Tamaño del archivo: {output_file.stat().st_size / 1024:.2f} KB")

    finally:
        # Siempre cerrar el túnel y el engine
        engine.dispose()
        tunnel.stop()
        print("\n🔒 Túnel SSH cerrado")


if __name__ == "__main__":
    main()

# %%
