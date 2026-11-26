"""
Extract & Load: Neon (PostgreSQL) -> BigQuery

Sekwencyjne ładowanie wszystkich tabel sklepu z grami na konsole
w kolejności wynikającej z relacji PK -> FK.
Uruchamiane przez GitHub Actions (.github/workflows/load.yml).
"""

import os
import psycopg2
import pandas as pd
from google.cloud import bigquery

# ============================================================
# KONFIGURACJA
# ============================================================

NEON_CONNECTION_STRING = os.environ["NEON_CONNECTION_STRING"]
BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_DATASET = os.environ["BQ_DATASET"]
BQ_LOCATION = os.environ.get("BQ_LOCATION", "EU")

# Kolejność ładowania uwzględniająca relacje PK -> FK:
# 1. Tabele niezależne (bez kluczy obcych): publishers, platforms, customers
# 2. Tabele zależne od poziomu 1: games (publishers), orders (customers)
# 3. Tabele zależne od poziomu 2: game_editions (games, platforms)
# 4. Tabele zależne od poziomu 3: order_items (orders, game_editions)
TABLES_IN_ORDER = [
    "publishers",
    "platforms",
    "customers",
    "games",
    "game_editions",
    "orders",
    "order_items",
]

INCREMENTAL_COLUMN = "updated_at"

# "full_refresh" -> nadpisuje całą tabelę w BigQuery (WRITE_TRUNCATE)
# "incremental"  -> ładuje tylko wiersze z updated_at > max(updated_at) w BigQuery (WRITE_APPEND)
LOAD_MODE = os.environ.get("LOAD_MODE", "incremental")

# ============================================================
# LOGIKA EXTRACT & LOAD
# ============================================================


def get_last_loaded_timestamp(bq_client: bigquery.Client, table_ref: str):
    """Odczytuje maksymalny timestamp już obecny w tabeli docelowej BigQuery."""
    query = f"SELECT MAX({INCREMENTAL_COLUMN}) AS max_ts FROM `{table_ref}`"
    try:
        result = bq_client.query(query, location=BQ_LOCATION).result()
        row = next(iter(result))
        return row.max_ts
    except Exception:
        return None


def extract_from_neon(conn, table_name: str, mode: str, last_ts=None) -> pd.DataFrame:
    """Wyciąga dane z pojedynczej tabeli w bazie Neon."""
    if mode == "incremental" and last_ts is not None:
        query = f"SELECT * FROM {table_name} WHERE {INCREMENTAL_COLUMN} > %s ORDER BY {INCREMENTAL_COLUMN} ASC"
        return pd.read_sql(query, conn, params=(last_ts,))
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, conn)


def load_to_bigquery(bq_client: bigquery.Client, df: pd.DataFrame, table_ref: str, mode: str):
    """Ładuje DataFrame do BigQuery."""
    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if mode == "full_refresh"
        else bigquery.WriteDisposition.WRITE_APPEND
    )
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = bq_client.load_table_from_dataframe(
        df, table_ref, job_config=job_config, location=BQ_LOCATION
    )
    job.result()
    return job


def main():
    print(f"Start pipeline EL | tryb: {LOAD_MODE} | cel: {BQ_PROJECT_ID}.{BQ_DATASET} ({BQ_LOCATION})")
    bq_client = bigquery.Client(project=BQ_PROJECT_ID, location=BQ_LOCATION)
    conn = psycopg2.connect(NEON_CONNECTION_STRING)

    try:
        for table_name in TABLES_IN_ORDER:
            table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"
            print(f"\n--- Tabela: {table_name} -> {table_ref} ---")

            last_ts = None
            if LOAD_MODE == "incremental":
                last_ts = get_last_loaded_timestamp(bq_client, table_ref)
                print(f"Ostatni załadowany timestamp w BigQuery: {last_ts}")

            df = extract_from_neon(conn, table_name, LOAD_MODE, last_ts)
            print(f"Wyciągnięto {len(df)} wierszy z Neon (tabela źródłowa: {table_name})")

            if df.empty:
                print(f"Brak nowych danych dla tabeli {table_name} -- pomijam.")
                continue

            job = load_to_bigquery(bq_client, df, table_ref, LOAD_MODE)
            print(f"Załadowano {job.output_rows} wierszy do {table_ref} (tryb: {LOAD_MODE})")
    finally:
        conn.close()

    print("\nPipeline EL zakończony pomyślnie.")


if __name__ == "__main__":
    main()
