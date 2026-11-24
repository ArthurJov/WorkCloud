 """
Módulo compartilhado: conexão com o Postgres da Aiven e leitura da API aa
de Dados Agregados do IBGE (Emprego e Preços — SIDRA/Agregados v3).
Usado por app.py — concentra a lógica de acesso a dados num único módulo.
"""

import os
import datetime
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool as psycopg2_pool
from psycopg2.extras import RealDictCursor
import requests

DB_URL = os.environ.get("AIVEN_DB_URL")

# Séries da API de Dados Agregados do IBGE (Emprego e Preços) — dados abertos, sem autenticação.
# Referência: https://servicodados.ibge.gov.br/api/docs/agregados?versao=3
SERIES = {
    63814099: {
        "name": "Taxa de desocupação (PNAD Contínua)",
        "unit": "%",
        "casas": 1,
        "agregado": 6381,
        "variavel": 4099,
    },
    17370063: {
        "name": "IPCA - variação mensal",
        "unit": "%",
        "casas": 2,
        "agregado": 1737,
        "variavel": 63,
    },
    17372265: {
        "name": "IPCA - acumulado 12 meses",
        "unit": "%",
        "casas": 2,
        "agregado": 1737,
        "variavel": 2265,
    },
}
POINTS_PER_SERIES = 30

_pool = None


def _get_pool():
    global _pool
    if _pool is None:
        if not DB_URL:
            raise RuntimeError("AIVEN_DB_URL não configurada")
        _pool = psycopg2_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=DB_URL,
            sslmode="require",
            cursor_factory=RealDictCursor,
            connect_timeout=10,
        )
    return _pool


@contextmanager
def get_connection():
    """Pega uma conexão emprestada do pool e devolve ao sair do bloco `with`."""
    conn = _get_pool().getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _get_pool().putconn(conn)


def init_db():
    """Cria as tabelas caso ainda não existam."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ibge_series (
                        series_code INTEGER NOT NULL,
                        ref_date    DATE NOT NULL,
                        value       NUMERIC NOT NULL,
                        fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                        PRIMARY KEY (series_code, ref_date)
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS visits (
                        id SERIAL PRIMARY KEY,
                        path TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )
            conn.commit()
    except psycopg2.errors.DuplicateTable:
        pass
    except psycopg2.errors.UniqueViolation as exc:
        if "pg_type" not in str(exc):
            raise


def fetch_series_from_ibge(series_code: int, n: int = POINTS_PER_SERIES):
    """Busca os últimos N períodos de uma série na API de Dados Agregados do IBGE."""
    meta = SERIES[series_code]
    agregado = meta["agregado"]
    variavel = meta["variavel"]

    url = (
        f"https://servicodados.ibge.gov.br/api/v3/agregados/{agregado}"
        f"/periodos/-{n}/variaveis/{variavel}"
    )
    params = {"localidades": "N1[all]"}
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    raw = resp.json()

    serie_dict = raw[0]["resultados"][0]["series"][0]["serie"]

    parsed = []
    for period_str, val_str in sorted(serie_dict.items()):
        if val_str in (None, "", "-", "...", ".."):
            continue
        try:
            val = float(val_str)
        except ValueError:
            continue
        year = int(period_str[:4])
        month = int(period_str[4:6])
        ref_date = datetime.date(year, month, 1)
        parsed.append({"ref_date": ref_date, "value": val})

    return parsed[-n:]


# Alias mantido para compatibilidade com chamadas existentes
fetch_series_from_bcb = fetch_series_from_ibge


def upsert_series(series_code: int, points: list):
    """Grava (ou atualiza) pontos de uma série direto no Postgres."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            for p in points:
                cur.execute(
                    """
                    INSERT INTO ibge_series (series_code, ref_date, value, fetched_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (series_code, ref_date)
                    DO UPDATE SET value = EXCLUDED.value, fetched_at = now();
                    """,
                    (series_code, p["ref_date"], p["value"]),
                )
        conn.commit()
