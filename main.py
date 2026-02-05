import os
import psycopg2
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import date

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=5432,
        sslmode="require"
    )

@app.get("/movimentacoes")
def movimentacoes(
    data_inicio: date = Query(...),
    data_fim: date = Query(...)
):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            data,
            semana,
            mes,
            produto,
            setor,
            quantidade,
            valor,
            tipo
        FROM vw_movimentacoes
        WHERE data BETWEEN %s AND %s
        ORDER BY data
    """, (data_inicio, data_fim))

    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    cur.close()
    conn.close()

    return [
        dict(zip(columns, row))
        for row in rows
    ]
