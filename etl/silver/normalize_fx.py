import pandas as pd
from sqlalchemy import text
from loguru import logger
from etl.common.db import get_engine
from sqlalchemy.types import Date, String, Float

OUT_TABLE = "md_silver.fx_rates"

def main():
    eng = get_engine()
    # ECB
    q_ecb = """
        SELECT date, code, rate_vs_eur
        FROM md_bronze.ecb_fx_raw
    """
    ecb = pd.read_sql(q_ecb, eng)
    ecb["date"] = pd.to_datetime(ecb["date"]).dt.date
    ecb = ecb.sort_values("date").drop_duplicates(subset=["date", "code"], keep="last")

    # PTAX USD/BRL
    q_ptax = "SELECT date, usdbrl FROM md_bronze.ptax_usdbrl_raw"
    ptax = pd.read_sql(q_ptax, eng)
    ptax["date"] = pd.to_datetime(ptax["date"]).dt.date
    ptax = ptax.sort_values("date").drop_duplicates(subset=["date"], keep="last")

    # junta
    base = ecb.pivot(index="date", columns="code", values="rate_vs_eur").reset_index()
    # Colunas possíveis: USD, BRL, GBP, EUR(=1)
    # Deriva EUR se não vier (garantia)
    if "EUR" not in base.columns:
        base["EUR"] = 1.0

    # Merge com PTAX (USD/BRL)
    df = base.merge(ptax, on="date", how="left")

    # Deriva BRL/EUR se faltou (a partir de USD/EUR e USD/BRL)
    # rate_vs_eur significa: 1 CODE = X EUR (na prática ECB é quoted vs EUR, então invertido)
    # ECB "D.USD.EUR.SP00.A": 1 USD in EUR. Logo USD/EUR.
    # Queremos CODE/BRL. Fórmulas:
    #   CODE/BRL = (CODE/EUR) / (BRL/EUR)
    # Mas do PTAX temos USD/BRL. E do ECB temos USD/EUR.
    # Podemos obter BRL/EUR = (BRL/USD) * (USD/EUR) = (1 / USD/BRL) * (USD/EUR)
    df["brl_per_eur"] = (1.0 / df["usdbrl"]) * df["USD"]  # BRL/EUR
    # Para cada moeda CODE:
    rates = []
    for code in [c for c in df.columns if c not in ("date","usdbrl","brl_per_eur")]:
        # CODE/EUR já está como df[code]
        # CODE/BRL = (CODE/EUR) / (BRL/EUR)
        rates.append(
            df[["date", code, "brl_per_eur"]]
              .assign(pair=lambda x: x.columns[1]+"/BRL",
                      rate=lambda x: x.iloc[:,1] / x["brl_per_eur"])
              .rename(columns={"date":"date"})
              [["date","pair","rate"]]
        )
    out = pd.concat(rates, ignore_index=True)

    # Persistir Silver
    out = out.dropna().sort_values(["pair","date"])
    out.to_sql(OUT_TABLE.split(".")[1], eng, schema=OUT_TABLE.split(".")[0],
               if_exists="append", index=False,
               dtype={"date": Date(), "pair": String(16), "rate": Float()})
    logger.success(f"SILVER FX -> {OUT_TABLE}: {len(out)} linhas.")

if __name__ == "__main__":
    main()
