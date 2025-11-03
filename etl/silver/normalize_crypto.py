import pandas as pd
from sqlalchemy.types import Date, String, Float
from loguru import logger
from etl.common.db import get_engine

OUT_TABLE = "md_silver.crypto_rates"

def main():
    eng = get_engine()
    btc = pd.read_sql("SELECT date, btc_usd FROM md_bronze.coingecko_btcusd_raw", eng)
    ptax = pd.read_sql("SELECT date, usdbrl FROM md_bronze.ptax_usdbrl_raw", eng)
    for df in (btc, ptax):
        df["date"] = pd.to_datetime(df["date"]).dt.date

    df = btc.merge(ptax, on="date", how="left")
    df["btc_brl"] = df["btc_usd"] * df["usdbrl"]

    out = pd.concat([
        df[["date"]].assign(symbol="BTC/USD", price=df["btc_usd"]),
        df[["date"]].assign(symbol="BTC/BRL", price=df["btc_brl"])
    ], ignore_index=True).dropna()

    out.to_sql(OUT_TABLE.split(".")[1], eng, schema=OUT_TABLE.split(".")[0],
               if_exists="append", index=False,
               dtype={"date": Date(), "symbol": String(16), "price": Float()})
    logger.success(f"SILVER CRYPTO -> {OUT_TABLE}: {len(out)} linhas.")

if __name__ == "__main__":
    main()
