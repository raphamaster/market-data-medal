import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import Integer, String, Date, Float
from loguru import logger
from etl.common.db import get_engine

def upsert(engine, table_full, df: pd.DataFrame, key_cols: list[str]):
    # Simples upsert via DELETE+INSERT (safe o suficiente em lotes diários)
    with engine.begin() as conn:
        # delete existentes
        if not df.empty:
            keys = " AND ".join([f"{k} = :{k}" for k in key_cols])
            del_sql = f"DELETE FROM {table_full} WHERE {keys}"
            for _, row in df.iterrows():
                conn.execute(text(del_sql), row.to_dict())
        # insert all
        df.to_sql(table_full.split(".")[1], conn.connection, schema=table_full.split(".")[0],
                  if_exists="append", index=False)

def main():
    eng = get_engine()

    # ---------------- FX ----------------
    fx = pd.read_sql("SELECT date, pair, rate FROM md_silver.fx_rates", eng)
    fx["date"] = pd.to_datetime(fx["date"]).dt.date
    # dim_currency
    currs = sorted(set([p.split("/")[0] for p in fx["pair"]] + [p.split("/")[1] for p in fx["pair"]]))
    dim_currency = pd.DataFrame({"currency_code": currs})
    dim_currency.to_sql("dim_currency", eng, schema="md_gold", if_exists="replace", index=False,
                        dtype={"currency_code": String(8)})

    fact_fx = fx.copy()
    fact_fx.rename(columns={"pair":"currency_pair", "rate":"rate_close"}, inplace=True)
    fact_fx.to_sql("fact_fx_daily", eng, schema="md_gold", if_exists="replace", index=False,
                   dtype={"date": Date(), "currency_pair": String(16), "rate_close": Float()})

    # ---------------- CRYPTO ----------------
    cr = pd.read_sql("SELECT date, symbol, price FROM md_silver.crypto_rates", eng)
    cr["date"] = pd.to_datetime(cr["date"]).dt.date
    cr.rename(columns={"symbol":"asset_symbol","price":"price_close"}, inplace=True)
    cr.to_sql("fact_crypto_daily", eng, schema="md_gold", if_exists="replace", index=False,
              dtype={"date": Date(), "asset_symbol": String(16), "price_close": Float()})

    # ---------------- INDEX ----------------
    idx = pd.read_sql("SELECT date,index_code,index_name,open,high,low,close,volume FROM md_silver.index_ohlc", eng)
    idx["date"] = pd.to_datetime(idx["date"]).dt.date
    dim_index = idx[["index_code","index_name"]].drop_duplicates()
    dim_index.to_sql("dim_index", eng, schema="md_gold", if_exists="replace", index=False,
                     dtype={"index_code": String(16), "index_name": String(64)})

    fact_index = idx.rename(columns={"close":"close_price"})
    fact_index = fact_index[["date","index_code","open","high","low","close_price","volume"]]
    fact_index.to_sql("fact_index_daily", eng, schema="md_gold", if_exists="replace", index=False,
                      dtype={"date": Date(), "index_code": String(16),
                             "open": Float(), "high": Float(), "low": Float(),
                             "close_price": Float(), "volume": Float()})

    logger.success("GOLD atualizado: dim_currency, fact_fx_daily, fact_crypto_daily, dim_index, fact_index_daily")

if __name__ == "__main__":
    main()
