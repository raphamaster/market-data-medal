import pandas as pd
from sqlalchemy.types import Date, String, Float, BigInteger
from loguru import logger
from etl.common.db import get_engine

OUT_TABLE = "md_silver.index_ohlc"

def main():
    eng = get_engine()
    q = """
      SELECT date, code, name, open, high, low, close, volume
      FROM md_bronze.stooq_index_raw
    """
    df = pd.read_sql(q, eng)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df.rename(columns={"code":"index_code","name":"index_name"}, inplace=True)

    df.to_sql(OUT_TABLE.split(".")[1], eng, schema=OUT_TABLE.split(".")[0],
              if_exists="append", index=False,
              dtype={
                "date": Date(), "index_code": String(16), "index_name": String(64),
                "open": Float(), "high": Float(), "low": Float(), "close": Float(),
                "volume": Float()
              })
    logger.success(f"SILVER INDEX -> {OUT_TABLE}: {len(df)} linhas.")

if __name__ == "__main__":
    main()
