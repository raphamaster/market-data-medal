import pandas as pd, io
from loguru import logger
from etl.common.io import load_sources_yaml, ensure_dirs, http_get, save_df, today_tag
from etl.common.db import get_engine
from etl.common.env import START_DATE
from sqlalchemy.types import Date, String, Float

BRONZE_DIR = "data/bronze/stooq"
TABLE = "md_bronze.stooq_index_raw"

def main():
    cfg = load_sources_yaml()
    ensure_dirs(BRONZE_DIR)
    frames = []
    for it in cfg["stooq"]["symbols"]:
        name, code, url = it["name"], it["code"], it["url"]
        raw = http_get(url)
        df = pd.read_csv(io.BytesIO(raw))
        # Stooq columns: Date,Open,High,Low,Close,Volume
        df.columns = df.columns.str.strip().str.lower()
        if "date" not in df.columns:
            preview = df.head(3).to_dict(orient="records")
            logger.warning(f"Stooq sem coluna 'Date' para {code}. Bytes recebidos: {len(raw)}. Preview: {preview}")
            continue
        if df.empty:
            logger.warning(f"Stooq retornou CSV vazio para {code}. Bytes recebidos: {len(raw)}")
            continue
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df = df.dropna(subset=["date"])
        df = df[df["date"] >= pd.to_datetime(START_DATE).date()]
        df["code"] = code
        df["name"] = name
        frames.append(df[["date","code","name","open","high","low","close","volume"]])
    all_df = pd.concat(frames, ignore_index=True).sort_values(["code","date"])

    tag = today_tag()
    save_df(all_df, f"{BRONZE_DIR}/indices_{tag}.parquet")

    engine = get_engine()
    dtypes = {
        "date": Date(), "code": String(16), "name": String(64),
        "open": Float(), "high": Float(), "low": Float(), "close": Float(), "volume": Float()
    }
    all_df.to_sql(TABLE.split(".")[1], engine, schema=TABLE.split(".")[0],
                  if_exists="append", index=False, dtype=dtypes)
    logger.success(f"Inserido Bronze -> {TABLE}: {len(all_df)} linhas.")

if __name__ == "__main__":
    main()
