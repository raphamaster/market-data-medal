import io
import json
from urllib.parse import urlencode

import pandas as pd
from loguru import logger
from etl.common.io import load_sources_yaml, ensure_dirs, http_get, save_df, today_tag
from etl.common.db import get_engine
from etl.common.env import START_DATE
from sqlalchemy.types import Date, String, Float

BRONZE_DIR = "data/bronze/stooq"
TABLE = "md_bronze.stooq_index_raw"

def _cutoff_date():
    return pd.to_datetime(START_DATE).date()

def _fetch_stooq(it: dict) -> pd.DataFrame | None:
    name, code, url = it["name"], it["code"], it["url"]
    raw = http_get(url)
    if len(raw) < 32:
        logger.warning(f"Stooq retornou payload muito pequeno para {code}: {raw!r}")
        return None
    df = pd.read_csv(io.BytesIO(raw))
    if df.shape[1] == 1 and ";" in df.columns[0]:
        logger.debug(f"Detecção de CSV delimitado por ';' para {code}")
        df = pd.read_csv(io.BytesIO(raw), sep=";")
    # Stooq columns: Date,Open,High,Low,Close,Volume
    df.columns = df.columns.str.replace("\ufeff", "").str.strip().str.lower()
    if "date" not in df.columns:
        preview = df.head(3).to_dict(orient="records")
        logger.warning(f"Stooq sem coluna 'Date' para {code}. Bytes recebidos: {len(raw)}. Preview: {preview}")
        return None
    if df.empty:
        logger.warning(f"Stooq retornou CSV vazio para {code}. Bytes recebidos: {len(raw)}")
        return None
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    df = df[df["date"] >= _cutoff_date()]
    df["code"] = code
    df["name"] = name
    return df[["date","code","name","open","high","low","close","volume"]]

def _fetch_alphavantage(symbol_cfg: dict, alpha_cfg: dict, api_key: str) -> pd.DataFrame | None:
    base_url = alpha_cfg.get("base_url", "https://www.alphavantage.co/query")
    function = symbol_cfg.get("function", alpha_cfg.get("function", "TIME_SERIES_DAILY_ADJUSTED"))
    symbol = symbol_cfg.get("symbol", symbol_cfg["code"])
    params = {
        "function": function,
        "symbol": symbol,
        "apikey": api_key,
        "outputsize": symbol_cfg.get("outputsize", "full"),
    }
    url = f"{base_url}?{urlencode(params)}"
    raw = http_get(url)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.error(f"Resposta inválida (JSON) do Alpha Vantage para {symbol_cfg['code']}: {raw[:120]!r}")
        return None
    if "Note" in data:
        logger.warning(f"Alpha Vantage rate limit atingido para {symbol_cfg['code']}: {data['Note']}")
        return None
    if "Error Message" in data:
        logger.error(f"Alpha Vantage erro para {symbol_cfg['code']}: {data['Error Message']}")
        return None
    series_key = next((k for k in data.keys() if "Time Series" in k), None)
    if not series_key:
        logger.warning(f"Alpha Vantage sem séries temporais para {symbol_cfg['code']}: chaves={list(data.keys())}")
        return None
    series = data[series_key]
    if not series:
        logger.warning(f"Alpha Vantage retornou série vazia para {symbol_cfg['code']}")
        return None
    df = pd.DataFrame.from_dict(series, orient="index")
    df.index.name = "date"
    df.reset_index(inplace=True)
    rename_map = {
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. adjusted close": "close_adjusted",
        "6. volume": "volume",
    }
    df.rename(columns=rename_map, inplace=True)
    if "close" not in df.columns and "close_adjusted" in df.columns:
        df["close"] = df["close_adjusted"]
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = pd.NA
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date", "close"])
    df = df[df["date"] >= _cutoff_date()]
    if df.empty:
        logger.warning(f"Alpha Vantage sem dados após {START_DATE} para {symbol_cfg['code']}")
        return None
    df["code"] = symbol_cfg["code"]
    df["name"] = symbol_cfg["name"]
    return df[["date","code","name","open","high","low","close","volume"]]

def main():
    cfg = load_sources_yaml()
    ensure_dirs(BRONZE_DIR)
    frames = []
    stooq_cfg = cfg.get("stooq", {})
    for it in stooq_cfg.get("symbols", []):
        try:
            df = _fetch_stooq(it)
        except Exception as exc:  # defensive log for parsing issues
            logger.exception(f"Falha ao processar {it.get('code')} do Stooq: {exc}")
            df = None
        if df is not None and not df.empty:
            frames.append(df)

    alpha_cfg = cfg.get("alphavantage")
    if alpha_cfg:
        api_key = alpha_cfg.get("api_key", "")
        if not api_key:
            logger.error("alphavantage.api_key não definido (verifique ALPHAVANTAGE_API_KEY no .env)")
        else:
            for it in alpha_cfg.get("symbols", []):
                try:
                    df = _fetch_alphavantage(it, alpha_cfg, api_key)
                except Exception as exc:
                    logger.exception(f"Falha ao processar {it.get('code')} do Alpha Vantage: {exc}")
                    df = None
                if df is not None and not df.empty:
                    frames.append(df)

    if not frames:
        raise RuntimeError("Nenhum dado de índices foi coletado (Stooq/Alpha Vantage).")

    all_df = pd.concat(frames, ignore_index=True).sort_values(["code","date"])
    all_df = all_df.drop_duplicates(subset=["code","date"], keep="first")

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
