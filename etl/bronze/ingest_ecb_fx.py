import io
import json
import pandas as pd
from loguru import logger
from urllib.parse import quote
from etl.common.io import load_sources_yaml, ensure_dirs, http_get, save_df, today_tag
from etl.common.db import get_engine
from etl.common.env import START_DATE
from sqlalchemy.types import Date, String, Float

BRONZE_DIR = "data/bronze/ecb"
TABLE = "md_bronze.ecb_fx_raw"

def build_url(base_url: str, series_key: str, fmt: str = "jsondata", start: str = "2025-01-01") -> str:
    # EX: https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=jsondata&startPeriod=2025-01-01
    return f"{base_url}/{quote(series_key)}?format={fmt}&startPeriod={start}"

def normalize_json(payload: dict, code: str) -> pd.DataFrame:
    # Navegação no SDMX-json do ECB
    data_section = payload.get("data", payload)
    data_sets = data_section.get("dataSets", [])
    if not data_sets:
        raise ValueError("ECB payload sem dataSets")

    try:
        series = data_sets[0]["series"]["0:0:0:0:0"]
    except (KeyError, IndexError) as exc:
        raise KeyError("Estrutura inesperada em dataSets.series") from exc

    obs = series.get("observations", {})
    if not obs:
        return pd.DataFrame(columns=["date", "code", "rate_vs_eur"])

    rows = []
    # Alguns payloads trazem índices numéricos, outros já trazem a data como chave.
    numeric_keys = all(str(k).isdigit() for k in obs.keys())
    if numeric_keys:
        structure = data_section.get("structure", {})
        dimensions = structure.get("dimensions", {})
        observation = dimensions.get("observation", [])
        if not observation:
            raise KeyError("Estrutura sem dimensions.observation para mapear datas")
        times = observation[0]["values"]
        for k, v in obs.items():
            idx = int(k)
            date = times[idx]["id"]
            rate = float(v[0])
            rows.append({"date": date, "code": code, "rate_vs_eur": rate})
    else:
        for date, v in obs.items():
            rate = float(v[0])
            rows.append({"date": date, "code": code, "rate_vs_eur": rate})
    return pd.DataFrame(rows)

def main():
    cfg = load_sources_yaml()
    ecb = cfg["ecb"]
    ensure_dirs(BRONZE_DIR)

    frames = []
    for sym in ecb["symbols"]:
        code, key = sym["code"], sym["key"]
        if code == "EUR":
            # EUR/EUR = 1 (vamos criar sintético)
            continue
        url = build_url(ecb["base_url"], key, ecb["format"], START_DATE)
        logger.info(f"ECB GET {code}: {url}")
        raw = http_get(url)
        payload = json.loads(raw.decode("utf-8"))
        df = normalize_json(payload, code)
        frames.append(df)

    df_all = pd.concat(frames, ignore_index=True).sort_values("date")
    # adiciona EUR/EUR = 1
    eur = df_all[["date"]].drop_duplicates().assign(code="EUR", rate_vs_eur=1.0)
    df_all = pd.concat([df_all, eur], ignore_index=True)

    # Persistir arquivo Bronze
    tag = today_tag()
    save_df(df_all, f"{BRONZE_DIR}/ecb_fx_{tag}.parquet")

    # Carregar no MariaDB (tabela raw)
    engine = get_engine()
    df_all["date"] = pd.to_datetime(df_all["date"]).dt.date
    dtypes = {"date": Date(), "code": String(10), "rate_vs_eur": Float()}
    df_all.to_sql(TABLE.split(".")[1], engine, schema=TABLE.split(".")[0],
                  if_exists="append", index=False, dtype=dtypes)
    logger.success(f"Inserido Bronze -> {TABLE}: {len(df_all)} linhas.")

if __name__ == "__main__":
    main()
