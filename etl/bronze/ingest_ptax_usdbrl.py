import pandas as pd
from loguru import logger
from etl.common.io import load_sources_yaml, ensure_dirs, http_get, save_df, today_tag
from etl.common.db import get_engine
from etl.common.env import START_DATE
from sqlalchemy.types import Date, Float

BRONZE_DIR = "data/bronze/bacen"
TABLE = "md_bronze.ptax_usdbrl_raw"

def build_url(serie: int) -> str:
    # https://api.bcb.gov.br/dados/serie/bcdata.sgs.10813/dados?dataInicial=01/01/2025&dataFinal=31/12/2099&formato=json
    return (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"
            f"?dataInicial=01/01/2025&dataFinal=31/12/2099&formato=json")

def main():
    cfg = load_sources_yaml()
    serie = cfg["bacen_ptax"]["serie_usdbrl"]
    ensure_dirs(BRONZE_DIR)
    url = build_url(serie)
    logger.info(f"BACEN GET PTAX USD/BRL: {url}")
    raw = http_get(url)
    df = pd.read_json(io.BytesIO(raw))
    df.rename(columns={"data":"date","valor":"usdbrl"}, inplace=True)
    # Datas vêm em dd/mm/yyyy
    df["date"] = pd.to_datetime(df["date"], dayfirst=True).dt.date
    df = df[df["date"] >= pd.to_datetime(START_DATE).date()].sort_values("date")

    tag = today_tag()
    save_df(df, f"{BRONZE_DIR}/ptax_{tag}.parquet")

    engine = get_engine()
    df.to_sql(TABLE.split(".")[1], engine, schema=TABLE.split(".")[0],
              if_exists="append", index=False,
              dtype={"date": Date(), "usdbrl": Float()})
    logger.success(f"Inserido Bronze -> {TABLE}: {len(df)} linhas.")

if __name__ == "__main__":
    import io
    main()
