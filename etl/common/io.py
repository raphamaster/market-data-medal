import os, io, yaml, pandas as pd
from loguru import logger
import requests
from datetime import datetime
from .env import START_DATE

def load_sources_yaml(path="configs/sources.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    # substitui ${START_DATE}
    raw = raw.replace("${START_DATE}", START_DATE)
    return yaml.safe_load(raw)

def ensure_dirs(*paths):
    for p in paths:
        os.makedirs(p, exist_ok=True)

def http_get(url: str, headers: dict | None = None, timeout: int = 60) -> bytes:
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.content

def save_df(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info(f"Saved: {path}")

def today_tag() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")
