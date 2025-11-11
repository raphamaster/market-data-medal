import os, io, yaml, pandas as pd
from loguru import logger
import requests
from datetime import datetime
from .env import START_DATE

def load_sources_yaml(path="configs/sources.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    os.environ.setdefault("START_DATE", START_DATE)
    raw = os.path.expandvars(raw)
    return yaml.safe_load(raw)

def ensure_dirs(*paths):
    for p in paths:
        os.makedirs(p, exist_ok=True)

DEFAULT_UA = "Mozilla/5.0 (X11; Linux x86_64) MarketDataETL/1.0"

def http_get(url: str, headers: dict | None = None, timeout: int = 60) -> bytes:
    # Alguns provedores (ex: Stooq) bloqueiam User-Agent padrão do requests.
    merged_headers = {"User-Agent": DEFAULT_UA}
    if headers:
        merged_headers.update(headers)
    r = requests.get(url, headers=merged_headers, timeout=timeout)
    r.raise_for_status()
    return r.content

def save_df(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    logger.info(f"Saved: {path}")

def today_tag() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")
