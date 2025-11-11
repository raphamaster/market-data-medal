import io, time, requests
import pandas as pd
from loguru import logger
from datetime import datetime, timezone
from etl.common.io import load_sources_yaml, ensure_dirs, save_df, today_tag
from etl.common.db import get_engine
from etl.common.env import START_DATE
from sqlalchemy.types import Date, String, Float

BRONZE_DIR = "data/bronze/yahoo"
TABLE = "md_bronze.stooq_index_raw"  # mesmo schema da bronze de índices

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

def unix_ts(dt_str: str) -> int:
    dt = datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def build_urls(ticker_enc: str, start_yyyy_mm_dd: str):
    p1 = unix_ts(start_yyyy_mm_dd)
    p2 = int(time.time())
    # PÁGINA (sem encode e SEM barra antes de ?p=)
    hist_url_page = "https://finance.yahoo.com/quote/^BVSP/history?p=^BVSP"
    # CSV (encodado)
    dl_url = (
        f"https://query1.finance.yahoo.com/v7/finance/download/{ticker_enc}"
        f"?period1={p1}&period2={p2}&interval=1d&events=history&includeAdjustedClose=true"
    )
    # CHART JSON (encodado)
    chart_url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_enc}"
        f"?interval=1d&range=max&includePrePost=false"
    )
    return hist_url_page, dl_url, chart_url

def fetch_yahoo_csv_or_chart(ticker_enc: str, start_date: str) -> bytes:
    hist_url, dl_url, chart_url = build_urls(ticker_enc, start_date)
    with requests.Session() as s:
        # 1) tentar abrir a página (se falhar, seguimos para chart json)
        try:
            r1 = s.get(hist_url, headers={"User-Agent": UA}, timeout=60)
            r1.raise_for_status()
        except requests.HTTPError as e:
            logger.warning(f"Hist page falhou ({e}); vou tentar direto o Chart API.")

        # 2) tentar CSV
        hdrs = {"User-Agent": UA, "Accept": "text/csv,application/json", "Referer": hist_url}
        r2 = s.get(dl_url, headers=hdrs, timeout=60)
        if r2.status_code == 200 and r2.content and r2.content.startswith(b"Date,"):
            return r2.content

        # 3) fallback: Chart API (JSON) → CSV equivalente
        r3 = s.get(chart_url, headers={"User-Agent": UA, "Referer": hist_url}, timeout=60)
        r3.raise_for_status()
        js = r3.json()
        result = js["chart"]["result"][0]
        ts = result.get("timestamp", [])
        q = result["indicators"]["quote"][0]
        # usa adjclose quando existir; senão close
        adj = result.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", q.get("close"))

        df = pd.DataFrame({
            "Date": pd.to_datetime(ts, unit="s").date if ts else [],
            "Open": q.get("open"),
            "High": q.get("high"),
            "Low" : q.get("low"),
            "Close": adj,
            "Adj Close": adj,
            "Volume": q.get("volume"),
        })
        if df.empty:
            raise RuntimeError("Chart API retornou vazio.")
        df = df[df["Date"] >= pd.to_datetime(start_date).date()]

        buf = io.StringIO()
        df.to_csv(buf, index=False)
        return buf.getvalue().encode("utf-8")

def main():
    cfg = load_sources_yaml()
    ensure_dirs(BRONZE_DIR)
    eng = get_engine()

    frames = []
    for it in cfg.get("yahoo", {}).get("indices", []):
        name = it["name"]            # "IBOV"
        code = it["code"]            # "^bvsp"
        ticker_enc = it["ticker"]    # "%5EBVSP"
        logger.info(f"Yahoo GET {name} ({code})")

        raw = fetch_yahoo_csv_or_chart(ticker_enc, START_DATE)
        df = pd.read_csv(io.BytesIO(raw))  # Date,Open,High,Low,Close,Adj Close,Volume
        if df.empty:
            logger.warning(f"Yahoo vazio para {code}")
            continue

        df.rename(columns=str.lower, inplace=True)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[df["date"] >= pd.to_datetime(START_DATE).date()]
        df["code"] = code
        df["name"] = name

        out = df[["date","code","name","open","high","low","close","volume"]].copy()
        frames.append(out)

    if not frames:
        logger.error("Nenhum índice Yahoo processado.")
        return

    all_df = pd.concat(frames, ignore_index=True).sort_values(["code","date"])
    tag = today_tag()
    save_df(all_df, f"{BRONZE_DIR}/indices_{tag}.parquet")

    dtypes = {
        "date": Date(), "code": String(16), "name": String(64),
        "open": Float(), "high": Float(), "low": Float(), "close": Float(), "volume": Float()
    }
    all_df.to_sql(TABLE.split(".")[1], eng, schema=TABLE.split(".")[0],
                  if_exists="append", index=False, dtype=dtypes)
    logger.success(f"Bronze: inseridos {len(all_df)} registros em {TABLE}")

if __name__ == "__main__":
    main()
