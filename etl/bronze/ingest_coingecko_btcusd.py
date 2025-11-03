import pandas as pd, io, json
from loguru import logger
from etl.common.io import load_sources_yaml, ensure_dirs, http_get, save_df, today_tag
from etl.common.db import get_engine
from etl.common.env import (
    START_DATE,
    COINGECKO_API_KEY,
    COINGECKO_API_KEY_HEADER,
    COINGECKO_API_KEY_QUERY_PARAM,
)
from sqlalchemy.types import Date, Float
from requests import HTTPError
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

BRONZE_DIR = "data/bronze/coingecko"
TABLE = "md_bronze.coingecko_btcusd_raw"

def main():
    cfg = load_sources_yaml()
    cg = cfg["coingecko"]
    api_key = cg.get("api_key") or COINGECKO_API_KEY
    api_key_header = (cg.get("api_key_header") or COINGECKO_API_KEY_HEADER or "").strip()
    api_key_query_param = (cg.get("api_key_query_param") or COINGECKO_API_KEY_QUERY_PARAM or "").strip()
    days_cfg = str(cg.get("days", "max"))
    start_dt = pd.to_datetime(START_DATE).date()
    today = pd.Timestamp.utcnow().date()
    diff_days = (today - start_dt).days
    if diff_days < 0:
        diff_days = 0
    use_demo_key = False
    if api_key:
        if api_key_header and "x-cg-demo" in api_key_header.lower():
            use_demo_key = True
        elif api_key_query_param and "x_cg_demo" in api_key_query_param.lower():
            use_demo_key = True
        elif not api_key_header and not api_key_query_param and api_key.upper().startswith("CG-"):
            use_demo_key = True

    days_param = days_cfg
    effective_start_date = start_dt
    if days_cfg.lower() == "max" and use_demo_key:
        days_window = diff_days + 1
        if days_window > 365:
            logger.warning("CoinGecko demo key limita a busca a 365 dias. Ajustando parâmetro 'days' para 365 e truncando período inicial.")
            days_window = 365
        if days_window <= 0:
            days_window = 1
        days_param = str(days_window)
    if days_param.isdigit():
        window_days = int(days_param)
        if window_days > 0:
            limit_start = (pd.Timestamp(today) - pd.Timedelta(days=window_days - 1)).date()
            effective_start_date = max(start_dt, limit_start)
    url = (f'{cg["base_url"]}/coins/{cg["coin_id"]}/market_chart'
           f'?vs_currency={cg["vs_currency"]}&days={days_param}')
    headers = {}
    def append_query_param(url_in: str, param: str, value: str) -> str:
        parsed = urlparse(url_in)
        query = dict(parse_qsl(parsed.query))
        query[param] = value
        new_query = urlencode(query)
        return urlunparse(parsed._replace(query=new_query))

    if api_key:
        if api_key_query_param:
            url = append_query_param(url, api_key_query_param, api_key)
        elif api_key_header:
            headers[api_key_header] = api_key
        else:
            # Heurística: chave demo (CG-) usa query param; caso contrário, header Pro.
            if api_key.upper().startswith("CG-"):
                url = append_query_param(url, "x_cg_demo_api_key", api_key)
                use_demo_key = True
            else:
                headers["x-cg-pro-api-key"] = api_key
    ensure_dirs(BRONZE_DIR)
    log_url = url.replace(api_key, "***") if api_key else url
    logger.info(f"CoinGecko GET: {log_url}")
    if headers:
        logger.debug(f"CoinGecko headers usados: {list(headers.keys())}")
    logger.debug(f"CoinGecko days param: {days_param}; filtro inicial: {effective_start_date}")
    try:
        raw = http_get(url, headers=headers or None)
    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        body = exc.response.text if exc.response is not None else ""
        if status == 401:
            logger.error(f"CoinGecko retornou 401 Unauthorized. Msg: {body}")
            logger.error("Verifique se a chave está correta e se o cabeçalho corresponde ao plano (demo -> x-cg-demo-api-key, pro -> x-cg-pro-api-key).")
        elif status == 400:
            logger.error(f"CoinGecko retornou 400 Bad Request. Corpo: {body}")
            logger.error("Se estiver usando chave gratuita, informe api_key_header: x-cg-demo-api-key em configs/sources.yaml ou variável COINGECKO_API_KEY_HEADER.")
        elif status == 403:
            logger.error(f"CoinGecko retornou 403 Forbidden. Corpo: {body}")
        raise
    payload = json.loads(raw.decode("utf-8"))
    # prices: [ [ts_ms, price], ... ]
    rows = []
    for ts_ms, price in payload.get("prices", []):
        d = pd.to_datetime(ts_ms, unit="ms").date()
        rows.append({"date": d, "btc_usd": float(price)})
    df = pd.DataFrame(rows)
    df = df[df["date"] >= effective_start_date].groupby("date", as_index=False).mean()

    tag = today_tag()
    save_df(df, f"{BRONZE_DIR}/btc_usd_{tag}.parquet")

    engine = get_engine()
    df.to_sql(TABLE.split(".")[1], engine, schema=TABLE.split(".")[0],
              if_exists="append", index=False,
              dtype={"date": Date(), "btc_usd": Float()})
    logger.success(f"Inserido Bronze -> {TABLE}: {len(df)} linhas.")

if __name__ == "__main__":
    main()
