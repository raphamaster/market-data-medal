from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from loguru import logger
from .env import SQLALCHEMY_URL

def get_engine() -> Engine:
    if not SQLALCHEMY_URL:
        raise RuntimeError("SQLALCHEMY_URL não definido no .env")
    engine = create_engine(SQLALCHEMY_URL, pool_pre_ping=True, pool_recycle=1800)
    return engine

def exec_sql(engine: Engine, sql: str, params: dict | None = None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})
        logger.debug("SQL executado.")
