import os
from dotenv import load_dotenv

load_dotenv()

START_DATE = os.getenv("START_DATE", "2025-01-01")
SQLALCHEMY_URL = os.getenv("SQLALCHEMY_URL")  # mysql+pymysql://etl:***@localhost:3306/md_catalog
TZ = os.getenv("TZ", "America/Sao_Paulo")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
COINGECKO_API_KEY_HEADER = os.getenv("COINGECKO_API_KEY_HEADER", "")
COINGECKO_API_KEY_QUERY_PARAM = os.getenv("COINGECKO_API_KEY_QUERY_PARAM", "")
