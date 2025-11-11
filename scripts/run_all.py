# scripts/run_all.py
import subprocess, sys

PIPELINES = [
    # BRONZE
    "etl.bronze.ingest_ecb_fx",
    "etl.bronze.ingest_ptax_usdbrl",
    "etl.bronze.ingest_coingecko_btcusd",
    "etl.bronze.ingest_stooq_indices",
    "etl.bronze.ingest_yahoo_index",
    # SILVER
    "etl.silver.normalize_fx",
    "etl.silver.normalize_crypto",
    "etl.silver.normalize_indices",
    # GOLD
    "etl.gold.build_gold",
]

def run(mod: str) -> int:
    print(f"\n>>> python -m {mod}")
    return subprocess.run([sys.executable, "-m", mod], check=True).returncode

if __name__ == "__main__":
    for m in PIPELINES:
        run(m)
    print("\nOK! Bronze → Silver → Gold finalizado.")
