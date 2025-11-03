import subprocess, sys

PIPELINES = [
    "etl/bronze/ingest_ecb_fx.py",
    "etl/bronze/ingest_ptax_usdbrl.py",
    "etl/bronze/ingest_coingecko_btcusd.py",
    "etl/bronze/ingest_stooq_indices.py",
    "etl/silver/normalize_fx.py",
    "etl/silver/normalize_crypto.py",
    "etl/silver/normalize_indices.py",
    "etl/gold/build_gold.py",
]

def to_module(path: str) -> list[str]:
    if path.endswith(".py"):
        mod = path[:-3].replace("/", ".")
        return [sys.executable, "-m", mod]
    return [sys.executable, path]

def run(cmd):
    print(f"\n>>> {cmd}")
    r = subprocess.run(to_module(cmd), check=True)
    return r.returncode

if __name__ == "__main__":
    for c in PIPELINES:
        run(c)
    print("\nOK! Bronze → Silver → Gold finalizado.")
