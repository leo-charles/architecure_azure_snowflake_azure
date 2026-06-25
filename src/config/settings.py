# src/config/settings.py

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------

BRONZE_ACCOUNT_URL = os.environ.get(
    "BRONZE_ACCOUNT_URL",
    "https://stecatcandlingfrcedev.blob.core.windows.net"
)
BRONZE_CONTAINER = os.environ.get("BRONZE_CONTAINER", "pmaf-analyzed-trays")
BRONZE_ACCOUNT_KEY = os.environ.get("BRONZE_ACCOUNT_KEY", "")

# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------

SILVER_STORAGE_ACCOUNT_NAME = os.environ.get(
    "SILVER_STORAGE_ACCOUNT_NAME", "dlsecatcandlingfrcedev"
)
SILVER_STORAGE_ACCOUNT_KEY = os.environ.get("SILVER_STORAGE_ACCOUNT_KEY", "")
SILVER_CONTAINER = os.environ.get("SILVER_CONTAINER", "silver")

# ---------------------------------------------------------------------------
# Catalog Iceberg
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).parent.parent.parent
CATALOG_URI = os.environ.get("CATALOG_URI", f"sqlite:///{ROOT_DIR / 'catalog_dev.db'}")
CATALOG_WAREHOUSE = os.environ.get(
    "CATALOG_WAREHOUSE",
    f"abfs://{SILVER_CONTAINER}@{SILVER_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/"
)
SNAPSHOT_RETENTION_HOURS = int(os.environ.get("SNAPSHOT_RETENTION_HOURS", "2"))