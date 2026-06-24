import os
from pathlib import Path
from pyiceberg.catalog.sql import SqlCatalog
from dotenv import load_dotenv

load_dotenv()

STORAGE_ACCOUNT = os.getenv("SILVER_STORAGE_ACCOUNT_NAME")
STORAGE_KEY      = os.getenv("SILVER_STORAGE_ACCOUNT_KEY")
CONTAINER        = os.getenv("SILVER_CONTAINER", "silver")

ADLS_URI = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Catalog SQLite — toujours à la racine du projet
ROOT_DIR     = Path(__file__).parent.parent
CATALOG_PATH = ROOT_DIR / "catalog_dev.db"

def get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "hatchlog_dev",
        **{
            "uri":               f"sqlite:///{CATALOG_PATH.resolve()}",
            "warehouse":         ADLS_URI,
            "adls.account-name": STORAGE_ACCOUNT,
            "adls.account-key":  STORAGE_KEY,
        }
    )