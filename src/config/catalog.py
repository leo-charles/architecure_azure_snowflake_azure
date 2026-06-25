from pyiceberg.catalog.sql import SqlCatalog
from src.config.settings import (
    CATALOG_URI,
    CATALOG_WAREHOUSE,
    SILVER_STORAGE_ACCOUNT_NAME,
    SILVER_STORAGE_ACCOUNT_KEY,
)

def get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "candling_dev",
        **{
            "uri":               CATALOG_URI,
            "warehouse":         CATALOG_WAREHOUSE,
            "adls.account-name": SILVER_STORAGE_ACCOUNT_NAME,
            "adls.account-key":  SILVER_STORAGE_ACCOUNT_KEY,
        }
    )