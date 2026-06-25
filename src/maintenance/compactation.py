"""
src/maintenance/compaction.py
------------------------------
Compaction de la table silver.trays.
 
Stratégie validée en NB08 :
    1. Pour chaque partition (machine_id, year, month, day) à compacter,
       on lit toutes les données et on les réécrit en un seul fichier Parquet
       via table.overwrite() filtré sur la partition.
    2. On expire les anciens snapshots pour libérer l'espace ADLS.
 
Résultat observé en test : 180 fichiers (2.69 Mo) → 1 fichier (0.55 Mo).
"""

import logging
from datetime import datetime, timedelta, timezone
 
from pyiceberg.maintenance import MaintenanceTable
from pyiceberg.table import Table
 
from src.config.catalog import get_catalog
from src.config.schema import get_or_create_silver_trays
from src.config.settings import SNAPSHOT_RETENTION_HOURS
 
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# API publique
# ---------------------------------------------------------------------------
 
def compact_partition(machine_id: str, year: int, month: int, day: int) -> dict:
    """
    Compacte une partition (machine_id, year, month, day) en un seul fichier.
    Retourne {"status": "ok"|"empty"|"error", ...}.
    """
    table = _load_table()
    partition_filter = (
        f"machine_id == '{machine_id}' "
        f"AND year == {year} "
        f"AND month == {month} "
        f"AND day == {day}"
    )
 
    logger.info(
        "Compaction démarrée : machine=%s date=%04d-%02d-%02d",
        machine_id, year, month, day,
    )
 
    try:
        scan_result = table.scan(row_filter=partition_filter).to_arrow()
    except Exception as e:
        logger.error("Scan partition échoué : %s", e)
        return {"status": "error", "error": str(e)}
 
    if len(scan_result) == 0:
        logger.info("Partition vide — rien à compacter")
        return {"status": "empty"}
 
    logger.info("Données lues : %d lignes — réécriture en cours", len(scan_result))
 
    try:
        table.overwrite(df=scan_result, overwrite_filter=partition_filter)
    except Exception as e:
        logger.error("Overwrite partition échoué : %s", e)
        return {"status": "error", "error": str(e)}
 
    _expire_old_snapshots(table)
 
    logger.info(
        "Compaction terminée : machine=%s date=%04d-%02d-%02d — %d lignes",
        machine_id, year, month, day, len(scan_result),
    )
    return {"status": "ok", "rows": len(scan_result)}
 
 
def compact_day(year: int, month: int, day: int) -> dict:
    """
    Compacte toutes les partitions d'une journée (toutes machines).
    Utilisé par le Timer trigger de fin de journée.
    """
    table = _load_table()
    day_filter = f"year == {year} AND month == {month} AND day == {day}"
 
    try:
        machines_result = (
            table.scan(row_filter=day_filter, selected_fields=("machine_id",))
            .to_arrow()
        )
    except Exception as e:
        logger.error("Scan des machines du jour échoué : %s", e)
        return {"status": "error", "error": str(e)}
 
    if len(machines_result) == 0:
        logger.info("Aucune donnée pour %04d-%02d-%02d", year, month, day)
        return {"status": "empty"}
 
    machine_ids = machines_result["machine_id"].unique().to_pylist()
    logger.info(
        "Compaction journée %04d-%02d-%02d : %d machine(s)",
        year, month, day, len(machine_ids),
    )
 
    results = {
        mid: compact_partition(mid, year, month, day)
        for mid in machine_ids
    }
    return {"status": "ok", "machines": results}
 
 
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
 
def _load_table() -> Table:
    catalog = get_catalog()
    return get_or_create_silver_trays(catalog)
 
 
def _expire_old_snapshots(table: Table) -> None:
    expire_before = datetime.now(timezone.utc) - timedelta(hours=SNAPSHOT_RETENTION_HOURS)
    try:
        (
            MaintenanceTable(table)
            .expire_snapshots()
            .older_than(expire_before)
            .commit()
        )
        logger.info("Snapshots expirés avant %s", expire_before.isoformat())
    except Exception as e:
        logger.warning("Expiration des snapshots échouée (non bloquant) : %s", e)