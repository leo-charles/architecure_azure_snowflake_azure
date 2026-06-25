"""
src/silver/writer.py
--------------------
Écrit un enregistrement tray parsé dans la table Iceberg silver.trays.
 
Idempotence : si tray_id existe déjà (même machine_id + candled_at_utc),
l'enregistrement est ignoré (no-op). PyIceberg 0.11.1 ne supporte pas
MERGE nativement → on filtre en mémoire avant l'append.
 
En prod, la compaction (src/maintenance/compaction.py) fusionne les petits
fichiers Parquet produits par les appends unitaires.
"""

import logging
 
import pyarrow as pa
from pyiceberg.table import Table
 
from src.config.catalog import get_catalog
from src.config.schema import get_or_create_silver_trays
 
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Point d'entrée principal
# ---------------------------------------------------------------------------

def write_tray(tray: dict) -> bool:
    """
    Écrit un enregistrement tray dans silver.trays.
 
    Retourne True si l'enregistrement a été écrit, False s'il existait déjà.
    Lève en cas d'erreur Iceberg / réseau.
    """
    catalog = get_catalog()
    table = get_or_create_silver_trays(catalog)
 
    tray_id = tray["tray_id"]
 
    if _tray_exists(table, tray_id):
        logger.info("tray_id=%s déjà présent — skip", tray_id)
        return False
 
    batch = _tray_to_arrow(tray, table.schema().as_arrow())
    table.append(batch)
    logger.info("tray_id=%s écrit dans silver.trays", tray_id)
    return True

# ---------------------------------------------------------------------------
# Idempotence : vérification existence
# ---------------------------------------------------------------------------
 
def _tray_exists(table: Table, tray_id: str) -> bool:
    """
    Vérifie si tray_id existe déjà dans la table.
    Utilise un scan filtré sur tray_id pour minimiser les données lues.
    """
    try:
        result = (
            table.scan(
                row_filter=f"tray_id == '{tray_id}'",
                selected_fields=("tray_id",),
                limit=1,
            )
            .to_arrow()
        )
        return len(result) > 0
    except Exception as e:
        # En cas d'erreur de scan (table vide, etc.), on laisse passer l'écriture
        logger.warning("Scan idempotence échoué pour tray_id=%s : %s", tray_id, e)
        return False

# ---------------------------------------------------------------------------
# Conversion dict → PyArrow RecordBatch
# ---------------------------------------------------------------------------
 
def _tray_to_arrow(tray: dict, arrow_schema: pa.Schema) -> pa.RecordBatch:
    """
    Convertit le dict tray en RecordBatch PyArrow aligné sur le schéma Silver.
 
    light_flat est une list[int] Python → pa.list_(pa.int32()).
    Les colonnes manquantes dans le dict sont remplies avec None.
    """
    row = {}
    for field in arrow_schema:
        name = field.name
        val = tray.get(name)
 
        if val is None:
            row[name] = pa.array([None], type=field.type)
            continue
 
        # Cas spécial : light_flat est une liste d'entiers
        if name == "light_flat":
            row[name] = pa.array([val], type=pa.list_(pa.int32()))
        elif pa.types.is_boolean(field.type):
            row[name] = pa.array([bool(val)], type=field.type)
        elif pa.types.is_integer(field.type):
            row[name] = pa.array([int(val)], type=field.type)
        elif pa.types.is_floating(field.type):
            row[name] = pa.array([float(val)], type=field.type)
        else:
            row[name] = pa.array([str(val)], type=field.type)
 
    return pa.RecordBatch.from_pydict(row, schema=arrow_schema)