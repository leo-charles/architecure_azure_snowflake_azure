"""
src/silver/function_app.py
--------------------------
Azure Function déclenchée par Event Grid sur chaque nouveau blob Bronze.
 
Trigger : Event Grid (BlobCreated sur le container pmaf-analyzed-trays)
Fonction : ProcessPmafMessage

Flow :
    Event Grid → blob URL → télécharger le blob Bronze (NDJSON)
               → parser chaque ligne JSON (1 ligne = 1 message IoT Hub)
               → router sur pmaf_trig == "Tray"
               → parser le plateau (bronze/parser.py)
               → écrire en Silver Iceberg (silver/writer.py)
 
Le blob Bronze est un fichier NDJSON IoT Hub (plusieurs messages par fichier).
Format du chemin Bronze : raw/trolley/year=YYYY/month=MM/day=DD/<fichier>.json
"""

import json
import logging
 
import azure.functions as func
from azure.storage.blob import BlobServiceClient
 
from src.bronze.parser import parse_iot_message
from src.config.settings import BRONZE_ACCOUNT_KEY, BRONZE_ACCOUNT_URL, BRONZE_CONTAINER
from src.silver.writer import write_tray
 
logger = logging.getLogger(__name__)
 
app = func.FunctionApp()


# ---------------------------------------------------------------------------
# Fonction principale
# ---------------------------------------------------------------------------
 
@app.event_grid_trigger(arg_name="event")
def ProcessPmafMessage(event: func.EventGridEvent) -> None:
    """
    Déclenchée par chaque BlobCreated dans le container Bronze.
 
    L'Event Grid passe l'URL du blob dans event.get_json()["url"].
    On télécharge le blob, on parse chaque ligne NDJSON, on route et on écrit.
    """
    logger.info("Event reçu : %s", event.event_type)
 
    event_data = event.get_json()
    blob_url = event_data.get("url", "")
 
    if not blob_url:
        logger.error("Pas d'URL de blob dans l'event — abandon")
        return
 
    logger.info("Traitement du blob : %s", blob_url)
 
    blob_name = _extract_blob_name(blob_url, BRONZE_CONTAINER)
    if blob_name is None:
        logger.warning("Impossible d'extraire le nom du blob depuis %s", blob_url)
        return
 
    ndjson_content = _download_blob(blob_name)
    if ndjson_content is None:
        return
 
    written = 0
    skipped = 0
    errors = 0
 
    for line_num, line in enumerate(ndjson_content.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
 
        try:
            raw_message = json.loads(line)
        except json.JSONDecodeError as e:
            logger.warning("Ligne %d — JSON invalide : %s", line_num, e)
            errors += 1
            continue
 
        try:
            tray = parse_iot_message(raw_message)
        except ValueError as e:
            logger.warning("Ligne %d — parse échoué : %s", line_num, e)
            errors += 1
            continue
 
        if tray is None:
            skipped += 1
            continue
 
        try:
            was_written = write_tray(tray)
            if was_written:
                written += 1
            else:
                skipped += 1
        except Exception as e:
            logger.error("Ligne %d — écriture Silver échouée : %s", line_num, e)
            errors += 1
 
    logger.info(
        "Blob %s traité — écrits: %d, skippés: %d, erreurs: %d",
        blob_name, written, skipped, errors
    )
 
    if errors > 0:
        logger.error(
            "%d erreurs sur le blob %s — vérifier les logs Application Insights",
            errors, blob_name
        )
 
 
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
 
def _extract_blob_name(blob_url: str, container: str) -> str | None:
    """
    Extrait le chemin du blob depuis son URL complète.
    Ex : https://stecatcandlingfrcedev.blob.core.windows.net/pmaf-analyzed-trays/raw/trolley/...
    → raw/trolley/...
    """
    marker = f"/{container}/"
    idx = blob_url.find(marker)
    if idx == -1:
        return None
    return blob_url[idx + len(marker):]
 
 
def _download_blob(blob_name: str) -> str | None:
    """
    Télécharge le contenu d'un blob Bronze en texte UTF-8.
    Clé de compte en dev, Managed Identity en prod (BRONZE_ACCOUNT_KEY vide).
    """
    try:
        if BRONZE_ACCOUNT_KEY:
            client = BlobServiceClient(
                account_url=BRONZE_ACCOUNT_URL,
                credential=BRONZE_ACCOUNT_KEY,
            )
        else:
            from azure.identity import DefaultAzureCredential
            client = BlobServiceClient(
                account_url=BRONZE_ACCOUNT_URL,
                credential=DefaultAzureCredential(),
            )
 
        blob_client = client.get_blob_client(
            container=BRONZE_CONTAINER,
            blob=blob_name,
        )
        return blob_client.download_blob().readall().decode("utf-8", errors="replace")
 
    except Exception as e:
        logger.error("Téléchargement du blob %s échoué : %s", blob_name, e)
        return None