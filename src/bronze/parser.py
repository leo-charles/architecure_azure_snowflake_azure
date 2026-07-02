"""
src/bronze/parser.py
--------------------
Parse un message NDJSON IoT Hub (1 ligne = 1 message) issu du container Bronze.
 
Format attendu (envelope IoT Hub, body base64 → JSON) :
    {
        "Body": "<base64>",
        "EnqueuedTimeUtc": "2026-05-15T09:24:00Z",
        "SystemProperties": { "connectionDeviceId": "PMAF-C012501" },
        "Properties": { "pmaf_trig": "Tray" }
    }
 
Body décodé :
    {
        "process_serial_number": "PMAF-C012501",
        "box": "LaserLife",
        "values": [
            {"id": "LaserLife.PMAF-C012501.flock_number", "value": 10, ...},
            {"id": "LaserLife.PMAF-C012501.final_candled_eggs[1][1]", "value": 1, ...}
        ]
    }
 
Règles métier :
  - Seul pmaf_trig == "Tray" est traité (les autres sont ignorés silencieusement).
  - Le timestamp autoritatif est iothub-enqueuedtime (pas l'horloge machine).
  - Tous les comptages sont recomputed depuis la matrice 15×10 (jamais copiés des tags).
  - tray_id = SHA-256(machine_id | candled_at_utc) pour l'idempotence.
"""

import base64
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Encodage des classes d'œufs (commun à tous les types de plateau)
# ---------------------------------------------------------------------------

CLASS_MISSING = 0
CLASS_FERTILE = 1
CLASS_EARLY_DEAD = 2
CLASS_CLEAR = 3
CLASS_LATE_DEAD = 4

PMAF_TRIG_TRAY = "Tray"

# ---------------------------------------------------------------------------
# Chargement des référentiels JSON
#
# tray_types.json     : setter_tray_type → {rows, cols, total}
# machine_registry.json : machine_id     → {couvoir, setter_tray_type, ...}
#
# Pour ajouter un nouveau type de plateau : compléter tray_types.json.
# Pour enregistrer une nouvelle machine : compléter machine_registry.json.
# ---------------------------------------------------------------------------

_CONFIG_DIR = Path(__file__).parent.parent / "config"

with open(_CONFIG_DIR / "tray_types.json", encoding="utf-8") as _f:
    _TRAY_TYPES: dict = {k: v for k, v in json.load(_f).items() if not k.startswith("_")}

with open(_CONFIG_DIR / "machine_registry.json", encoding="utf-8") as _f:
    _MACHINE_REGISTRY: dict = {k: v for k, v in json.load(_f).items() if not k.startswith("_")}

_DEFAULT_ROWS, _DEFAULT_COLS = 15, 10  # fallback Petersime_150


def get_tray_config(machine_id: str) -> tuple[int, int]:
    """
    Retourne (rows, cols) pour une machine donnée.

    Lookup : machine_id → setter_tray_type → (rows, cols).
    Logue un avertissement si la machine ou le type de plateau est inconnu
    ou si les dimensions ne sont pas encore renseignées dans tray_types.json.
    """
    machine = _MACHINE_REGISTRY.get(machine_id)
    if machine is None:
        logger.warning(
            "Machine '%s' absente du registre — dimensions par défaut utilisées (%dx%d). "
            "Ajoutez-la dans src/config/machine_registry.json.",
            machine_id, _DEFAULT_ROWS, _DEFAULT_COLS,
        )
        return _DEFAULT_ROWS, _DEFAULT_COLS

    tray_type = machine["setter_tray_type"]
    tray_cfg = _TRAY_TYPES.get(tray_type, {})
    rows, cols = tray_cfg.get("rows"), tray_cfg.get("cols")

    if rows is None or cols is None:
        logger.warning(
            "Dimensions du plateau '%s' non renseignées dans tray_types.json "
            "— dimensions par défaut utilisées (%dx%d). À compléter.",
            tray_type, _DEFAULT_ROWS, _DEFAULT_COLS,
        )
        return _DEFAULT_ROWS, _DEFAULT_COLS

    return rows, cols


# Alias rétrocompatibles (utiles dans les tests et notebooks existants)
MATRIX_ROWS, MATRIX_COLS = _DEFAULT_ROWS, _DEFAULT_COLS
TOTAL_EGGS = MATRIX_ROWS * MATRIX_COLS

# ---------------------------------------------------------------------------
# Point d'entrée principal
# ---------------------------------------------------------------------------

def parse_iot_message(raw_message: dict) -> Optional[dict]:
    """
    Parse un message IoT Hub déjà désérialisé (dict Python).
 
    Retourne un dict tray normalisé prêt pour silver/writer.py,
    ou None si le message ne concerne pas un plateau (pmaf_trig != "Tray").
 
    Lève ValueError si le message est malformé.
    """
    props = raw_message.get("Properties") or raw_message.get("properties") or {}
    trig = props.get("pmaf_trig", "")

    if trig != PMAF_TRIG_TRAY:
        logger.debug("pmaf_trig=%s — ignoré", trig)
        return None
    
    machine_id = _extract_machine_id(raw_message)
    candled_at_utc = _extract_timestamp(raw_message)
    tags = _decode_body(raw_message)

    rows, cols = get_tray_config(machine_id)
    matrix = _build_matrix(tags, rows, cols)

    tray_id = _compute_tray_id(machine_id, candled_at_utc)

    counts = _recompute_counts(matrix)
    matrix_compact = _matrix_to_compact(matrix)
    light_flat = _extract_light_flat(tags, rows, cols)

    return {
        # Identité
        "tray_id":                  tray_id,
        "machine_id":               machine_id,
        "candled_at_utc":           candled_at_utc.isoformat(),
 
        # --- Colonnes de partition (dérivées, entières) ---
        "year":                     candled_at_utc.year,
        "month":                    candled_at_utc.month,
        "day":                      candled_at_utc.day,
 
        # --- Matrice ---
        "matrix_compact":           matrix_compact,   # str 150 chars
        "light_flat":               light_flat,        # list[int] 150 valeurs
 
        # --- Comptages recomputed ---
        "fertile_count":            counts["fertile"],
        "clear_count":              counts["clear"],
        "early_dead_count":         counts["early_dead"],
        "late_dead_count":          counts["late_dead"],
        "missing_count":            counts["missing"],
        # --- Métadonnées lot / trolley ---
        "flock_number":             _tag_int(tags, "flock_number"),
        "flock_name":               _tag_str(tags, "flock_name"),
        "trolley_name":             _tag_str(tags, "trolley_name"),
        "setter_tray_number_flock": _tag_int(tags, "setter_tray_number_flock"),
        "setter_tray_length":       _tag_int(tags, "setter_tray_length"),
        "space_with_previous_tray": _tag_int(tags, "space_with_previous_tray"),
        "caliber":                  _tag_int(tags, "caliber"),
 
        # --- Alarmes ---
        "alarm_emergency_stop":     bool(_tag_int(tags, "alarm_emergency_stop")),
        "alarm_air_pressure_fault": bool(_tag_int(tags, "alarm_air_pressure_fault")),
        "alarm_common":             bool(_tag_int(tags, "alarm_common")),
    }

# ---------------------------------------------------------------------------
# Extraction des champs IoT Hub
# ---------------------------------------------------------------------------

def _extract_machine_id(msg: dict) -> str:
    # IoT Hub expose le device ID dans SystemProperties
    sys_props = msg.get("SystemProperties") or msg.get("systemProperties") or {}
    mid = sys_props.get("connectionDeviceId") or sys_props.get("iothub-connection-device-id", "")
    if mid:
        return mid
    # Fallback : process_serial_number dans le body décodé
    raise ValueError("machine_id introuvable dans SystemProperties")

def _extract_timestamp(msg: dict) -> datetime:
    """
    Timestamp autoritatif = EnqueuedTimeUtc (IoT Hub).
    L'horloge interne de la machine (hour/minute/second dans les tags)
    peut être invalide — on ne l'utilise pas.
    """
    # IoT Hub écrit "EnqueuedTimeUtc" avec cette casse exacte
    raw_ts = msg.get("EnqueuedTimeUtc") or msg.get("enqueuedTimeUtc", "")
    if not raw_ts:
        raise ValueError("Champ 'EnqueuedTimeUtc' manquant dans le message IoT")
    try:
        dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except ValueError as e:
        raise ValueError(f"Timestamp invalide '{raw_ts}' : {e}") from e

# ---------------------------------------------------------------------------
# Décodage du body (JSON base64 IoT Hub)
# ---------------------------------------------------------------------------
 
def _decode_body(msg: dict) -> dict:
    """
    Décode le body base64 → JSON IoT Hub → dict plat {tag_name: value}.
 
    Structure du body décodé :
        {
            "process_serial_number": "PMAF-C012501",
            "box": "LaserLife",
            "values": [
                {"id": "LaserLife.PMAF-C012501.flock_number", "value": 10, ...},
                {"id": "LaserLife.PMAF-C012501.final_candled_eggs[1][1]", "value": 1, ...},
                ...
            ]
        }
 
    On extrait la partie finale de l'id (après le dernier ".") comme nom de tag.
    Ex : "LaserLife.PMAF-C012501.final_candled_eggs[1][1]" → "final_candled_eggs[1][1]"
    """
    body_b64 = msg.get("Body") or msg.get("body", "")
    if not body_b64:
        raise ValueError("Champ 'Body' manquant ou vide dans le message IoT")
 
    try:
        raw_bytes = base64.b64decode(body_b64)
        body = json.loads(raw_bytes.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"Décodage du body échoué : {e}") from e
 
    tags = {}
    for entry in body.get("values", []):
        tag_id = entry.get("id", "")
        # Le nom du tag est la partie après le dernier "."
        tag_name = tag_id.rsplit(".", 1)[-1] if "." in tag_id else tag_id
        tags[tag_name] = entry.get("value")

    return tags

# ---------------------------------------------------------------------------
# Construction de la matrice 15×10
# ---------------------------------------------------------------------------

def _build_matrix(tags: dict, rows: int = MATRIX_ROWS, cols: int = MATRIX_COLS) -> list[list[int]]:
    """
    Reconstruit la matrice rows×cols depuis les tags final_candled_eggs[r][c].
    Indices 1-based dans les tags → 0-based dans la liste Python.
    """
    matrix = [[CLASS_MISSING] * cols for _ in range(rows)]

    for r in range(1, rows + 1):
        for c in range(1, cols + 1):
            key = f"final_candled_eggs[{r}][{c}]"
            raw = tags.get(key)
            if raw is not None:
                try:
                    matrix[r - 1][c - 1] = int(raw)
                except (ValueError, TypeError):
                    matrix[r - 1][c - 1] = CLASS_MISSING

    return matrix

# ---------------------------------------------------------------------------
# Recompute des comptages depuis la matrice (règle d'or)
# ---------------------------------------------------------------------------
 
def _recompute_counts(matrix: list[list[int]]) -> dict:
    """
    Compte chaque classe depuis la matrice.
    """
    counts = {
        "fertile": 0,
        "clear": 0,
        "early_dead": 0,
        "late_dead": 0,
        "missing": 0,
    }
    for row in matrix:
        for val in row:
            if val == CLASS_FERTILE:
                counts["fertile"] += 1
            elif val == CLASS_CLEAR:
                counts["clear"] += 1
            elif val == CLASS_EARLY_DEAD:
                counts["early_dead"] += 1
            elif val == CLASS_LATE_DEAD:
                counts["late_dead"] += 1
            else:
                counts["missing"] += 1
    return counts

# ---------------------------------------------------------------------------
# Sérialisation de la matrice
# ---------------------------------------------------------------------------

def _matrix_to_compact(matrix: list[list[int]]) -> str:
    """Convertit la matrice 15×10 en chaîne de 150 caractères (row-major)."""
    return "".join(str(cell) for row in matrix for cell in row)
 
 
def _extract_light_flat(tags: dict, rows: int = MATRIX_ROWS, cols: int = MATRIX_COLS) -> list[int]:
    """
    Extrait les rows×cols valeurs laser1_light_transmitted_eggs[r][c]
    dans l'ordre row-major.
    Valeur manquante → 0.
    """
    flat = []
    for r in range(1, rows + 1):
        for c in range(1, cols + 1):
            key = f"laser1_light_transmitted_eggs[{r}][{c}]"
            raw = tags.get(key)
            try:
                flat.append(int(raw) if raw is not None else 0)
            except (ValueError, TypeError):
                flat.append(0)
    return flat

# ---------------------------------------------------------------------------
# Idempotence
# ---------------------------------------------------------------------------
 
def _compute_tray_id(machine_id: str, candled_at_utc: datetime) -> str:
    """
    tray_id = SHA-256(machine_id | candled_at_utc ISO)
    Garantit l'unicité et l'idempotence : un même plateau rejoué
    deux fois produit le même tray_id → pas de doublon Silver.
    """
    payload = f"{machine_id}|{candled_at_utc.isoformat()}"
    return hashlib.sha256(payload.encode()).hexdigest()

# ---------------------------------------------------------------------------
# Helpers lecture tags
# ---------------------------------------------------------------------------
 
def _tag_int(tags: dict, name: str, default: int = 0) -> int:
    raw = tags.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (ValueError, TypeError):
        return default
 
 
def _tag_str(tags: dict, name: str, default: str = "") -> str:
    return str(tags.get(name, default)).strip().strip('"') or default