"""
src/maintenance/function_app.py
--------------------------------
Azure Function Timer trigger : compaction quotidienne de silver.trays.

Planification : tous les jours à 23h30 UTC.
CRON Azure    : "0 30 23 * * *"
"""

import logging
from datetime import datetime, timezone

import azure.functions as func

from src.maintenance.compaction import compact_day

logger = logging.getLogger(__name__)

app = func.FunctionApp()


@app.timer_trigger(
    schedule="0 30 23 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def DailyCompaction(timer: func.TimerRequest) -> None:
    """
    Compacte toutes les partitions Silver de la journée courante.
    Idempotent : peut être relancé sans risque si en retard.
    """
    utc_now = datetime.now(timezone.utc)

    if timer.past_due:
        logger.warning("Timer en retard — exécution rattrapée à %s", utc_now.isoformat())

    year, month, day = utc_now.year, utc_now.month, utc_now.day
    logger.info("Compaction quotidienne démarrée pour %04d-%02d-%02d", year, month, day)

    result = compact_day(year=year, month=month, day=day)

    if result["status"] == "ok":
        machines = result.get("machines", {})
        ok_count = sum(1 for r in machines.values() if r.get("status") == "ok")
        logger.info(
            "Compaction terminée : %d/%d machine(s) compactée(s)",
            ok_count, len(machines),
        )
    elif result["status"] == "empty":
        logger.info("Aucune donnée à compacter pour aujourd'hui")
    else:
        logger.error("Compaction échouée : %s", result.get("error", "inconnu"))