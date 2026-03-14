"""
Airflow DAG: Инкрементальная синхронизация данных goszakup.gov.kz.

Запускается каждые 12 часов:
  1. incremental_sync — журнал изменений → дозагрузка обновлённых записей
  2. run_etl          — очистка + обогащение данных
  3. refresh_views    — обновление 5 materialized views
"""
import asyncio
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.config import db_config, TARGET_BINS
from src.ingestion.api_client import GoszakupClient
from src.ingestion.data_loader import (
    get_conn,
    load_subjects,
    load_announcements,
    load_lots,
    load_contracts,
    load_plans,
)
from src.etl.pipeline import run_etl_pipeline

import psycopg2

logger = logging.getLogger(__name__)

# ============================================================
# Default args & DAG definition
# ============================================================

default_args = {
    "owner": "goszakup",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="goszakup_incremental_sync",
    default_args=default_args,
    description="Incremental sync from goszakup.gov.kz OWS v3 API every 12 hours",
    schedule_interval="0 */12 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["goszakup", "etl", "sync"],
)


# ============================================================
# Helpers
# ============================================================

ENTITY_MAP = {
    "TrdBuy": {
        "fetch": "fetch_announcements",
        "load": load_announcements,
        "filter_key": "org_bin",
    },
    "Lots": {
        "fetch": "fetch_lots",
        "load": load_lots,
        "filter_key": "customer_bin",
    },
    "Contract": {
        "fetch": "fetch_contracts",
        "load": load_contracts,
        "filter_key": "customer_bin",
    },
    "Plans": {
        "fetch": "fetch_plans",
        "load": load_plans,
        "filter_key": "subject_bin",
    },
    "Subjects": {
        "fetch": "fetch_subjects",
        "load": load_subjects,
        "filter_key": "bin",
    },
}

JOURNAL_QUERY = """
query($limit: Int, $after: Int) {
    Journal(limit: $limit, after: $after, filter: {indexDate: "%s"}) {
        id entityType entityId
        indexDate
    }
}
"""

MATERIALIZED_VIEWS = [
    "mv_price_statistics",
    "mv_volume_trends",
    "mv_supplier_stats",
    "mv_regional_coefficients",
    "mv_data_overview",
]


def _get_last_sync_ts(conn) -> str:
    """
    Reads last successful sync timestamp from sync_journal table.
    Falls back to 24 hours ago if no record exists.
    """
    fallback = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(sync_ts)
                FROM sync_journal
                WHERE status = 'completed'
            """)
            row = cur.fetchone()
            if row and row[0]:
                return row[0].strftime("%Y-%m-%dT%H:%M:%S")
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        # Create sync_journal table if it does not exist
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sync_journal (
                    id          SERIAL PRIMARY KEY,
                    sync_ts     TIMESTAMP NOT NULL DEFAULT NOW(),
                    status      VARCHAR(20) NOT NULL DEFAULT 'running',
                    entities    JSONB,
                    total_count INTEGER DEFAULT 0,
                    error_msg   TEXT,
                    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
        conn.commit()
        logger.info("Created sync_journal table")
    except Exception:
        conn.rollback()
    return fallback


def _record_sync(conn, status: str, entities: dict, total: int, error: str | None = None):
    """Write a sync record into sync_journal."""
    import json
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sync_journal (
                    id          SERIAL PRIMARY KEY,
                    sync_ts     TIMESTAMP NOT NULL DEFAULT NOW(),
                    status      VARCHAR(20) NOT NULL DEFAULT 'running',
                    entities    JSONB,
                    total_count INTEGER DEFAULT 0,
                    error_msg   TEXT,
                    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute(
                """INSERT INTO sync_journal (status, entities, total_count, error_msg)
                   VALUES (%s, %s, %s, %s)""",
                (status, json.dumps(entities, ensure_ascii=False), total, error),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to record sync journal: {e}")


# ============================================================
# Task 1: Incremental Sync
# ============================================================

def _incremental_sync(**context):
    """
    Reads the API journal for changes since last sync, fetches
    updated records for each changed entity, and upserts them
    into PostgreSQL.
    """
    conn = get_conn()
    client = GoszakupClient()

    try:
        # 1. Determine the last sync timestamp
        since = _get_last_sync_ts(conn)
        logger.info(f"Incremental sync since: {since}")

        # 2. Fetch journal entries from API
        journal_query = JOURNAL_QUERY % since
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            journal_entries = loop.run_until_complete(
                client.fetch_all("Journal", journal_query, max_records=10000)
            )
        except Exception as e:
            logger.warning(
                f"Journal endpoint unavailable ({e}). "
                "Falling back to full re-fetch for all TARGET_BINS."
            )
            journal_entries = None

        counts = {}

        if journal_entries:
            # 3a. Group changed entity IDs by type
            changed: dict[str, set[int]] = {}
            for entry in journal_entries:
                entity_type = entry.get("entityType", "")
                entity_id = entry.get("entityId")
                if entity_type and entity_id is not None:
                    changed.setdefault(entity_type, set()).add(entity_id)

            logger.info(
                f"Journal returned {len(journal_entries)} changes across "
                f"{len(changed)} entity types: {list(changed.keys())}"
            )

            # 4a. For each changed entity type, re-fetch and upsert
            for entity_type, entity_ids in changed.items():
                mapping = ENTITY_MAP.get(entity_type)
                if not mapping:
                    logger.warning(f"Unknown entity type in journal: {entity_type}")
                    continue

                fetch_method = getattr(client, mapping["fetch"])
                load_fn = mapping["load"]
                filter_key = mapping["filter_key"]
                total = 0

                for bin_code in TARGET_BINS:
                    kwargs = {filter_key: bin_code}
                    records = loop.run_until_complete(fetch_method(**kwargs))
                    # Keep only records whose source IDs appear in the journal
                    relevant = [
                        r for r in records
                        if r.get("id") in entity_ids
                    ]
                    if relevant:
                        cnt = load_fn(conn, relevant)
                        total += cnt
                        logger.info(
                            f"  {entity_type} BIN={bin_code}: "
                            f"upserted {cnt}/{len(relevant)} changed records"
                        )

                counts[entity_type] = total
                logger.info(f"{entity_type}: {total} records synced")

        else:
            # 3b. Fallback: re-fetch recent data for all BINs
            logger.info("Performing full incremental re-fetch for all TARGET_BINS")

            for entity_type, mapping in ENTITY_MAP.items():
                fetch_method = getattr(client, mapping["fetch"])
                load_fn = mapping["load"]
                filter_key = mapping["filter_key"]
                total = 0

                for bin_code in TARGET_BINS:
                    kwargs = {filter_key: bin_code, "max_records": 500}
                    records = loop.run_until_complete(fetch_method(**kwargs))
                    if records:
                        cnt = load_fn(conn, records)
                        total += cnt

                counts[entity_type] = total
                logger.info(f"{entity_type}: {total} records synced (fallback)")

        loop.run_until_complete(client.close())
        loop.close()

        total_all = sum(counts.values())
        _record_sync(conn, "completed", counts, total_all)

        logger.info(f"Incremental sync completed. Total upserted: {total_all}")
        logger.info(f"  Breakdown: {counts}")

    except Exception as e:
        logger.error(f"Incremental sync failed: {e}")
        _record_sync(conn, "failed", {}, 0, str(e))
        raise
    finally:
        conn.close()


# ============================================================
# Task 2: Run ETL
# ============================================================

def _run_etl(**context):
    """Runs the full ETL pipeline (clean + enrich)."""
    logger.info("Starting ETL pipeline...")
    run_etl_pipeline()
    logger.info("ETL pipeline completed.")


# ============================================================
# Task 3: Refresh Materialized Views
# ============================================================

def _refresh_views(**context):
    """Refreshes all 5 materialized views."""
    conn = get_conn()
    try:
        for view_name in MATERIALIZED_VIEWS:
            try:
                with conn.cursor() as cur:
                    cur.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
                conn.commit()
                logger.info(f"Refreshed materialized view: {view_name}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to refresh {view_name}: {e}")
                raise
    finally:
        conn.close()


# ============================================================
# Task wiring
# ============================================================

incremental_sync = PythonOperator(
    task_id="incremental_sync",
    python_callable=_incremental_sync,
    dag=dag,
)

run_etl = PythonOperator(
    task_id="run_etl",
    python_callable=_run_etl,
    dag=dag,
)

refresh_views = PythonOperator(
    task_id="refresh_views",
    python_callable=_refresh_views,
    dag=dag,
)

incremental_sync >> run_etl >> refresh_views
