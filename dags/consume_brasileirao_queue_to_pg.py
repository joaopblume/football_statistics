"""DAG: consume_brasileirao_queue_to_pg -- Ingest queued data into PostgreSQL.

Reads ``pending.jsonl``, deduplicates messages, and persists entity data
into dynamically created ``raw_soccerdata_*`` tables using idempotent
upserts.  Supports entity types: match, match_lineup, team, player,
and player_profile.

All heavy business logic lives in ``dags/lib/ingestion_helpers.py``.
"""

import json
import logging
import os
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from lib.ingestion_helpers import (
    append_jsonl,
    dedupe_key,
    ensure_control_tables,
    is_done_event,
    message_hash,
    move_processed_files,
    process_message,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGGER = logging.getLogger(__name__)
POSTGRES_CONN_ID = os.getenv("PG_CONN_ID", "db-pg-futebol-dados")

# Retry / timeout defaults applied to every task in this DAG
DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------


def _get_conn():
    """Create a new PostgreSQL connection via Airflow's connection manager.

    Returns a psycopg2 connection with autocommit disabled so callers
    control transaction boundaries explicitly.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs corrupting the queue file
    default_args=DEFAULT_ARGS,
    tags=["soccerdata", "queue", "postgres", "ingestion", "brasileirao"],
)
def consume_brasileirao_queue_to_pg():
    """Consume pending.jsonl and persist data into PostgreSQL."""

    @task()
    def consume_pending_queue() -> str:
        """Read, deduplicate, and ingest pending queue messages into PostgreSQL.

        Processes up to QUEUE_BATCH_SIZE messages per run.  Successfully
        processed messages go to ``done.jsonl``; failures go to ``failed.jsonl``.
        Remaining messages are written back to ``pending.jsonl``.
        """
        started = time.perf_counter()

        # Resolve configuration from environment
        output_root = Path(os.getenv("OUTPUT_DIR", "output")).resolve()
        queue_dir = output_root / "queue"
        pending_file = queue_dir / "pending.jsonl"
        done_file = queue_dir / "done.jsonl"
        failed_file = queue_dir / "failed.jsonl"
        batch_size = int(os.getenv("QUEUE_BATCH_SIZE", "100"))
        move_processed = os.getenv("MOVE_PROCESSED_FILES", "true").strip().lower() in {
            "1", "true", "yes", "y", "on",
        }

        LOGGER.info(
            "Starting consume_pending_queue output=%s batch_size=%s conn_id=%s move=%s",
            output_root,
            batch_size,
            POSTGRES_CONN_ID,
            move_processed,
        )

        # Guard: nothing to do if the queue file does not exist
        if not pending_file.exists():
            return f"No pending queue file found at {pending_file}"

        # Ensure the ingestion control table exists
        ensure_control_tables(_get_conn)

        # Read all lines from the pending file
        raw_lines = pending_file.read_text(encoding="utf-8").splitlines()
        if not raw_lines:
            return f"No pending messages in {pending_file}"

        # ------------------------------------------------------------------
        # Phase 1: Parse JSON lines
        # ------------------------------------------------------------------
        valid_messages: list[dict[str, Any]] = []
        parse_failures: list[dict[str, Any]] = []

        for line in raw_lines:
            try:
                valid_messages.append(json.loads(line))
            except json.JSONDecodeError as exc:
                parse_failures.append(
                    {
                        "status": "failed",
                        "error": f"json_decode_error: {exc}",
                        "raw_line": line,
                        "failed_at": pendulum.now("UTC").to_iso8601_string(),
                    }
                )

        # ------------------------------------------------------------------
        # Phase 2: In-batch deduplication by dedupe_key
        # ------------------------------------------------------------------
        deduped_messages: list[dict[str, Any]] = []
        duplicate_rows: list[dict[str, Any]] = []
        seen_dedupe_keys: set[str] = set()

        for msg in valid_messages:
            msg_hash = message_hash(msg)
            msg_dk = dedupe_key(msg)

            # Skip if we already saw this dedupe key in this batch
            if msg_dk in seen_dedupe_keys:
                duplicate_rows.append(
                    {
                        "status": "done",
                        "processed_at": pendulum.now("UTC").to_iso8601_string(),
                        "entity_type": msg.get("entity_type"),
                        "entity_id": msg.get("entity_id"),
                        "path": msg.get("path"),
                        "provider": msg.get("provider"),
                        "league": msg.get("league"),
                        "season": msg.get("season"),
                        "payload_format": msg.get("payload_format"),
                        "message_hash": msg_hash,
                        "dedupe_key": msg_dk,
                        "inserted_rows": 0,
                        "skip_reason": "duplicate_in_pending",
                    }
                )
                continue
            seen_dedupe_keys.add(msg_dk)
            deduped_messages.append(msg)

        # ------------------------------------------------------------------
        # Phase 3: Check DB for already-done events
        # ------------------------------------------------------------------
        pending_not_done: list[dict[str, Any]] = []
        already_done_rows: list[dict[str, Any]] = []

        done_lookup_conn = _get_conn()
        try:
            with done_lookup_conn.cursor() as done_cursor:
                for msg in deduped_messages:
                    msg_hash = message_hash(msg)
                    msg_dk = dedupe_key(msg)
                    # Check if this entity was already ingested successfully
                    if is_done_event(done_cursor, msg, msg_dk):
                        already_done_rows.append(
                            {
                                "status": "done",
                                "processed_at": pendulum.now("UTC").to_iso8601_string(),
                                "entity_type": msg.get("entity_type"),
                                "entity_id": msg.get("entity_id"),
                                "path": msg.get("path"),
                                "provider": msg.get("provider"),
                                "league": msg.get("league"),
                                "season": msg.get("season"),
                                "payload_format": msg.get("payload_format"),
                                "message_hash": msg_hash,
                                "dedupe_key": msg_dk,
                                "inserted_rows": 0,
                                "skip_reason": "already_done_in_db",
                            }
                        )
                    else:
                        pending_not_done.append(msg)
        finally:
            done_lookup_conn.close()

        # ------------------------------------------------------------------
        # Phase 4: Process the batch
        # ------------------------------------------------------------------
        batch = pending_not_done[:batch_size]
        remaining = pending_not_done[batch_size:]

        done_rows: list[dict[str, Any]] = duplicate_rows + already_done_rows
        failed_rows: list[dict[str, Any]] = parse_failures.copy()

        processed = 0
        inserted_total = 0

        for idx, msg in enumerate(batch, start=1):
            msg_hash_val = message_hash(msg)
            msg_dk = dedupe_key(msg)

            # Process the message (load -> transform -> persist)
            status, inserted_rows, error = process_message(msg, _get_conn)
            processed += 1
            inserted_total += inserted_rows

            # Build the result record
            base = {
                "status": status,
                "processed_at": pendulum.now("UTC").to_iso8601_string(),
                "entity_type": msg.get("entity_type"),
                "entity_id": msg.get("entity_id"),
                "path": msg.get("path"),
                "provider": msg.get("provider"),
                "league": msg.get("league"),
                "season": msg.get("season"),
                "payload_format": msg.get("payload_format"),
                "message_hash": msg_hash_val,
                "dedupe_key": msg_dk,
                "inserted_rows": inserted_rows,
            }

            if status == "done":
                done_rows.append(base)
            else:
                base["error"] = error
                failed_rows.append(base)

            # Periodic progress logging
            if idx == 1 or idx % 25 == 0:
                LOGGER.info(
                    "Ingestion progress: %s/%s (done=%s failed=%s inserted=%s)",
                    idx,
                    len(batch),
                    len(done_rows),
                    len(failed_rows),
                    inserted_total,
                )

        # ------------------------------------------------------------------
        # Phase 5: Update queue files and move processed source files
        # ------------------------------------------------------------------

        # Rewrite pending.jsonl with only the remaining unprocessed messages
        serialized_remaining = [json.dumps(item, ensure_ascii=False) for item in remaining]
        pending_file.write_text(
            "\n".join(serialized_remaining) + ("\n" if serialized_remaining else ""),
            encoding="utf-8",
        )

        # Move source files for successfully ingested messages
        moved_count = move_processed_files(
            output_root=output_root,
            done_rows=done_rows,
            move_enabled=move_processed,
        )

        # Append results to done.jsonl and failed.jsonl
        append_jsonl(done_file, done_rows)
        append_jsonl(failed_file, failed_rows)

        elapsed = time.perf_counter() - started
        summary = (
            f"Ingestion completed in {elapsed:.2f}s. "
            f"Read={len(raw_lines)} valid={len(valid_messages)} parse_fail={len(parse_failures)}. "
            f"Processed={processed} done={len(done_rows)} failed={len(failed_rows)} "
            f"inserted_rows={inserted_total}. "
            f"Files_moved={moved_count}. "
            f"Remaining={len(remaining)} in {pending_file}."
        )
        LOGGER.info(summary)
        return summary

    # Run the single task
    consume_pending_queue()


# Instantiate the DAG
consume_brasileirao_queue_to_pg()
