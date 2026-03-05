"""Shared helpers for queue-based data ingestion into PostgreSQL."""

import hashlib
import json
import logging
import re
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum

# Module-level logger
LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column / table-name constants
# ---------------------------------------------------------------------------

# Columns that exist on every raw entity table (the "meta" envelope)
BASE_COLUMNS = {
    "meta_message_hash": "TEXT NOT NULL",
    "meta_row_index": "INTEGER NOT NULL",
    "meta_entity_type": "TEXT NOT NULL",
    "meta_entity_id": "TEXT",
    "meta_provider": "TEXT",
    "meta_league": "TEXT",
    "meta_season": "INTEGER",
    "meta_source_path": "TEXT",
    "meta_source_created_at": "TIMESTAMPTZ",
    "meta_ingested_at": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
    "payload_json": "JSONB NOT NULL",
}

# DDL for the ingestion control / audit table
DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS raw_ingestion_events (
        id BIGSERIAL PRIMARY KEY,
        message_hash TEXT NOT NULL UNIQUE,
        dedupe_key TEXT,
        entity_type TEXT NOT NULL,
        entity_id TEXT,
        provider TEXT,
        league_key TEXT,
        season INTEGER,
        payload_format TEXT,
        source_path TEXT,
        status TEXT NOT NULL,
        error_message TEXT,
        created_at TIMESTAMPTZ,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    ALTER TABLE raw_ingestion_events
    ADD COLUMN IF NOT EXISTS dedupe_key TEXT;
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_raw_ingestion_events_status_dedupe
    ON raw_ingestion_events (status, dedupe_key);
    """,
]


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------

def quote_ident(identifier: str) -> str:
    """Safely double-quote a SQL identifier after validating it.

    Only allows lowercase letters, digits, and underscores.
    Raises ValueError for anything else to prevent injection.
    """
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", identifier):
        raise ValueError(f"Invalid identifier: {identifier}")
    return f'"{identifier}"'


def normalize_identifier(value: str) -> str:
    """Normalize an arbitrary string into a valid SQL column name.

    Replaces non-alphanumeric characters with underscores, lowercases,
    and prepends ``col_`` if the result starts with a digit.
    """
    # Strip, replace non-alphanumeric, drop leading/trailing underscores
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    # Fallback for empty strings
    if not normalized:
        return "col_unknown"
    # SQL identifiers cannot start with a digit
    if normalized[0].isdigit():
        normalized = f"col_{normalized}"
    return normalized


def entity_table_name(entity_type: str) -> str:
    """Derive the PostgreSQL table name for a given entity type.

    E.g. ``"match"`` -> ``"raw_soccerdata_match"``.
    """
    return f"raw_soccerdata_{normalize_identifier(entity_type)}"


# ---------------------------------------------------------------------------
# Hashing / deduplication
# ---------------------------------------------------------------------------

def message_hash(msg: dict[str, Any]) -> str:
    """Compute a SHA-256 hash uniquely identifying a queue message.

    The hash considers the entity type/id, provider, league, season,
    source path, and creation timestamp -- enough to deduplicate
    identical extraction outputs.
    """
    payload = {
        "entity_type": msg.get("entity_type"),
        "entity_id": str(msg.get("entity_id")),
        "provider": msg.get("provider"),
        "league": msg.get("league"),
        "season": msg.get("season"),
        "path": msg.get("path"),
        "created_at": msg.get("created_at"),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def dedupe_key(msg: dict[str, Any]) -> str:
    """Compute a semantic deduplication key for a queue message.

    Unlike ``message_hash``, this ignores the file path and timestamp,
    so re-extractions of the same entity are recognized as duplicates.
    """
    payload = {
        "entity_type": str(msg.get("entity_type") or "").strip().lower(),
        "entity_id": str(msg.get("entity_id") or "").strip().lower(),
        "provider": str(msg.get("provider") or "").strip().lower(),
        "league": str(msg.get("league") or "").strip().lower(),
        "season": str(msg.get("season") or "").strip().lower(),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Payload loading
# ---------------------------------------------------------------------------

def replace_nan(value: Any) -> Any:
    """Replace pandas NaN with None for JSON serialization."""
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def clean_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Replace NaN values with None across all rows."""
    cleaned: list[dict[str, Any]] = []
    for row in records:
        cleaned.append({k: replace_nan(v) for k, v in row.items()})
    return cleaned


def load_payload(msg: dict[str, Any]) -> Any:
    """Load the raw data payload referenced by a queue message.

    Supports ``json`` and ``csv`` payload formats.
    """
    path = Path(str(msg.get("path", "")))
    payload_format = str(msg.get("payload_format", "")).lower().strip()

    # Verify the file actually exists on disk
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    # JSON payloads -> dict
    if payload_format == "json":
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    # CSV payloads -> list of dicts
    if payload_format == "csv":
        df = pd.read_csv(path)
        return clean_records(df.to_dict(orient="records"))

    raise ValueError(f"Unsupported payload_format: {payload_format}")


def serialize_value(value: Any) -> str | None:
    """Serialize a value to a string suitable for TEXT columns."""
    if value is None:
        return None
    # Dicts and lists are JSON-serialized
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return str(value)


def to_rows(payload: Any, entity_type: str) -> list[dict[str, Any]]:
    """Normalize a payload into a list of row dicts based on entity type.

    - ``match`` and ``player_profile``: expect a single JSON object -> [dict]
    - ``match_lineup``, ``team``, ``player``: expect a list of objects -> [dict, ...]
    """
    # Single-object entity types
    if entity_type in {"match", "player_profile"}:
        if not isinstance(payload, dict):
            raise TypeError(f"Payload for {entity_type} must be a JSON object")
        return [payload]

    # List-based entity types
    if entity_type in {"match_lineup", "team", "player"}:
        if not isinstance(payload, list):
            raise TypeError(f"Payload for {entity_type} must be a list")
        rows = [row for row in payload if isinstance(row, dict)]
        if len(rows) != len(payload):
            raise TypeError(f"Payload for {entity_type} contains non-object items")
        return rows

    raise ValueError(f"Unsupported entity_type: {entity_type}")


# ---------------------------------------------------------------------------
# Dynamic schema management
# ---------------------------------------------------------------------------

def extract_dynamic_columns(rows: list[dict[str, Any]]) -> list[str]:
    """Discover dynamic column names from a list of row dicts.

    Column names are normalized and de-duplicated.  Names that collide
    with ``BASE_COLUMNS`` get a ``src_`` prefix.
    """
    cols: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            col = normalize_identifier(str(key))
            # Avoid collisions with meta/envelope columns
            if col in BASE_COLUMNS:
                col = f"src_{col}"
            if col not in seen:
                seen.add(col)
                cols.append(col)
    return cols


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def ensure_control_tables(get_conn_fn) -> None:
    """Create the ``raw_ingestion_events`` control table if it does not exist.

    Parameters
    ----------
    get_conn_fn : callable
        A zero-argument function that returns a psycopg2 connection.
    """
    conn = get_conn_fn()
    try:
        with conn.cursor() as cursor:
            for stmt in DDL_STATEMENTS:
                cursor.execute(stmt)
        conn.commit()
    finally:
        conn.close()


def event_signature(msg: dict[str, Any]) -> tuple[str, str, str, str, str]:
    """Return a 5-tuple of lowered entity fields for legacy dedup matching."""
    return (
        str(msg.get("entity_type") or "").strip().lower(),
        str(msg.get("entity_id") or "").strip().lower(),
        str(msg.get("provider") or "").strip().lower(),
        str(msg.get("league") or "").strip().lower(),
        str(msg.get("season") or "").strip().lower(),
    )


def is_done_event(cursor, msg: dict[str, Any], dedupe_key_val: str) -> bool:
    """Check if a matching 'done' event already exists in the control table."""
    et, eid, prov, league, season = event_signature(msg)
    cursor.execute(
        """
        SELECT 1
        FROM raw_ingestion_events
        WHERE status = 'done'
          AND (
            dedupe_key = %s
            OR (
                dedupe_key IS NULL
                AND LOWER(COALESCE(entity_type, '')) = %s
                AND LOWER(COALESCE(entity_id, '')) = %s
                AND LOWER(COALESCE(provider, '')) = %s
                AND LOWER(COALESCE(league_key, '')) = %s
                AND LOWER(COALESCE(season::TEXT, '')) = %s
            )
          )
        LIMIT 1;
        """,
        (dedupe_key_val, et, eid, prov, league, season),
    )
    return cursor.fetchone() is not None


def ensure_entity_table(cursor, table_name: str) -> None:
    """Create the entity data table if it does not exist."""
    table_sql = quote_ident(table_name)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_sql} (
            meta_message_hash TEXT NOT NULL,
            meta_row_index INTEGER NOT NULL,
            meta_entity_type TEXT NOT NULL,
            meta_entity_id TEXT,
            meta_provider TEXT,
            meta_league TEXT,
            meta_season INTEGER,
            meta_source_path TEXT,
            meta_source_created_at TIMESTAMPTZ,
            meta_ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            payload_json JSONB NOT NULL,
            PRIMARY KEY (meta_message_hash, meta_row_index)
        );
        """
    )


def ensure_dynamic_columns(cursor, table_name: str, dynamic_columns: list[str]) -> None:
    """Add any missing dynamic columns to an existing entity table."""
    table_sql = quote_ident(table_name)
    for col in dynamic_columns:
        col_sql = quote_ident(col)
        cursor.execute(
            f"ALTER TABLE {table_sql} ADD COLUMN IF NOT EXISTS {col_sql} TEXT;"
        )


def persist_rows(
    cursor,
    msg: dict[str, Any],
    rows: list[dict[str, Any]],
    msg_hash: str,
) -> int:
    """Insert or upsert rows into the appropriate entity table.

    Returns the number of rows written.
    """
    entity_type = str(msg.get("entity_type") or "").strip().lower()
    table_name = entity_table_name(entity_type)

    # Ensure the target table and dynamic columns exist
    ensure_entity_table(cursor, table_name)
    dynamic_columns = extract_dynamic_columns(rows)
    ensure_dynamic_columns(cursor, table_name, dynamic_columns)

    # Build the column lists for the upsert statement
    base_insert_columns = [
        "meta_message_hash",
        "meta_row_index",
        "meta_entity_type",
        "meta_entity_id",
        "meta_provider",
        "meta_league",
        "meta_season",
        "meta_source_path",
        "meta_source_created_at",
        "payload_json",
    ]
    insert_columns = base_insert_columns + dynamic_columns

    # PK columns are excluded from the UPDATE SET clause
    pk_columns = {"meta_message_hash", "meta_row_index"}
    update_columns = [col for col in insert_columns if col not in pk_columns]

    # Build the SQL statement
    table_sql = quote_ident(table_name)
    columns_sql = ", ".join(quote_ident(col) for col in insert_columns)
    placeholders_sql = ", ".join(["%s"] * len(insert_columns))
    conflict_sql = ", ".join(quote_ident(col) for col in ["meta_message_hash", "meta_row_index"])
    update_sql = ", ".join(
        f"{quote_ident(col)}=EXCLUDED.{quote_ident(col)}" for col in update_columns
    )

    upsert_sql = f"""
    INSERT INTO {table_sql} ({columns_sql})
    VALUES ({placeholders_sql})
    ON CONFLICT ({conflict_sql})
    DO UPDATE SET {update_sql};
    """

    inserted = 0
    for idx, row in enumerate(rows):
        # Build params dict with meta columns
        params: dict[str, Any] = {
            "meta_message_hash": msg_hash,
            "meta_row_index": idx,
            "meta_entity_type": entity_type,
            "meta_entity_id": (
                str(msg.get("entity_id")) if msg.get("entity_id") is not None else None
            ),
            "meta_provider": msg.get("provider"),
            "meta_league": msg.get("league"),
            "meta_season": msg.get("season"),
            "meta_source_path": msg.get("path"),
            "meta_source_created_at": msg.get("created_at"),
            "payload_json": json.dumps(row, ensure_ascii=False, default=str),
        }

        # Add dynamic columns from the row data
        for original_key, value in row.items():
            col = normalize_identifier(str(original_key))
            if col in BASE_COLUMNS:
                col = f"src_{col}"
            params[col] = serialize_value(value)

        # Fill missing dynamic columns with None
        for col in dynamic_columns:
            params.setdefault(col, None)

        # Execute the upsert for this row
        values = [params[col] for col in insert_columns]
        cursor.execute(upsert_sql, values)
        inserted += 1

    return inserted


def persist_ingestion_event(
    cursor,
    msg: dict[str, Any],
    msg_hash: str,
    dedupe_key_val: str,
    status: str,
    error_message: str | None = None,
) -> None:
    """Record an ingestion event in the control table (upsert by message_hash)."""
    cursor.execute(
        """
        INSERT INTO raw_ingestion_events (
            message_hash,
            dedupe_key,
            entity_type,
            entity_id,
            provider,
            league_key,
            season,
            payload_format,
            source_path,
            status,
            error_message,
            created_at,
            processed_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            CAST(%s AS TIMESTAMPTZ), NOW()
        )
        ON CONFLICT (message_hash) DO UPDATE SET
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message,
            processed_at = NOW();
        """,
        (
            msg_hash,
            dedupe_key_val,
            msg.get("entity_type"),
            str(msg.get("entity_id")) if msg.get("entity_id") is not None else None,
            msg.get("provider"),
            msg.get("league"),
            msg.get("season"),
            msg.get("payload_format"),
            msg.get("path"),
            status,
            error_message,
            msg.get("created_at"),
        ),
    )


# ---------------------------------------------------------------------------
# Message processing (orchestrates load -> transform -> persist)
# ---------------------------------------------------------------------------

def process_message(msg: dict[str, Any], get_conn_fn) -> tuple[str, int, str | None]:
    """Process a single queue message: load, transform, persist.

    Opens a DB connection, persists the payload rows, records
    the ingestion event, and commits.  On failure, records a
    failed event and rolls back the data.

    Parameters
    ----------
    msg : dict
        The queue message (from pending.jsonl).
    get_conn_fn : callable
        Zero-argument function returning a psycopg2 connection.

    Returns
    -------
    tuple[str, int, str | None]
        (status, inserted_rows, error_message)
    """
    msg_hash = message_hash(msg)
    dk = dedupe_key(msg)

    conn = get_conn_fn()
    try:
        with conn.cursor() as cursor:
            # Load the payload from disk and normalize to rows
            payload = load_payload(msg)
            entity_type = str(msg.get("entity_type") or "").strip().lower()
            rows = to_rows(payload, entity_type)

            # Insert into the appropriate entity table
            inserted_rows = persist_rows(cursor, msg, rows, msg_hash)

            # Record a successful ingestion event
            persist_ingestion_event(
                cursor=cursor,
                msg=msg,
                msg_hash=msg_hash,
                dedupe_key_val=dk,
                status="done",
                error_message=None,
            )
        conn.commit()
        return "done", inserted_rows, None

    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        # Attempt to record the failure in the control table
        try:
            with conn.cursor() as cursor:
                persist_ingestion_event(
                    cursor=cursor,
                    msg=msg,
                    msg_hash=msg_hash,
                    dedupe_key_val=dk,
                    status="failed",
                    error_message=str(exc),
                )
            conn.commit()
        except Exception:  # noqa: BLE001
            conn.rollback()
        return "failed", 0, str(exc)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# File management helpers
# ---------------------------------------------------------------------------

def append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    """Append a list of dicts as JSONL lines to *path*."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def safe_move_file(src: Path, dst: Path) -> Path:
    """Move *src* to *dst*, adding a numeric suffix on name collision."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    # No collision: move directly
    if not dst.exists():
        shutil.move(str(src), str(dst))
        return dst

    # Collision: find a unique name with incrementing suffix
    stem = dst.stem
    suffix = dst.suffix
    counter = 1
    while True:
        candidate = dst.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            shutil.move(str(src), str(candidate))
            return candidate
        counter += 1


def move_processed_files(
    output_root: Path,
    done_rows: list[dict[str, Any]],
    move_enabled: bool,
) -> int:
    """Move successfully processed source files to output/processed/.

    Returns the number of files moved.
    """
    if not move_enabled:
        return 0

    moved_count = 0
    # Track already-moved paths to avoid redundant moves
    moved_targets: dict[str, str] = {}

    for row in done_rows:
        src_path_str = str(row.get("path") or "").strip()
        if not src_path_str:
            continue

        # Skip if we already moved this exact path
        if src_path_str in moved_targets:
            row["moved_to"] = moved_targets[src_path_str]
            continue

        src = Path(src_path_str)
        if not src.exists() or not src.is_file():
            continue

        # Compute destination preserving relative path structure
        try:
            rel = src.relative_to(output_root)
            dst = output_root / "processed" / rel
        except ValueError:
            dst = output_root / "processed" / src.name

        final_dst = safe_move_file(src, dst)
        moved_targets[src_path_str] = str(final_dst)
        row["moved_to"] = str(final_dst)
        moved_count += 1

    return moved_count
