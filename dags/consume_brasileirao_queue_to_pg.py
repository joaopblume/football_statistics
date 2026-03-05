import hashlib
import json
import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

LOGGER = logging.getLogger(__name__)
POSTGRES_CONN_ID = os.getenv("PG_CONN_ID", "db-pg-futebol-dados")

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


def _get_conn():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    return conn


def _quote_ident(identifier: str) -> str:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", identifier):
        raise ValueError(f"Identificador invalido: {identifier}")
    return f'"{identifier}"'


def _replace_nan(value: Any) -> Any:
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def _clean_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    for row in records:
        cleaned.append({k: _replace_nan(v) for k, v in row.items()})
    return cleaned


def _normalize_identifier(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip()).strip("_").lower()
    if not normalized:
        return "col_unknown"
    if normalized[0].isdigit():
        normalized = f"col_{normalized}"
    return normalized


def _entity_table_name(entity_type: str) -> str:
    return f"raw_soccerdata_{_normalize_identifier(entity_type)}"


def _message_hash(msg: dict[str, Any]) -> str:
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


def _dedupe_key(msg: dict[str, Any]) -> str:
    payload = {
        "entity_type": str(msg.get("entity_type") or "").strip().lower(),
        "entity_id": str(msg.get("entity_id") or "").strip().lower(),
        "provider": str(msg.get("provider") or "").strip().lower(),
        "league": str(msg.get("league") or "").strip().lower(),
        "season": str(msg.get("season") or "").strip().lower(),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _load_payload(msg: dict[str, Any]) -> Any:
    path = Path(str(msg.get("path", "")))
    payload_format = str(msg.get("payload_format", "")).lower().strip()

    if not path.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {path}")

    if payload_format == "json":
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    if payload_format == "csv":
        df = pd.read_csv(path)
        return _clean_records(df.to_dict(orient="records"))

    raise ValueError(f"payload_format nao suportado: {payload_format}")


def _serialize_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return str(value)


def _extract_dynamic_columns(rows: list[dict[str, Any]]) -> list[str]:
    cols: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            col = _normalize_identifier(str(key))
            if col in BASE_COLUMNS:
                col = f"src_{col}"
            if col not in seen:
                seen.add(col)
                cols.append(col)
    return cols


def _ensure_control_tables() -> None:
    conn = _get_conn()
    try:
        with conn.cursor() as cursor:
            for stmt in DDL_STATEMENTS:
                cursor.execute(stmt)
        conn.commit()
    finally:
        conn.close()


def _event_signature(msg: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(msg.get("entity_type") or "").strip().lower(),
        str(msg.get("entity_id") or "").strip().lower(),
        str(msg.get("provider") or "").strip().lower(),
        str(msg.get("league") or "").strip().lower(),
        str(msg.get("season") or "").strip().lower(),
    )


def _is_done_event(cursor, msg: dict[str, Any], dedupe_key: str) -> bool:
    entity_type, entity_id, provider, league, season = _event_signature(msg)
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
        (dedupe_key, entity_type, entity_id, provider, league, season),
    )
    return cursor.fetchone() is not None


def _ensure_entity_table(cursor, table_name: str) -> None:
    table_sql = _quote_ident(table_name)
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


def _ensure_dynamic_columns(cursor, table_name: str, dynamic_columns: list[str]) -> None:
    table_sql = _quote_ident(table_name)
    for col in dynamic_columns:
        col_sql = _quote_ident(col)
        cursor.execute(f"ALTER TABLE {table_sql} ADD COLUMN IF NOT EXISTS {col_sql} TEXT;")


def _persist_rows(cursor, msg: dict[str, Any], rows: list[dict[str, Any]]) -> int:
    entity_type = str(msg.get("entity_type") or "").strip().lower()
    table_name = _entity_table_name(entity_type)
    message_hash = _message_hash(msg)

    _ensure_entity_table(cursor, table_name)
    dynamic_columns = _extract_dynamic_columns(rows)
    _ensure_dynamic_columns(cursor, table_name, dynamic_columns)

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

    pk_columns = {"meta_message_hash", "meta_row_index"}
    update_columns = [col for col in insert_columns if col not in pk_columns]

    table_sql = _quote_ident(table_name)
    columns_sql = ", ".join(_quote_ident(col) for col in insert_columns)
    placeholders_sql = ", ".join(["%s"] * len(insert_columns))
    conflict_sql = ", ".join(_quote_ident(col) for col in ["meta_message_hash", "meta_row_index"])
    update_sql = ", ".join(f"{_quote_ident(col)}=EXCLUDED.{_quote_ident(col)}" for col in update_columns)

    upsert_sql = f"""
    INSERT INTO {table_sql} ({columns_sql})
    VALUES ({placeholders_sql})
    ON CONFLICT ({conflict_sql})
    DO UPDATE SET {update_sql};
    """

    inserted = 0
    for idx, row in enumerate(rows):
        params: dict[str, Any] = {
            "meta_message_hash": message_hash,
            "meta_row_index": idx,
            "meta_entity_type": entity_type,
            "meta_entity_id": str(msg.get("entity_id")) if msg.get("entity_id") is not None else None,
            "meta_provider": msg.get("provider"),
            "meta_league": msg.get("league"),
            "meta_season": msg.get("season"),
            "meta_source_path": msg.get("path"),
            "meta_source_created_at": msg.get("created_at"),
            "payload_json": json.dumps(row, ensure_ascii=False, default=str),
        }

        for original_key, value in row.items():
            col = _normalize_identifier(str(original_key))
            if col in BASE_COLUMNS:
                col = f"src_{col}"
            params[col] = _serialize_value(value)

        for col in dynamic_columns:
            params.setdefault(col, None)

        values = [params[col] for col in insert_columns]
        cursor.execute(upsert_sql, values)
        inserted += 1

    return inserted


def _persist_ingestion_event(
    cursor,
    msg: dict[str, Any],
    message_hash: str,
    dedupe_key: str,
    status: str,
    error_message: str | None = None,
) -> None:
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
            message_hash,
            dedupe_key,
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


def _to_rows(payload: Any, entity_type: str) -> list[dict[str, Any]]:
    if entity_type == "match":
        if not isinstance(payload, dict):
            raise TypeError("Payload de match precisa ser JSON objeto")
        return [payload]

    if entity_type in {"match_lineup", "team", "player"}:
        if not isinstance(payload, list):
            raise TypeError(f"Payload de {entity_type} precisa ser lista")
        rows = [row for row in payload if isinstance(row, dict)]
        if len(rows) != len(payload):
            raise TypeError(f"Payload de {entity_type} contem itens nao-objeto")
        return rows

    raise ValueError(f"entity_type nao suportado: {entity_type}")


def _process_message(msg: dict[str, Any]) -> tuple[str, int, str | None]:
    entity_type = str(msg.get("entity_type") or "").strip().lower()
    message_hash = _message_hash(msg)
    dedupe_key = _dedupe_key(msg)

    conn = _get_conn()
    try:
        with conn.cursor() as cursor:
            payload = _load_payload(msg)
            rows = _to_rows(payload, entity_type)
            inserted_rows = _persist_rows(cursor, msg, rows)
            _persist_ingestion_event(
                cursor=cursor,
                msg=msg,
                message_hash=message_hash,
                dedupe_key=dedupe_key,
                status="done",
                error_message=None,
            )
        conn.commit()
        return "done", inserted_rows, None
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        try:
            with conn.cursor() as cursor:
                _persist_ingestion_event(
                    cursor=cursor,
                    msg=msg,
                    message_hash=message_hash,
                    dedupe_key=dedupe_key,
                    status="failed",
                    error_message=str(exc),
                )
            conn.commit()
        except Exception:  # noqa: BLE001
            conn.rollback()
        return "failed", 0, str(exc)
    finally:
        conn.close()


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def _safe_move_file(src: Path, dst: Path) -> Path:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.move(str(src), str(dst))
        return dst

    stem = dst.stem
    suffix = dst.suffix
    counter = 1
    while True:
        candidate = dst.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            shutil.move(str(src), str(candidate))
            return candidate
        counter += 1


def _move_processed_files(
    output_root: Path,
    done_rows: list[dict[str, Any]],
    move_enabled: bool,
) -> int:
    if not move_enabled:
        return 0

    moved_count = 0
    moved_targets: dict[str, str] = {}

    for row in done_rows:
        src_path_str = str(row.get("path") or "").strip()
        if not src_path_str:
            continue

        if src_path_str in moved_targets:
            row["moved_to"] = moved_targets[src_path_str]
            continue

        src = Path(src_path_str)
        if not src.exists() or not src.is_file():
            continue

        try:
            rel = src.relative_to(output_root)
            dst = output_root / "processed" / rel
        except ValueError:
            dst = output_root / "processed" / src.name

        final_dst = _safe_move_file(src, dst)
        moved_targets[src_path_str] = str(final_dst)
        row["moved_to"] = str(final_dst)
        moved_count += 1

    return moved_count


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["soccerdata", "queue", "postgres", "ingestion", "brasileirao"],
)
def consume_brasileirao_queue_to_pg():
    @task()
    def consume_pending_queue() -> str:
        started = time.perf_counter()
        output_root = Path(os.getenv("OUTPUT_DIR", "output")).resolve()
        queue_dir = output_root / "queue"
        pending_file = queue_dir / "pending.jsonl"
        done_file = queue_dir / "done.jsonl"
        failed_file = queue_dir / "failed.jsonl"
        batch_size = int(os.getenv("QUEUE_BATCH_SIZE", "100"))
        move_processed_files = os.getenv("MOVE_PROCESSED_FILES", "true").strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }

        LOGGER.info(
            "Inicio consume_pending_queue output=%s batch_size=%s conn_id=%s move_processed_files=%s",
            output_root,
            batch_size,
            POSTGRES_CONN_ID,
            move_processed_files,
        )

        if not pending_file.exists():
            return f"Fila pendente nao encontrada: {pending_file}"

        _ensure_control_tables()

        raw_lines = pending_file.read_text(encoding="utf-8").splitlines()
        if not raw_lines:
            return f"Nenhuma mensagem pendente em {pending_file}"

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

        deduped_messages: list[dict[str, Any]] = []
        duplicate_rows: list[dict[str, Any]] = []
        seen_dedupe_keys: set[str] = set()
        for msg in valid_messages:
            msg_hash = _message_hash(msg)
            msg_dedupe_key = _dedupe_key(msg)
            if msg_dedupe_key in seen_dedupe_keys:
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
                        "dedupe_key": msg_dedupe_key,
                        "inserted_rows": 0,
                        "skip_reason": "duplicate_in_pending",
                    }
                )
                continue
            seen_dedupe_keys.add(msg_dedupe_key)
            deduped_messages.append(msg)

        pending_not_done: list[dict[str, Any]] = []
        already_done_rows: list[dict[str, Any]] = []
        done_lookup_conn = _get_conn()
        try:
            with done_lookup_conn.cursor() as done_cursor:
                for msg in deduped_messages:
                    msg_hash = _message_hash(msg)
                    msg_dedupe_key = _dedupe_key(msg)
                    if _is_done_event(done_cursor, msg, msg_dedupe_key):
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
                                "dedupe_key": msg_dedupe_key,
                                "inserted_rows": 0,
                                "skip_reason": "already_done_in_db",
                            }
                        )
                    else:
                        pending_not_done.append(msg)
        finally:
            done_lookup_conn.close()

        batch = pending_not_done[:batch_size]
        remaining = pending_not_done[batch_size:]

        done_rows: list[dict[str, Any]] = duplicate_rows + already_done_rows
        failed_rows: list[dict[str, Any]] = parse_failures.copy()

        processed = 0
        inserted_total = 0

        for idx, msg in enumerate(batch, start=1):
            msg_hash = _message_hash(msg)
            msg_dedupe_key = _dedupe_key(msg)

            status, inserted_rows, error = _process_message(msg)
            processed += 1
            inserted_total += inserted_rows

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
                "message_hash": msg_hash,
                "dedupe_key": msg_dedupe_key,
                "inserted_rows": inserted_rows,
            }

            if status == "done":
                done_rows.append(base)
            else:
                base["error"] = error
                failed_rows.append(base)

            if idx == 1 or idx % 25 == 0:
                LOGGER.info(
                    "Progresso consumo: %s/%s (done=%s failed=%s inserted_rows=%s)",
                    idx,
                    len(batch),
                    len(done_rows),
                    len(failed_rows),
                    inserted_total,
                )

        serialized_remaining = [json.dumps(item, ensure_ascii=False) for item in remaining]
        pending_file.write_text(
            "\n".join(serialized_remaining) + ("\n" if serialized_remaining else ""),
            encoding="utf-8",
        )

        moved_count = _move_processed_files(
            output_root=output_root,
            done_rows=done_rows,
            move_enabled=move_processed_files,
        )

        _append_jsonl(done_file, done_rows)
        _append_jsonl(failed_file, failed_rows)

        elapsed = time.perf_counter() - started
        summary = (
            f"Consumo concluido em {elapsed:.2f}s. "
            f"Lidas={len(raw_lines)} validas={len(valid_messages)} parse_fail={len(parse_failures)}. "
            f"Processadas={processed} done={len(done_rows)} failed={len(failed_rows)} inserted_rows={inserted_total}. "
            f"Arquivos_movidos={moved_count}. "
            f"Restantes={len(remaining)} em {pending_file}."
        )
        LOGGER.info(summary)
        return summary

    consume_pending_queue()


consume_brasileirao_queue_to_pg()
