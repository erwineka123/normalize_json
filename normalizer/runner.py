from __future__ import annotations

import logging
import os
import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv
from httpx import HTTPError
from supabase import Client, PostgrestAPIError, create_client

from normalizer.transform import normalize_source_row

LOGGER = logging.getLogger("normalizer")
TARGET_BATCH_SIZE = 200


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Environment variable {name} is required")
    return value


def _supabase_project_url(name: str) -> str:
    value = _required_env(name)
    parsed = urlsplit(value)
    path = parsed.path.rstrip("/")

    if path.lower().endswith("/rest/v1"):
        LOGGER.warning(
            "%s contains the /rest/v1 API suffix; removing it (use the project URL)",
            name,
        )
        path = path[: -len("/rest/v1")]

    return urlunsplit((parsed.scheme, parsed.netloc, path, "", "")).rstrip("/")


def _positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


class SyncWorker:
    def __init__(
        self,
        source: Client,
        target: Client,
        *,
        source_table: str,
        batch_size: int,
        target_table: str,
        defaults: Mapping[str, str],
    ) -> None:
        self.source = source
        self.target = target
        self.source_table = source_table
        self.batch_size = batch_size
        self.target_table = target_table
        self.defaults = defaults
        self.last_id = 0

    def poll_once(self) -> None:
        response = (
            self.source.table(self.source_table)
            .select("id,content,created_at")
            .gt("id", self.last_id)
            .order("id")
            .limit(self.batch_size)
            .execute()
        )
        source_rows = response.data or []
        if not source_rows:
            return

        destination_rows: list[dict[str, Any]] = []
        source_ids: list[int] = []
        skipped = 0
        for source_row in source_rows:
            row_id = int(source_row["id"])
            source_ids.append(row_id)
            converted = normalize_source_row(
                source_row,
                default_file_name=self.defaults["file_name"],
                default_source_channel=self.defaults["source_channel"],
                default_status=self.defaults["status"],
            )
            if not converted:
                skipped += 1
                LOGGER.warning(
                    "Skipping source row id=%s: content has no recognized fields",
                    row_id,
                )
            destination_rows.extend(converted)

        for start in range(0, len(destination_rows), TARGET_BATCH_SIZE):
            batch = destination_rows[start : start + TARGET_BATCH_SIZE]
            (
                self.target.table(self.target_table)
                .upsert(batch, on_conflict="hash-256", ignore_duplicates=True)
                .execute()
            )

        # Advance only after every target write succeeds so a failed batch is retried.
        self.last_id = max(source_ids)
        LOGGER.info(
            "Processed %s source rows, prepared %s destination rows, skipped %s",
            len(source_rows),
            len(destination_rows),
            skipped,
        )


def build_worker() -> tuple[SyncWorker, int]:
    source = create_client(
        _supabase_project_url("SOURCE_SUPABASE_URL"),
        _required_env("SOURCE_SUPABASE_KEY"),
    )
    target = create_client(
        _supabase_project_url("TARGET_SUPABASE_URL"),
        _required_env("TARGET_SUPABASE_KEY"),
    )
    source_table = os.getenv("SOURCE_TABLE", "raw_leaks")
    worker = SyncWorker(
        source,
        target,
        source_table=source_table,
        batch_size=_positive_int("BATCH_SIZE", 500),
        target_table=os.getenv("TARGET_TABLE", "leaked_data"),
        defaults={
            "file_name": os.getenv("DEFAULT_FILE_NAME", source_table),
            "source_channel": os.getenv("DEFAULT_SOURCE_CHANNEL", source_table),
            "status": os.getenv("DEFAULT_STATUS", "new"),
        },
    )
    return worker, _positive_int("POLL_INTERVAL_SECONDS", 60)


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    worker, interval = build_worker()
    LOGGER.info(
        "Starting sync: source=%s target=%s poll_interval=%ss",
        worker.source_table,
        worker.target_table,
        interval,
    )

    while True:
        try:
            worker.poll_once()
        except (PostgrestAPIError, HTTPError):
            # Keep the in-memory position unchanged so the failed batch is retried.
            LOGGER.exception("Sync batch failed; it will be retried")
        time.sleep(interval)
