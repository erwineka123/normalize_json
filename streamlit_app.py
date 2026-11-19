from __future__ import annotations

import logging
import os
import queue
import threading
from collections import deque
from collections.abc import Mapping

import streamlit as st
from dotenv import load_dotenv

from normalizer.runner import LOGGER, SyncWorker, build_worker, run_worker

CONFIG_KEYS = (
    "SOURCE_SUPABASE_URL",
    "SOURCE_SUPABASE_KEY",
    "TARGET_SUPABASE_URL",
    "TARGET_SUPABASE_KEY",
    "SOURCE_TABLE",
    "TARGET_TABLE",
    "POLL_INTERVAL_SECONDS",
    "BATCH_SIZE",
    "DEFAULT_FILE_NAME",
    "DEFAULT_SOURCE_CHANNEL",
    "DEFAULT_STATUS",
    "LOG_LEVEL",
)
LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
MAX_LOG_LINES = 500


class _QueueLogHandler(logging.Handler):
    def __init__(self, records: queue.Queue[str]) -> None:
        super().__init__()
        self.records = records

    def emit(self, record: logging.LogRecord) -> None:
        self.records.put(self.format(record))


class _CurrentThreadFilter(logging.Filter):
    def __init__(self) -> None:
        super().__init__()
        self.thread_id = threading.get_ident()

    def filter(self, record: logging.LogRecord) -> bool:
        return record.thread == self.thread_id


def _streamlit_config() -> dict[str, str]:
    load_dotenv()
    config = dict(os.environ)
    secrets: Mapping[str, object] = st.secrets
    config.update(
        {
            key: str(secrets[key])
            for key in CONFIG_KEYS
            if key in secrets
        }
    )
    log_level = config.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=log_level, format=LOG_FORMAT)
    LOGGER.setLevel(log_level)
    return config


def _run_in_background(
    worker: SyncWorker,
    interval: int,
    stop_event: threading.Event,
    records: queue.Queue[str],
) -> None:
    handler = _QueueLogHandler(records)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    handler.addFilter(_CurrentThreadFilter())
    LOGGER.addHandler(handler)
    try:
        run_worker(worker, interval, stop_event)
    except Exception:
        LOGGER.exception("Sync worker stopped unexpectedly")
    finally:
        LOGGER.removeHandler(handler)


def _start_worker() -> None:
    try:
        worker, interval = build_worker(_streamlit_config())
    except Exception as error:
        LOGGER.exception("Could not start sync worker")
        st.error(f"Tidak dapat memulai worker: {error}")
        return

    stop_event = threading.Event()
    records: queue.Queue[str] = queue.Queue()
    thread = threading.Thread(
        target=_run_in_background,
        args=(worker, interval, stop_event, records),
        name="normalizer-sync-worker",
        daemon=True,
    )
    st.session_state.stop_event = stop_event
    st.session_state.log_queue = records
    st.session_state.log_lines = deque(maxlen=MAX_LOG_LINES)
    st.session_state.worker_thread = thread
    thread.start()


@st.fragment(run_every=1)
def _show_worker_status() -> None:
    records: queue.Queue[str] | None = st.session_state.get("log_queue")
    lines: deque[str] = st.session_state.get(
        "log_lines", deque(maxlen=MAX_LOG_LINES)
    )
    if records is not None:
        while True:
            try:
                lines.append(records.get_nowait())
            except queue.Empty:
                break
    st.session_state.log_lines = lines

    thread: threading.Thread | None = st.session_state.get("worker_thread")
    stop_event: threading.Event | None = st.session_state.get("stop_event")
    if thread is not None and thread.is_alive():
        if stop_event is not None and stop_event.is_set():
            st.info("Worker sedang berhenti setelah proses yang berjalan selesai.")
        else:
            st.success("Worker berjalan.")
    else:
        st.info("Worker tidak berjalan.")

    st.subheader("Log")
    st.code("\n".join(lines) if lines else "Belum ada log.", language="text")


def main() -> None:
    st.set_page_config(page_title="Normalizer Worker")
    st.title("Supabase Normalizer Worker")
    st.write("Jalankan atau hentikan worker sinkronisasi dan pantau log-nya.")

    thread: threading.Thread | None = st.session_state.get("worker_thread")
    is_running = thread is not None and thread.is_alive()
    start_column, stop_column = st.columns(2)
    with start_column:
        if st.button("Start", disabled=is_running, use_container_width=True):
            _start_worker()
    with stop_column:
        if st.button("Stop", disabled=not is_running, use_container_width=True):
            stop_event: threading.Event | None = st.session_state.get("stop_event")
            if stop_event is not None:
                stop_event.set()

    _show_worker_status()


if __name__ == "__main__":
    main()
