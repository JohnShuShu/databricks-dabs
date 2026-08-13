"""Notebook code scan surface.

Lists notebooks under a workspace path (recursively), exports each as SOURCE,
and scans the source line-by-line against the egress pattern catalog. This is
the only surface that catches `requests`-style network egress, since
system.access.outbound_network is empty in this account.

Incremental: notebooks whose ``modified_at`` is at or before the stored
watermark are skipped (mirrors the last_check_time watermark in github-monitor).
The caller decides the watermark; on a full scan it passes ``None``.

Robustness: a failure exporting/scanning a single notebook is logged and
skipped -- one bad notebook must not sink the whole run.
"""

from __future__ import annotations

import base64
from collections.abc import Iterator
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ObjectInfo, ObjectType

from .config import Settings
from .logging_setup import logging_setup
from .patterns import scan_line
from .records import NotebookFinding

logger = logging_setup(__name__)

# Databricks encodes cell boundaries as this comment line in exported SOURCE.
_CELL_MARKER = "# COMMAND ----------"
# `# MAGIC ` prefixes non-Python cell bodies (SQL/scala/sh/md) in a .py export;
# we strip it so the scan sees the real cell content.
_MAGIC_PREFIX = "# MAGIC "


def _ms_to_dt(ms: int | None) -> datetime | None:
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _iter_notebooks(ws: WorkspaceClient, root: str) -> Iterator[ObjectInfo]:
    """Yield every NOTEBOOK object under ``root`` (recursive)."""
    # workspace.list is recursive when passed recursive=True and paginates
    # internally, yielding ObjectInfo items.
    for obj in ws.workspace.list(root, recursive=True):
        if obj.object_type == ObjectType.NOTEBOOK:
            yield obj


def _decode_source(ws: WorkspaceClient, path: str) -> str:
    """Export a notebook as SOURCE and return decoded text."""
    resp = ws.workspace.export(path, format=ExportFormat.SOURCE)
    if not resp.content:
        return ""
    return base64.b64decode(resp.content).decode("utf-8", errors="replace")


def _scan_source(nb: ObjectInfo, source: str, snippet_chars: int) -> list[NotebookFinding]:
    """Scan decoded notebook source, tracking cell + line numbers."""
    findings: list[NotebookFinding] = []
    cell_no = 1
    modified = _ms_to_dt(nb.modified_at)
    language = nb.language.value if nb.language else "UNKNOWN"
    for line_no, raw in enumerate(source.splitlines(), start=1):
        if raw.strip() == _CELL_MARKER:
            cell_no += 1
            continue
        line = raw[len(_MAGIC_PREFIX):] if raw.startswith(_MAGIC_PREFIX) else raw
        for pat in scan_line(line):
            findings.append(
                NotebookFinding(
                    path=nb.path or "",
                    language=language,
                    object_id=nb.object_id or 0,
                    modified_at=modified,
                    cell_no=cell_no,
                    line_no=line_no,
                    category=pat.category,
                    pattern_name=pat.name,
                    severity=pat.severity,
                    snippet=line.strip()[:snippet_chars],
                )
            )
    return findings


def scan_notebooks(
    settings: Settings,
    since: datetime | None,
    ws: WorkspaceClient | None = None,
) -> list[NotebookFinding]:
    """Scan notebooks under ``settings.scan_root`` for egress patterns.

    ``since`` is the incremental watermark: notebooks modified at or before it
    are skipped. Pass ``None`` for a full scan.
    """
    ws = ws or WorkspaceClient()
    findings: list[NotebookFinding] = []
    scanned = skipped = failed = 0

    for nb in _iter_notebooks(ws, settings.scan_root):
        modified = _ms_to_dt(nb.modified_at)
        if since and modified and modified <= since:
            skipped += 1
            continue
        try:
            source = _decode_source(ws, nb.path or "")
            if len(source.encode("utf-8", errors="replace")) > settings.max_notebook_bytes:
                logger.warning("skipping oversized notebook %s (> %d bytes)", nb.path, settings.max_notebook_bytes)
                skipped += 1
                continue
            findings.extend(_scan_source(nb, source, settings.snippet_chars))
            scanned += 1
        except Exception as exc:  # noqa: BLE001 - isolate per-notebook failures
            failed += 1
            logger.warning("failed to scan notebook %s: %s", nb.path, exc)

    logger.info(
        "notebook scan: %d scanned, %d skipped, %d failed, %d findings",
        scanned, skipped, failed, len(findings),
    )
    return findings
