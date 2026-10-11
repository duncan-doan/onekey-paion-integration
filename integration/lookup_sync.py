"""Load OneKey code lists into Dataverse lookup tables.

The external id written to each row comes from
``lookup_table_mapping.csv`` (``external_id_template``), not from a pattern
hardcoded here. ``{codBase}.{list}.{code}`` produces ``WDE.SP.01``.

The name is the label in that country's language. OneKey keeps ``longLabel``
in English and returns the requested language in ``longLocalizedLabel``
(German ``Allgemeinmedizin`` for ``WDE`` specialty ``01``).
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dataverse.client import escape_odata
from dataverse.exceptions import DataverseHTTPError
from onekey.exceptions import OneKeyNotFoundError

from .env import REPO_ROOT, skip_unchanged

if TYPE_CHECKING:
    from dataverse import DataverseClient
    from onekey import OneKeyClient

    from .dynamic_mapping import LookupTableMapping, MappingConfig

logger = logging.getLogger(__name__)

# Territory → language requested from OneKey. See docs/PICKLIST_GUIDE.md.
CODEBASE_ISO_LANGUAGE = {
    "WUK": "en",
    "WDE": "de",
    "WNL": "nl",
    "WDK": "da",
    "WNO": "nb",
    "WSE": "sv",
    "WFI": "fi",
}


def render_external_id(
    template: str,
    *,
    cod_base: str,
    list_code: str,
    code: str,
) -> str | None:
    """Fill ``external_id_template`` from the mapping row and the code.

    Placeholders: ``{codBase}``, ``{list}``, ``{code}``.
    """
    try:
        rendered = template.format(
            codBase=(cod_base or "").strip(),
            list=(list_code or "").strip(),
            code=(code or "").strip(),
        )
    except (KeyError, IndexError, ValueError) as exc:
        logger.error(
            "Invalid external_id_template %r: %s",
            template,
            exc,
        )
        return None
    rendered = rendered.strip()
    return rendered or None


def odata_lookup_bind(entity_set: str, key_field: str, key_value: str) -> str:
    """Alternate-key bind, e.g. ``/ts_hcotype_refs(ts_externalid='WUK.TET.HOS')``."""
    escaped = str(key_value).replace("'", "''")
    return f"/{entity_set}({key_field}='{escaped}')"


def iso_language_for(cod_base: str, fallback: str = "en") -> str:
    """Language to request for a territory. Unknown territories use ``fallback``."""
    key = (cod_base or "").strip().upper()
    if key in CODEBASE_ISO_LANGUAGE:
        return CODEBASE_ISO_LANGUAGE[key]
    text = (fallback or "en").strip()
    return text or "en"


def code_export_dirs() -> list[Path]:
    """Directories that hold code-list exports."""
    dirs: list[Path] = []
    configured = os.environ.get("ONEKEY_CODE_DIR", "").strip()
    if configured:
        dirs.append(Path(configured))
    dirs.append(REPO_ROOT.parent / "output" / "codes")
    return dirs


# Parsed ``{codBase}_requested_lists.json`` bodies, keyed by file path.
_REQUESTED_LISTS: dict[str, dict] = {}


def requested_lists(directory: Path, cod_base: str) -> dict:
    """Lists inside ``{codBase}_requested_lists.json``, or an empty dict."""
    base = (cod_base or "").strip().upper()
    path = directory / f"{base}_requested_lists.json"
    key = str(path)
    if key in _REQUESTED_LISTS:
        return _REQUESTED_LISTS[key]
    if not path.is_file():
        _REQUESTED_LISTS[key] = {}
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("Code bundle %s could not be read: %s", path, exc)
        _REQUESTED_LISTS[key] = {}
        return {}
    lists = data.get("lists") if isinstance(data, dict) else None
    if not isinstance(lists, dict):
        _REQUESTED_LISTS[key] = {}
        return {}
    _REQUESTED_LISTS[key] = lists
    logger.info("Code bundle %s has %d lists in %s", base, len(lists), path)
    return lists


def load_code_export(cod_base: str, list_code: str) -> dict | None:
    """Read one list from the country bundle, then from ``{codBase}_{list}.json``."""
    base = (cod_base or "").strip().upper()
    code = (list_code or "").strip()
    for directory in code_export_dirs():
        envelope = requested_lists(directory, base).get(code)
        if isinstance(envelope, dict) and isinstance(envelope.get("codes"), list):
            logger.info(
                "Code list %s/%s loaded from %s_requested_lists.json",
                base,
                code,
                base,
            )
            return envelope
        path = directory / f"{base}_{code}.json"
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.error("Code export %s could not be read: %s", path, exc)
            return None
        if isinstance(data, dict):
            logger.info("Code list %s/%s loaded from %s", base, code, path)
            return data
    return None


# OneKey code property → Dataverse column and max length on the live catalogs.
CODE_COLUMNS = (
    ("longLabel", "ts_longlabel", 200),
    ("longLocalizedLabel", "ts_longlocalizedlabel", 200),
    ("shortLabel", "ts_shortlabel", 100),
    ("shortLocalizedLabel", "ts_shortlocalizedlabel", 100),
    ("isoLanguage", "ts_isolanguage", 10),
    ("localeEid", "ts_localeeid", 10),
    ("miCode", "ts_micode", 40),
    ("corporateEid", "ts_corporateeid", 40),
    ("corporateLongLabel", "ts_corporatelonglabel", 200),
    ("corporateLongLocalizedLabel", "ts_corporatelonglocalizedlabel", 200),
)


def _clip(value: object, limit: int) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if limit and len(text) > limit:
        return text[:limit]
    return text


def lookup_row(
    table: "LookupTableMapping",
    code: dict,
    cod_base: str,
) -> dict[str, str] | None:
    """Build one catalog row with every OneKey code field the table stores.

    ``ts_externalid`` is the rendered template (``WDE.SP.01``).
    ``ts_longlabel`` is the primary name. ``ts_longlocalizedlabel`` is the
    language requested for that country (German for ``WDE``).
    """
    local = str(code.get("externalId") or "").strip()
    if not local:
        return None
    base = (cod_base or code.get("codBase") or "").strip()
    external = render_external_id(
        table.external_id_template,
        cod_base=base,
        list_code=table.list_code,
        code=local,
    )
    if not external:
        return None
    external = _clip(external, 40)
    row: dict[str, str] = {table.key_field: external}
    for source, target, limit in CODE_COLUMNS:
        text = _clip(code.get(source), limit)
        if text:
            row[target] = text
    country = _clip(code.get("codBase") or base, 10)
    if country:
        row["ts_codbase"] = country
    if "ts_longlabel" not in row:
        row["ts_longlabel"] = _clip(code.get("longLocalizedLabel") or local, 200)
    # The live primary name is ts_longlabel. ts_name is not a column.
    return row


class CatalogResolver:
    """Insert a code-catalog row when a record lookup cannot find it.

    After the row exists, the lookup bind uses that row's id. Tables have no
    alternate key, so ``/ts_specialty_refs(ts_externalid='WDE.SP.01')`` is
    rejected. ``/ts_specialty_refs(<guid>)`` is not.
    """

    def __init__(self, dv):
        self.dv = dv
        self.dry_run = False
        self._ids: dict[tuple[str, str], str] = {}

    def preload(self, tables) -> None:
        """Remember codes already stored, so a later lookup does not insert them."""
        if not hasattr(self.dv, "retrieve_multiple"):
            return
        for table in tables:
            _, primary_id = _primary_id(table.entity_set)
            try:
                rows = self.dv.retrieve_multiple(
                    table.entity_set,
                    select=[table.key_field, primary_id],
                )
            except Exception as exc:  # noqa: BLE001 — one table must not stop the rest
                logger.error("Could not read %s codes: %s", table.entity_set, exc)
                continue
            for row in rows or []:
                external = str(row.get(table.key_field) or "").strip()
                record_id = str(row.get(primary_id) or "").strip()
                if external and record_id:
                    self._ids[(table.entity_set, external)] = record_id

    def ensure(self, table, external: str, code: str, cod_base: str, source=None) -> str:
        """Return a lookup bind, creating the catalog row when it is missing."""
        key = (table.entity_set, external)
        record_id = self._ids.get(key)
        if record_id:
            return f"/{table.entity_set}({record_id})"
        if self.dry_run or not hasattr(self.dv, "upsert_by_field"):
            return odata_lookup_bind(table.entity_set, table.key_field, external)

        row = lookup_row(
            table,
            {
                "externalId": code,
                "codBase": cod_base,
                "longLabel": code,
                "longLocalizedLabel": code,
                "isoLanguage": iso_language_for(cod_base),
            },
            cod_base,
        )
        if not row:
            return odata_lookup_bind(table.entity_set, table.key_field, external)
        # A row the preload did not see (its read failed, or another country
        # added it since) keeps its proper label: the stub row carries the raw
        # code as its label, so it is only written when the code is new.
        _, primary_id = _primary_id(table.entity_set)
        existing = None
        if hasattr(self.dv, "find_one"):
            existing = self.dv.find_one(
                table.entity_set,
                filter=f"{table.key_field} eq '{escape_odata(external)}'",
                select=[primary_id],
            )
        if existing and existing.get(primary_id):
            record_id, created = existing[primary_id], False
        else:
            record_id, created = self.dv.upsert_by_field(
                table.entity_set,
                table.key_field,
                external,
                row,
            )
        self._ids[key] = str(record_id)
        logger.info(
            "Catalog %s %s %s",
            "inserted" if created else "found",
            table.list_code,
            external,
        )
        return f"/{table.entity_set}({record_id})"


def _primary_id(entity_set: str) -> tuple[str, str]:
    from dataverse.client import entity_meta

    return entity_meta(entity_set)


def _codes_for(
    onekey: "OneKeyClient",
    cod_base: str,
    table: "LookupTableMapping",
) -> list[dict]:
    language = iso_language_for(cod_base, table.iso_language)
    try:
        codes = onekey.get_code_list(
            cod_base,
            table.list_code,
            iso_language=language,
        )
    except OneKeyNotFoundError:
        # UAT does not publish /ok/code. Saved exports use the file envelope.
        envelope = load_code_export(cod_base, table.list_code)
        if envelope is None:
            raise
        codes = envelope.get("codes") if isinstance(envelope, dict) else None
    if not isinstance(codes, list):
        return []
    return [code for code in codes if isinstance(code, dict)]


def sync_lookup_tables(
    dv: "DataverseClient",
    onekey: "OneKeyClient",
    config: "MappingConfig",
    codebases: list[str],
    *,
    dry_run: bool = False,
    tables: list[str] | None = None,
) -> list[dict]:
    """Upsert code-list rows for each configured lookup table.

    ``codebases`` supplies ``{codBase}`` (for example ``WUK``). One OneKey
    ``/ok/searchCodeList`` call is made per codebase and list.
    """
    selected = {
        item.strip().lower()
        for item in (tables or [])
        if item and item.strip()
    }
    summaries: list[dict] = []
    bases = [cb.strip() for cb in codebases if cb and str(cb).strip()]
    if not bases:
        logger.info("Lookup tables skipped: no codebases")
        return summaries
    if not config.lookup_tables:
        logger.info("Lookup tables skipped: lookup_table_mapping.csv is empty")
        return summaries

    for table in config.lookup_tables:
        if selected and table.lookup_id.lower() not in selected and table.list_code.lower() not in selected:
            continue
        records: list[dict[str, Any]] = []
        fetch_errors = 0
        for cod_base in bases:
            try:
                codes = _codes_for(onekey, cod_base, table)
            except Exception as exc:  # noqa: BLE001 — one list must not stop the rest
                fetch_errors += 1
                logger.error(
                    "Code list %s/%s failed: %s",
                    cod_base,
                    table.list_code,
                    exc,
                )
                continue
            for code in codes:
                row = lookup_row(table, code, cod_base)
                if row:
                    records.append(row)

        summary = {
            "mapping_type": table.lookup_id,
            "target": table.entity_set,
            "kind": "lookup",
            "csv": len(records),
            "dataverse": 0,
            "insert": 0,
            "update": 0,
            "unchanged": 0,
            "errors": fetch_errors,
            "skipped": False,
        }
        logger.info(
            "Lookup %s → %s (%s): codes=%d codebases=%s",
            table.list_code,
            table.entity_set,
            table.external_id_template,
            len(records),
            ",".join(bases),
        )
        if dry_run or not records:
            summary["insert"] = 0 if not dry_run else len(records)
            summaries.append(summary)
            continue
        if not hasattr(dv, "bulk_upsert_by_field"):
            logger.warning(
                "Lookup %s skipped: Dataverse client has no bulk upsert",
                table.lookup_id,
            )
            summary["skipped"] = True
            summary["errors"] += 1
            summaries.append(summary)
            continue
        try:
            unchanged: set = set()
            written = dv.bulk_upsert_by_field(
                table.entity_set,
                table.key_field,
                records,
                skip_unchanged=skip_unchanged(),
                skipped=unchanged,
            )
            summary["insert"] = sum(1 for _key, _id, created in written if created)
            summary["unchanged"] = len(unchanged)
            summary["update"] = sum(
                1 for key, _id, created in written if not created and key not in unchanged
            )
            summary["dataverse"] = len(written)
        except DataverseHTTPError as exc:
            logger.error("Lookup upsert %s failed: %s", table.entity_set, exc)
            summary["errors"] += 1
        summaries.append(summary)
    return summaries
