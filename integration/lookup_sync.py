"""Load OneKey code lists into Dataverse lookup tables.

The external id written to each row comes from
``lookup_table_mapping.csv`` (``external_id_template``), not from a pattern
hardcoded here. ``{codBase}.{list}.{code}`` produces ``WUK.TET.HOS``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from dataverse.exceptions import DataverseHTTPError

if TYPE_CHECKING:
    from dataverse import DataverseClient
    from onekey import OneKeyClient

    from .dynamic_mapping import LookupTableMapping, MappingConfig

logger = logging.getLogger(__name__)


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


def lookup_row(
    table: "LookupTableMapping",
    code: dict,
    cod_base: str,
) -> dict[str, str] | None:
    """Build one lookup-table payload. ``ts_externalid`` is the rendered template."""
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
    label = str(code.get(table.label_field) or "").strip() or local
    if table.name_max_length and len(label) > table.name_max_length:
        label = label[: table.name_max_length]
    return {
        table.key_field: external,
        table.name_field: label,
    }


def _codes_for(
    onekey: "OneKeyClient",
    cod_base: str,
    table: "LookupTableMapping",
) -> list[dict]:
    envelope = onekey.get_code_list(
        cod_base,
        table.list_code,
        iso_language=table.iso_language,
    )
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
    code call is made per codebase and list.
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
            written = dv.bulk_upsert_by_field(
                table.entity_set,
                table.key_field,
                records,
            )
            summary["insert"] = sum(1 for _key, _id, created in written if created)
            summary["update"] = sum(1 for _key, _id, created in written if not created)
            summary["dataverse"] = len(written)
        except DataverseHTTPError as exc:
            logger.error("Lookup upsert %s failed: %s", table.entity_set, exc)
            summary["errors"] += 1
        summaries.append(summary)
    return summaries
