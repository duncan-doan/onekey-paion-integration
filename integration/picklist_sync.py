"""Pre-sync upsert of Dataverse choice options from value_mapping.csv.

Catalog-driven only: never invent option integers from live OneKey feed.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import TYPE_CHECKING

from dataverse.exceptions import DataverseHTTPError

if TYPE_CHECKING:
    from dataverse import DataverseClient
    from .dynamic_mapping import MappingConfig

logger = logging.getLogger(__name__)

LANG = 1033


def label_payload(text: str) -> dict:
    return {
        "LocalizedLabels": [{"Label": text, "LanguageCode": LANG}],
        "UserLocalizedLabel": {"Label": text, "LanguageCode": LANG},
    }


def csv_options(config: "MappingConfig", mapping_type: str) -> dict[int, str]:
    out: dict[int, str] = {}
    for vm in config.value_mappings:
        if vm.mapping_type != mapping_type:
            continue
        try:
            value = int(vm.target_value)
        except (TypeError, ValueError):
            logger.warning(
                "Skip %s/%s: non-integer target_value %r",
                mapping_type,
                vm.source_code,
                vm.target_value,
            )
            continue
        out[value] = (vm.target_label or vm.source_value or str(value)).strip()
    return out


def load_global_options(dv: "DataverseClient", name: str) -> dict[int, str]:
    meta = dv.request("GET", f"GlobalOptionSetDefinitions(Name='{name}')").json()
    return {
        int(o["Value"]): (
            ((o.get("Label") or {}).get("UserLocalizedLabel") or {}).get("Label") or ""
        ).strip()
        for o in meta.get("Options") or []
    }


def load_local_options(
    dv: "DataverseClient", entity: str, attribute: str
) -> dict[int, str]:
    meta = dv.request(
        "GET",
        f"EntityDefinitions(LogicalName='{entity}')"
        f"/Attributes(LogicalName='{attribute}')"
        f"/Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$expand=OptionSet",
    ).json()
    return {
        int(o["Value"]): (
            ((o.get("Label") or {}).get("UserLocalizedLabel") or {}).get("Label") or ""
        ).strip()
        for o in (meta.get("OptionSet") or {}).get("Options") or []
    }


def build_insert_body(target: dict, value: int, label: str) -> dict:
    body = {"Value": value, "Label": label_payload(label)}
    if target["kind"] == "global":
        body["OptionSetName"] = target["option_set"]
    else:
        body["EntityLogicalName"] = target["entity"]
        body["AttributeLogicalName"] = target["attribute"]
    return body


def build_update_body(target: dict, value: int, label: str) -> dict:
    body = build_insert_body(target, value, label)
    body["MergeLabels"] = True
    return body


def post_action(dv: "DataverseClient", action: str, body: dict) -> None:
    dv.request("POST", action, json=body)


def run_batch(dv: "DataverseClient", operations: list[tuple[str, dict]]) -> list[str]:
    if not operations:
        return []

    batch_id = f"batch_{uuid.uuid4().hex}"
    changeset_id = f"changeset_{uuid.uuid4().hex}"
    lines = [
        f"--{batch_id}",
        f"Content-Type: multipart/mixed;boundary={changeset_id}",
        "",
    ]
    for index, (action, body) in enumerate(operations, start=1):
        lines.extend(
            [
                f"--{changeset_id}",
                "Content-Type: application/http",
                "Content-Transfer-Encoding: binary",
                f"Content-ID: {index}",
                "",
                f"POST {dv.config.api_url}/{action} HTTP/1.1",
                "Content-Type: application/json;type=entry",
                "",
                json.dumps(body, ensure_ascii=False),
                "",
            ]
        )
    lines.extend([f"--{changeset_id}--", f"--{batch_id}--", ""])
    payload = "\r\n".join(lines)

    response = dv.request(
        "POST",
        "$batch",
        data=payload.encode("utf-8"),
        headers={"Content-Type": f"multipart/mixed;boundary={batch_id}"},
    )
    text = response.text
    errors = []
    if '"code"' in text and (
        "error" in text.lower() or "HTTP/1.1 4" in text or "HTTP/1.1 5" in text
    ):
        for chunk in text.split("--"):
            if "HTTP/1.1 4" in chunk or "HTTP/1.1 5" in chunk or '"error"' in chunk:
                errors.append(chunk.strip()[:400])
    return errors


def sync_type(
    dv: "DataverseClient",
    mapping_type: str,
    target: dict,
    desired: dict[int, str],
    *,
    dry_run: bool,
    use_batch: bool = True,
    batch_size: int = 50,
) -> dict:
    try:
        if target["kind"] == "global":
            current = load_global_options(dv, target["option_set"])
            target_label = target["option_set"]
        else:
            current = load_local_options(dv, target["entity"], target["attribute"])
            target_label = f"{target['entity']}.{target['attribute']}"
    except DataverseHTTPError as exc:
        logger.warning(
            "Picklist %s target missing in Dataverse (%s) — skipped",
            mapping_type,
            exc,
        )
        return {
            "mapping_type": mapping_type,
            "target": str(target),
            "csv": len(desired),
            "dataverse": 0,
            "insert": 0,
            "update": 0,
            "unchanged": 0,
            "errors": 1,
            "skipped": True,
        }

    to_insert = []
    to_update = []
    for value, label in sorted(desired.items()):
        if value not in current:
            to_insert.append((value, label))
        elif (current[value] or "").strip() != label:
            to_update.append((value, label, current[value]))

    summary = {
        "mapping_type": mapping_type,
        "target": target_label,
        "csv": len(desired),
        "dataverse": len(current),
        "insert": len(to_insert),
        "update": len(to_update),
        "unchanged": len(desired) - len(to_insert) - len(to_update),
        "errors": 0,
        "skipped": False,
    }

    logger.info(
        "Picklist %s → %s: csv=%d dv=%d insert=%d update=%d unchanged=%d",
        mapping_type,
        target_label,
        summary["csv"],
        summary["dataverse"],
        summary["insert"],
        summary["update"],
        summary["unchanged"],
    )

    if dry_run or (not to_insert and not to_update):
        return summary

    ops: list[tuple[str, dict]] = []
    for value, label in to_insert:
        ops.append(("InsertOptionValue", build_insert_body(target, value, label)))
    for value, label, _old in to_update:
        ops.append(("UpdateOptionValue", build_update_body(target, value, label)))

    if use_batch:
        from dataverse.client import chunked

        for batch in chunked(ops, batch_size):
            errors = run_batch(dv, batch)
            if errors:
                summary["errors"] += len(errors)
                for action, body in batch:
                    try:
                        post_action(dv, action, body)
                        summary["errors"] = max(0, summary["errors"] - 1)
                    except DataverseHTTPError as exc:
                        logger.error("FAIL %s %s: %s", action, body.get("Value"), exc)
                        summary["errors"] += 1
    else:
        for action, body in ops:
            try:
                post_action(dv, action, body)
            except DataverseHTTPError as exc:
                logger.error("FAIL %s %s: %s", action, body.get("Value"), exc)
                summary["errors"] += 1

    return summary


def sync_picklists_from_config(
    dv: "DataverseClient",
    config: "MappingConfig",
    *,
    types: list[str] | None = None,
    dry_run: bool = False,
    use_batch: bool = True,
    batch_size: int = 50,
    targets: dict | None = None,
) -> list[dict]:
    """Upsert choice options for configured mapping types. Returns summaries.

    Targets come from ``config/picklist_targets.csv`` (via MappingConfig)
    unless ``targets`` is passed explicitly.
    """
    target_map = targets if targets is not None else config.picklist_target_map()
    if not target_map:
        logger.warning(
            "No picklist targets configured (picklist_targets.csv) — skipping"
        )
        return []
    selected = types or list(target_map.keys())
    summaries = []
    for mapping_type in selected:
        if mapping_type not in target_map:
            logger.warning("Unknown picklist mapping type %r — skipped", mapping_type)
            continue
        desired = csv_options(config, mapping_type)
        if not desired:
            logger.info("Picklist %s: no CSV rows — skipped", mapping_type)
            continue
        summaries.append(
            sync_type(
                dv,
                mapping_type,
                target_map[mapping_type],
                desired,
                dry_run=dry_run,
                use_batch=use_batch,
                batch_size=batch_size,
            )
        )
    return summaries
