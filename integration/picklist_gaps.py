"""Track unmapped OneKey → Dataverse picklist codes during transform.

Unmapped values are omitted from payloads (quarantine). Operators extend
``value_mapping.csv``, run ``load_picklist_values``, then re-sync.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .env import log_dir

logger = logging.getLogger(__name__)


@dataclass
class PicklistGap:
    """One unmapped source code seen during transform."""

    mapping_type: str
    source_value: str
    target_field: str = ""
    target_entity: str = ""
    source_eid: str = ""
    transform_rule: str = ""


@dataclass
class PicklistGapCollector:
    """Deduplicated gap accumulator for a sync session."""

    gaps: list[PicklistGap] = field(default_factory=list)
    _seen: set[tuple[str, str, str]] = field(default_factory=set)

    def record(
        self,
        mapping_type: str,
        source_value: str,
        *,
        target_field: str = "",
        target_entity: str = "",
        source_eid: str = "",
        transform_rule: str = "",
    ) -> None:
        key = (mapping_type, str(source_value), target_field)
        if key in self._seen:
            return
        self._seen.add(key)
        gap = PicklistGap(
            mapping_type=mapping_type,
            source_value=str(source_value),
            target_field=target_field,
            target_entity=target_entity,
            source_eid=source_eid or "",
            transform_rule=transform_rule,
        )
        self.gaps.append(gap)
        logger.warning(
            "Picklist gap: type=%s source=%r field=%s.%s eid=%s — field omitted",
            mapping_type,
            source_value,
            target_entity,
            target_field,
            source_eid or "?",
        )

    def clear(self) -> None:
        self.gaps.clear()
        self._seen.clear()

    def write_report(self, path: Path | None = None) -> Path | None:
        """Write gaps to CSV. Returns path, or None if no gaps or nowhere to write.

        Each gap is already logged as a warning, so a missing report never fails
        the sync: SYNC_LOG_DIR=off skips it and a read-only directory is logged.
        """
        if not self.gaps:
            return None

        if path is None:
            logs = log_dir()
            if logs is None:
                logger.info("Picklist gap report skipped (SYNC_LOG_DIR is off)")
                return None
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            path = logs / f"picklist_gaps_{stamp}.csv"

        path = Path(path)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            fh = path.open("w", encoding="utf-8", newline="")
        except OSError as exc:
            logger.warning("Picklist gap report not written to %s: %s", path, exc)
            return None
        with fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "mapping_type",
                    "source_value",
                    "target_entity",
                    "target_field",
                    "source_eid",
                    "transform_rule",
                    "action",
                ],
            )
            writer.writeheader()
            for gap in self.gaps:
                writer.writerow(
                    {
                        "mapping_type": gap.mapping_type,
                        "source_value": gap.source_value,
                        "target_entity": gap.target_entity,
                        "target_field": gap.target_field,
                        "source_eid": gap.source_eid,
                        "transform_rule": gap.transform_rule,
                        "action": "add_to_value_mapping_csv_then_load_picklist_values",
                    }
                )
        logger.info("Wrote %d picklist gap(s) to %s", len(self.gaps), path)
        return path
