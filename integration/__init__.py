"""Glue between the OneKey and Dataverse clients.

    OneKey workplace  (HCO) -> Dataverse account
    OneKey individual (HCP) -> Dataverse contact
    
This module provides both:
1. Original hardcoded mapping (mapping.py, sync.py) for backward compatibility
2. New dynamic CSV-based mapping (dynamic_mapping.py, dynamic_sync.py) for flexibility

For new implementations, prefer the dynamic mapping approach:
    from integration import create_sync_engine, load_mapping_config
"""

from .env import REPO_ROOT, load_env

# Original hardcoded mapping (backward compatible)
from .mapping import (
    ACCOUNT_KEY_FIELD,
    CONTACT_KEY_FIELD,
    hco_to_account,
    hcp_to_contact,
)
from .sync import sync_sample, upsert_account, upsert_contact

# New dynamic mapping system
from .dynamic_mapping import (
    load_mapping_config,
    MappingConfig,
    FieldMapping,
    RelationshipMapping,
    EntityDefinition,
    ValueMapping,
    FieldConstraint,
)
from .validation import (
    MappingValidator,
    DataverseValidator,
    ValidationResult,
    ValidationError,
)
from .transform import TransformEngine, create_transform_engine
from .dynamic_sync import DynamicSyncEngine, create_sync_engine
from .sync_logger import SyncLogger, setup_logging
from .job_trace import JobTracer
from .picklist_gaps import PicklistGapCollector
from .picklist_sync import sync_picklists_from_config
from .hierarchy import edges_from_hcos
from .w2w_relations import collect_w2w_relations, normalize_hco_hco_edge

__all__ = [
    # Environment
    "REPO_ROOT",
    "load_env",
    
    # Original hardcoded mapping (backward compatible; keys from CSV)
    "ACCOUNT_KEY_FIELD",
    "CONTACT_KEY_FIELD",
    "hco_to_account",
    "hcp_to_contact",
    "sync_sample",
    "upsert_account",
    "upsert_contact",
    
    # Dynamic mapping configuration
    "load_mapping_config",
    "MappingConfig",
    "FieldMapping",
    "RelationshipMapping",
    "EntityDefinition",
    "ValueMapping",
    "FieldConstraint",
    
    # Validation
    "MappingValidator",
    "DataverseValidator",
    "ValidationResult",
    "ValidationError",
    
    # Transformation
    "TransformEngine",
    "create_transform_engine",
    
    # Dynamic sync engine
    "DynamicSyncEngine",
    "create_sync_engine",
    
    # Logging
    "SyncLogger",
    "setup_logging",
    "JobTracer",

    # Migration helpers
    "PicklistGapCollector",
    "sync_picklists_from_config",
    "edges_from_hcos",
    "collect_w2w_relations",
    "normalize_hco_hco_edge",
]
