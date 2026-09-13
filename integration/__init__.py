"""Glue between the OneKey and Dataverse clients.

    OneKey workplace  (HCO) -> Dataverse account
    OneKey individual (HCP) -> Dataverse contact
"""

from .env import REPO_ROOT, load_env
from .mapping import (
    ACCOUNT_KEY_FIELD,
    CONTACT_KEY_FIELD,
    hco_to_account,
    hcp_to_contact,
)
from .sync import sync_sample, upsert_account, upsert_contact

__all__ = [
    "ACCOUNT_KEY_FIELD",
    "CONTACT_KEY_FIELD",
    "REPO_ROOT",
    "hco_to_account",
    "hcp_to_contact",
    "load_env",
    "sync_sample",
    "upsert_account",
    "upsert_contact",
]
