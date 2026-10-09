"""Lookup-table external ids come from lookup_table_mapping.csv."""

from integration.dynamic_mapping import LookupTableMapping, load_mapping_config
from integration.lookup_sync import lookup_row, render_external_id, sync_lookup_tables
from integration.transform import create_transform_engine
from onekey.parse import flatten_hco


def test_template_builds_full_code():
    assert (
        render_external_id(
            "{codBase}.{list}.{code}",
            cod_base="WUK",
            list_code="TET",
            code="HOS",
        )
        == "WUK.TET.HOS"
    )


def test_lookup_row_ignores_onekey_uid():
    table = LookupTableMapping(
        lookup_id="LK-TET",
        list_code="TET",
        lookup_entity="ts_hcotype_ref",
        entity_set="ts_hcotype_refs",
        key_field="ts_externalid",
        name_field="ts_name",
        external_id_template="{code}|{list}|{codBase}",
        label_field="longLabel",
    )
    row = lookup_row(
        table,
        {
            "uid": "WUK.TET.HOS",
            "externalId": "HOS",
            "longLabel": "Hospital",
        },
        "WUK",
    )
    assert row == {"ts_externalid": "HOS|TET|WUK", "ts_name": "Hospital"}


def test_configured_template_is_codbase_list_code():
    config = load_mapping_config()
    table = config.get_lookup_table("ts_specialty_ref")
    assert table is not None
    assert table.list_code == "SP"
    assert table.external_id_template == "{codBase}.{list}.{code}"
    assert (
        render_external_id(
            table.external_id_template,
            cod_base="WDE",
            list_code=table.list_code,
            code="01",
        )
        == "WDE.SP.01"
    )


def test_sync_dry_run_uses_template_counts():
    class FakeOneKey:
        def get_code_list(self, cod_base, list_external_id, iso_language="en"):
            assert list_external_id == "TIT"
            assert cod_base == "WUK"
            return [
                {
                    "uid": "NOT-USED",
                    "externalId": "DR",
                    "longLabel": "Doctor",
                }
            ]

    summaries = sync_lookup_tables(
        None,
        FakeOneKey(),
        load_mapping_config(),
        ["WUK"],
        dry_run=True,
        tables=["LK-TIT"],
    )
    assert len(summaries) == 1
    assert summaries[0]["mapping_type"] == "LK-TIT"
    assert summaries[0]["target"] == "ts_title_refs"
    assert summaries[0]["insert"] == 1
    assert summaries[0]["errors"] == 0


def test_contact_title_bind():
    engine = create_transform_engine(load_mapping_config())
    payload = engine.transform_hcp(
        {
            "individualEid": "WUKI1",
            "lastName": "Smith",
            "codBase": "WUK",
            "title": "DR",
        }
    )
    assert (
        payload["ts_title_ref@odata.bind"]
        == "/ts_title_refs(ts_externalid='WUK.TIT.DR')"
    )


def test_flatten_hco_legal_category():
    row = flatten_hco(
        {
            "codBase": "WUK",
            "workplace": {
                "workplaceEid": "WUKH1",
                "officialName": "Practice",
                "ada": {"CAJ,1": {"listCode": "CAJ", "rank": 1, "code": "PRV"}},
            },
        }
    )
    assert row["legalCategoryCode"] == "PRV"
    assert row["codBase"] == "WUK"
