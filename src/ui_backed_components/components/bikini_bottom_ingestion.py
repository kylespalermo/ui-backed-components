import csv
from pathlib import Path
from typing import Annotated, Literal

import dagster as dg
from dagster.components.resolved.form_config import ComponentFormConfig

DATA_DIR = Path(__file__).parent.parent / "data" / "bikini_bottom"

BikiniBottomTable = Literal[
    "characters",
    "locations",
    "menu_items",
    "krusty_krab_orders",
    "jellyfish_catches",
]

ALL_TABLES: list[BikiniBottomTable] = [
    "characters",
    "locations",
    "menu_items",
    "krusty_krab_orders",
    "jellyfish_catches",
]

TABLE_DESCRIPTIONS: dict[str, str] = {
    "characters": "Residents of Bikini Bottom, their species, jobs, and where they live.",
    "locations": "Homes, restaurants, and landmarks around Bikini Bottom.",
    "menu_items": "The Krusty Krab menu (plus one regrettable Chum Bucket entry).",
    "krusty_krab_orders": "Every order placed at the Krusty Krab in September 2026.",
    "jellyfish_catches": "Jellyfishing logbook: who caught what, where, and when.",
}


def _read_csv(table: str) -> tuple[list[str], list[dict[str, str]]]:
    with open(DATA_DIR / f"{table}.csv", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def _markdown_preview(columns: list[str], rows: list[dict[str, str]]) -> str:
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    body = ["| " + " | ".join(row[c] for c in columns) + " |" for row in rows]
    return "\n".join([header, divider, *body])


class BikiniBottomIngestion(dg.Component, dg.Model, dg.Resolvable):
    """Ingest tables from the Bikini Bottom demo dataset.

    Each selected table becomes an asset. Materializing it reads the source CSV
    and records the row count, column schema, and a preview of the first rows.
    """

    key_prefix: Annotated[
        str,
        dg.Resolver.default(
            description="Prefix for the asset keys, e.g. `bikini_bottom` -> `bikini_bottom/characters`.",
            form_config=ComponentFormConfig(
                label="Asset key prefix",
                placeholder="e.g. bikini_bottom",
                id_source=True,
            ),
        ),
    ]
    tables: Annotated[
        list[BikiniBottomTable],
        dg.Resolver.default(
            description="Which tables to ingest. Each one becomes an asset.",
            # Dagster only propagates scalar defaults into the form schema, so the
            # list default is declared here to pre-check every table in the form.
            json_schema_extra={
                "default": ALL_TABLES,
                "uniqueItems": True,
                "ui:widget": "checkboxes",
            },
            form_config=ComponentFormConfig(label="Tables to ingest"),
        ),
    ] = ALL_TABLES
    group_name: Annotated[
        str | None,
        dg.Resolver.default(
            description="Asset group for the ingested tables. Defaults to the key prefix.",
            form_config=ComponentFormConfig(label="Asset group", advanced=True),
        ),
    ] = None
    preview_rows: Annotated[
        int,
        dg.Resolver.default(
            description="How many rows to show in each materialization's preview.",
            form_config=ComponentFormConfig(label="Preview rows", advanced=True),
        ),
    ] = 5

    @classmethod
    def get_form_config(cls) -> ComponentFormConfig:
        return ComponentFormConfig(label="Bikini Bottom ingestion", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(assets=[self._build_asset(table) for table in self.tables])

    def _build_asset(self, table: str) -> dg.AssetsDefinition:
        preview_rows = self.preview_rows

        @dg.asset(
            key=dg.AssetKey([self.key_prefix, table]),
            group_name=self.group_name or self.key_prefix,
            description=TABLE_DESCRIPTIONS[table],
            kinds={"csv"},
        )
        def _ingest() -> dg.MaterializeResult:
            columns, rows = _read_csv(table)
            return dg.MaterializeResult(
                metadata={
                    "dagster/row_count": len(rows),
                    "dagster/column_schema": dg.TableSchema(
                        columns=[dg.TableColumn(name=c, type="string") for c in columns]
                    ),
                    "preview": dg.MetadataValue.md(
                        _markdown_preview(columns, rows[:preview_rows])
                    ),
                    "source_file": f"data/bikini_bottom/{table}.csv",
                }
            )

        return _ingest
