from typing import Annotated, Literal

import dagster as dg
from dagster.components.resolved.form_config import ComponentFormConfig


class RowCountCheck(dg.Component, dg.Model, dg.Resolvable):
    """Check that an asset's latest materialization has at least a minimum number of rows.

    Reads the row count the asset recorded on its latest materialization, so it
    works on any asset that reports `dagster/row_count` without touching the data.
    """

    asset_key: Annotated[
        str,
        dg.Resolver.default(
            description="The asset to check, as a slash-separated key.",
            form_config=ComponentFormConfig(
                label="Asset to check",
                placeholder="e.g. bikini_bottom/krusty_krab_orders",
                id_source=True,
            ),
        ),
    ]
    min_rows: Annotated[
        int,
        dg.Resolver.default(
            description="The check fails if the asset has fewer rows than this.",
            form_config=ComponentFormConfig(label="Minimum rows"),
        ),
    ] = 1
    severity: Annotated[
        Literal["WARN", "ERROR"],
        dg.Resolver.default(
            description="How serious a failure is. WARN flags it; ERROR marks the check as failed.",
            form_config=ComponentFormConfig(label="Severity when it fails"),
        ),
    ] = "WARN"
    blocking: Annotated[
        bool,
        dg.Resolver.default(
            description=(
                "If on, an ERROR failure stops downstream assets in the same run from materializing."
            ),
            form_config=ComponentFormConfig(label="Block downstream assets", advanced=True),
        ),
    ] = False

    @classmethod
    def get_form_config(cls) -> ComponentFormConfig:
        return ComponentFormConfig(label="Row count check", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        key = dg.AssetKey.from_user_string(self.asset_key)
        min_rows = self.min_rows
        severity = dg.AssetCheckSeverity(self.severity)

        @dg.asset_check(
            asset=key,
            name="min_row_count",
            description=f"Passes when the latest materialization has at least {min_rows} rows.",
            blocking=self.blocking,
        )
        def _check(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
            event = context.instance.get_latest_materialization_event(key)
            row_count = None
            if event and event.asset_materialization:
                entry = event.asset_materialization.metadata.get("dagster/row_count")
                row_count = entry.value if entry else None

            if row_count is None:
                return dg.AssetCheckResult(
                    passed=False,
                    severity=severity,
                    description="No row count found. Materialize the asset first.",
                    metadata={"min_rows": min_rows},
                )
            return dg.AssetCheckResult(
                passed=row_count >= min_rows,
                severity=severity,
                metadata={"row_count": row_count, "min_rows": min_rows},
            )

        return dg.Definitions(asset_checks=[_check])
