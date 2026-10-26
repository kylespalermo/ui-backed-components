from typing import Annotated

import dagster as dg
from dagster.components.resolved.form_config import ComponentFormConfig, FormWidget


class AssetSchedule(dg.Component, dg.Model, dg.Resolvable):
    """Materialize a selection of assets on a cron schedule.

    Any asset checks on the selected assets run along with them.
    """

    name: Annotated[
        str,
        dg.Resolver.default(
            description="A unique name for the schedule. Letters, numbers, and underscores only.",
            form_config=ComponentFormConfig(
                label="Schedule name",
                placeholder="e.g. bikini_bottom_hourly",
                id_source=True,
            ),
        ),
    ]
    selection: Annotated[
        str,
        dg.Resolver.default(
            description=(
                "Which assets to refresh, in asset selection syntax. Examples: "
                '`group:bikini_bottom`, `key:"bikini_bottom/characters"`, '
                '`key:"bikini_bottom/*"`.'
            ),
            form_config=ComponentFormConfig(
                label="Assets to refresh",
                placeholder="e.g. group:bikini_bottom",
            ),
        ),
    ]
    cron_schedule: Annotated[
        str,
        dg.Resolver.default(
            description="When to run.",
            form_config=ComponentFormConfig(label="Schedule", widget=FormWidget.CRON),
        ),
    ] = "0 * * * *"
    execution_timezone: Annotated[
        str,
        dg.Resolver.default(
            description="Timezone for the schedule, e.g. `America/Los_Angeles`.",
            form_config=ComponentFormConfig(label="Timezone", advanced=True),
        ),
    ] = "UTC"
    start_running: Annotated[
        bool,
        dg.Resolver.default(
            description=(
                "If on, the schedule starts running as soon as it's created. "
                "If off, turn it on from the schedule's page."
            ),
            form_config=ComponentFormConfig(label="Turn on immediately"),
        ),
    ] = False

    @classmethod
    def get_form_config(cls) -> ComponentFormConfig:
        return ComponentFormConfig(label="Refresh schedule", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        job = dg.define_asset_job(
            name=f"{self.name}_job",
            selection=dg.AssetSelection.from_string(self.selection),
        )
        schedule = dg.ScheduleDefinition(
            name=self.name,
            job=job,
            cron_schedule=self.cron_schedule,
            execution_timezone=self.execution_timezone,
            default_status=(
                dg.DefaultScheduleStatus.RUNNING
                if self.start_running
                else dg.DefaultScheduleStatus.STOPPED
            ),
        )
        return dg.Definitions(jobs=[job], schedules=[schedule])
