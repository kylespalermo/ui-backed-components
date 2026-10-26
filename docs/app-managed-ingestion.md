# Bikini Bottom ingestion: an app-managed component

This project ships one **app-managed component**: a component type that the
Dagster+ UI can create instances of from a form, with no YAML or Python
written by hand. It ingests a small SpongeBob-themed dataset and turns each
table into an asset.

There are no component instances in this repo. That's on purpose: the first
instance is meant to be created through the UI form.

Getting from an empty project to "this shows up in the UI's picker" took
three steps:

1. **Add the data:** five CSV files bundled with the project.
2. **Write the component:** a Python class that opts in to UI editing.
3. **Register the component:** an entry point in `pyproject.toml`, so Dagster
   advertises the type to the UI.

Each step is covered below, then what to expect when you fill in the form.

---

## Step 1: The data

The files live in `src/ui_backed_components/data/bikini_bottom/`. They are
packaged with the code, so they deploy with it and need no external database
or credentials.

| Table                | Rows | What's in it                                                                    |
| -------------------- | ---: | ------------------------------------------------------------------------------- |
| `characters`         |   16 | SpongeBob, Patrick, Squidward, Mr. Krabs, Plankton, Sandy, and friends           |
| `locations`          |   12 | The Krusty Krab, the Chum Bucket, the Pineapple, Jellyfish Fields, Goo Lagoon…   |
| `menu_items`         |   15 | The Krusty Krab menu with prices (and one Chum Burger)                           |
| `krusty_krab_orders` |  240 | A month of orders: who ordered what, how many, and when                         |
| `jellyfish_catches`  |  120 | The jellyfishing logbook: who caught which jellyfish, where, and when           |

The tables relate the way a real source system's would: orders point at
characters and menu items, catches point at characters and locations, and
characters point at their home location. That leaves room for interesting
downstream assets and checks later.

## Step 2: The component

The file is `src/ui_backed_components/components/bikini_bottom_ingestion.py`.

`BikiniBottomIngestion` is an ordinary Dagster component. Given a few settings,
it produces one asset per selected table. When an asset materializes, it reads
its CSV and records metadata in Dagster:

- **Row count:** shown as the asset's row count in the UI.
- **Column schema:** the column names.
- **Preview:** the first few rows as a table.
- **Source file:** which CSV it came from.

### What makes it app-managed

Two pieces of the code are specific to the UI form.

**1. The class opts in to UI editing:**

```python
@classmethod
def get_form_config(cls) -> ComponentFormConfig:
    return ComponentFormConfig(label="Bikini Bottom ingestion", editable=True)
```

`editable=True` makes the type appear in the UI's "Add component instance"
picker. `label` is the name the picker shows. Without this override, a
component can still be used from YAML but never appears in the UI.

**2. Each field carries hints for how the form renders it.** These are passed
as `form_config=ComponentFormConfig(...)` on each field. Here is the form they
produce:

| Form field           | Required? | Default        | Form behavior                                                                         |
| -------------------- | --------- | -------------- | ------------------------------------------------------------------------------------- |
| **Asset key prefix** | Yes       | —              | Text box with placeholder `e.g. bikini_bottom`. Its value is also used as the component's ID. |
| **Tables to ingest** | No        | All five       | One checkbox per table, all checked to start                                         |
| **Asset group**      | No        | The key prefix | Under the collapsed **Advanced** section                                              |
| **Preview rows**     | No        | 5              | Under the collapsed **Advanced** section                                              |

For example, if you enter `bikini_bottom` as the prefix and leave everything
else alone, you get five assets: `bikini_bottom/characters`,
`bikini_bottom/locations`, and so on, all in an asset group called
`bikini_bottom`.

## Step 3: Register the component

**This is the step that's easy to miss.** The UI only lists component types
that Python packages advertise through a specific entry point. The
`registry_modules` setting under `[tool.dg.project]` in `pyproject.toml` is
**not** enough: that setting only feeds the `dg` command-line tool.

Registration has two parts:

**a) Export the class from the components package.**
`src/ui_backed_components/components/__init__.py` imports
`BikiniBottomIngestion`, so it's visible at
`ui_backed_components.components`.

**b) Declare the entry point in `pyproject.toml`:**

```toml
[project.entry-points."dagster_dg_cli.registry_modules"]
ui_backed_components = "ui_backed_components.components"
```

This tells Dagster to scan `ui_backed_components.components` for component
types. When the code location loads, Dagster records every registered type in
the location's metadata. The UI reads that list and shows only the types
flagged as editable.

Entry points are only installed when the package is (re)installed. After
changing this section locally, run `uv sync`. Deploys pick it up
automatically, because the package is installed fresh on every build.

### Checking it locally

```bash
uv run dg list components
```

`ui_backed_components.components.BikiniBottomIngestion` should appear next to
the built-in `dagster.*` types. If it doesn't, the entry point isn't
installed; run `uv sync` and try again.

## Step 4: Get it deployed

The code has to be running in Dagster+ before the UI can offer the component.

1. `uv run dg plus login`: log in to the Dagster+ organization.
2. `uv run dg plus deploy configure`: generates the GitHub Actions workflow and
   Dagster+ config. It asks a few questions along the way.
3. Commit, then push to `main` on GitHub. The push itself starts the first
   deploy, so no empty commit is needed. If the first run fails because the
   API token secret wasn't set yet, re-run it from the **Actions** tab.

**Don't rename the code location once you've created components in the UI.**
UI-created instances are stored per code location name, so a renamed location
starts out with none.

## Step 5: Fill in the form

What you should see, and what to expect afterward.

**Before you start, check:**
- The component-instance feature flag is on for your organization or user.
- A git integration (GitHub) is connected. The **Add** button is disabled
  without one.
- If git-backed authoring is on, a repository is set for the code location.
  In that mode, saving the form opens a pull request instead of applying the
  change immediately.
- You're in the base deployment (`prod`), not a branch deployment. Branch
  deployments only allow editing an existing draft.

**The walkthrough:**
1. Go to the code location and open its **Components** page. Two subtabs
   should appear: **Library**, where `BikiniBottomIngestion` should be listed,
   and **Instances**, which starts empty.
2. On **Instances**, click **Add**. The "Add component instance" dialog
   lists only editable types: **Bikini Bottom ingestion**, plus the two Phase 2
   types below.
3. Pick it. You'll get the form described in Step 2, with **Form** and
   **YAML** tabs.
4. Enter an asset key prefix (try `bikini_bottom`), optionally uncheck some
   tables, and save.
5. After the code location reloads, the new assets appear in the asset
   catalog under the group you chose.
6. Materialize them. Each one's latest materialization should show the row
   count, column schema, and preview table.

## Phase 2: Components that modify existing assets

Two more component types change how the ingested assets behave instead of
adding new ones. Each one points at assets that already exist. Like the
ingestion component, they live in `src/ui_backed_components/components/`, are
marked `editable=True`, and are exported from `components/__init__.py`, so the
existing entry point registers them. No changes to `pyproject.toml` were
needed.

### Row count check (`RowCountCheck`)

This adds an asset check that passes when an asset's latest materialization
has at least a minimum number of rows. It reads the row count the asset
already recorded, so it never touches the data.

| Form field                  | Required? | Default | What it does                                                              |
| --------------------------- | --------- | ------- | ------------------------------------------------------------------------- |
| **Asset to check**          | Yes       | —       | Slash-separated key, e.g. `bikini_bottom/krusty_krab_orders`. Also the component's ID. |
| **Minimum rows**            | No        | 1       | Fails below this                                                          |
| **Severity when it fails**  | No        | WARN    | Dropdown: WARN or ERROR                                                   |
| **Block downstream assets** | No        | Off     | Under **Advanced**. With ERROR severity, stops downstream assets in the same run. |

The check appears on the asset's **Checks** tab as `min_row_count`.

### Refresh schedule (`AssetSchedule`)

This materializes a selection of assets on a cron schedule. Any checks on
those assets run in the same run.

| Form field              | Required? | Default         | What it does                                                    |
| ----------------------- | --------- | --------------- | --------------------------------------------------------------- |
| **Schedule name**       | Yes       | —               | Letters, numbers, underscores. Also the component's ID.          |
| **Assets to refresh**   | Yes       | —               | Asset selection, e.g. `group:bikini_bottom` or `key:"bikini_bottom/characters"` |
| **Schedule**            | No        | Hourly          | Cron widget                                                     |
| **Turn on immediately** | No        | Off             | If off, turn it on from the schedule's page                     |
| **Timezone**            | No        | UTC             | Under **Advanced**                                              |

It creates a schedule with the name you give and a job named
`<name>_job`.

### Suggested test run

1. **Row count check:** `bikini_bottom/krusty_krab_orders`, minimum 200. It
   has 240 rows, so it should **pass**.
2. **Row count check:** `bikini_bottom/characters`, minimum 100. It has 16
   rows, so it should **fail with a warning**.
3. **Refresh schedule:** name `bikini_bottom_hourly`, assets
   `group:bikini_bottom`, any cron.
4. Open the schedule and launch a run from it (or wait for a tick if you
   turned it on). All five assets should materialize, and both checks should
   run in the same run with the results above.

If you turn the schedule on, turn it off again when you're done, or it will
keep launching runs every hour.

## Rough edges worth watching (dogfooding notes)

- **The "Tables to ingest" checkboxes.** The form's custom dropdown widget only
  supports picking one value. So this field uses the standard checkbox group
  from react-jsonschema-form (the library the form is built on), which isn't
  part of Dagster's own widget set. It should work, but it may look unstyled
  next to the other fields.
- **List defaults don't reach the form on their own.** Dagster only copies
  simple defaults (text, numbers, true/false) into the form. A list default like
  "all five tables" is dropped, which would leave every box unchecked. The
  component works around this by declaring the default in the form schema
  explicitly (see the comment on the `tables` field). That's probably worth
  fixing in the framework.
- **The ID comes from the prefix.** Because the prefix field is marked as the
  ID source, two instances with the same prefix would collide. Choose distinct
  prefixes if you add more than one.
