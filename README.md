# metaflow-flyte2

Run **Metaflow** flows on **Flyte 2** — without changing a line of your flow.

```bash
pyflyte-metaflow run --remote --datastore-root s3://my-bucket/metaflow my_flow.py
```

## What this is

Two existing libraries each solve half the problem, and neither knows about the other:

| Library | What it does | What it leaves you with |
|---|---|---|
| [`metaflow-flyte`](https://github.com/npow/metaflow-flyte) | Compiles a Metaflow `FlowSpec` into a workflow module | …written against **Flyte 1** (`flytekit`) |
| [`flyte-migrate`](https://github.com/flyteorg/flyte-migrate) | Shims `flytekit`'s namespace onto Flyte 2 | …but only for code that was *hand-written* as Flyte 1 |

Chained, they get you from `FlowSpec` to a Flyte 2 run. `metaflow-flyte2` is the glue that
makes the chain actually hold together — the reconciliation work described in
[How it works](#how-it-works) below.

Metaflow's execution model is preserved throughout. Each step still runs as a Metaflow
subprocess against a Metaflow datastore, so versioning, artifacts, `Run`/`Task` client
APIs, and `current.run_id` all keep working. Flyte 2 provides the scheduling, retries,
fan-out, UI, and observability.

```
FlowSpec ──metaflow-flyte──▶ flytekit module ──flyte-migrate──▶ Flyte 2 entities
                    └───────── metaflow-flyte2 ──────────┘
```

## Install

```bash
pip install -e .          # pulls metaflow, metaflow-flyte, flyte-migrate, flyte
```

Cluster connection uses the standard Flyte 2 config discovery (`./config.yaml`,
`.flyte/config.yaml`, `~/.flyte/config.yaml`), overridable with `-c /path/to/config.yaml`.

## Usage

### CLI — `pyflyte-metaflow`

Mirrors `pyflyte-migrate` (which mirrors `pyflyte`), but takes the **Metaflow flow file**.
Compilation and patching happen before anything is imported, so your flow files need no
import line at all.

```bash
# Run locally — no cluster required
pyflyte-metaflow run my_flow.py

# Run on the cluster
pyflyte-metaflow run --remote --datastore-root s3://my-bucket/metaflow my_flow.py

# Metaflow Parameters go after the file name
pyflyte-metaflow run my_flow.py --alpha 0.5 --label experiment-1

# Register, so it can be launched and scheduled from the UI or API
pyflyte-metaflow deploy --datastore-root s3://my-bucket/metaflow my_flow.py

# Inspect the generated module without running anything
pyflyte-metaflow compile --remote --datastore-root s3://my-bucket/metaflow my_flow.py
```

Useful flags: `-p/--project`, `-d/--domain`, `--follow`, `-i/--image`,
`--package` (extra pip packages for the task image — your flow's own dependencies),
`--cpu`/`--memory`, `--max-parallelism`, `-e/--env KEY=VALUE`.

### Programmatic API

`import metaflow_flyte2` applies the shim, the same way `import flyte_migrate` does. From
there you hold an ordinary Flyte 2 task:

```python
import flyte
import metaflow_flyte2

flyte.init_from_config()

workflow = metaflow_flyte2.load_workflow(
    "my_flow.py",
    datastore="s3",
    datastore_root="s3://my-bucket/metaflow",
)
run = flyte.with_runcontext(mode="remote", copy_style="all").run(workflow, alpha=0.5)
print(run.url)
```

Or the one-liners:

```python
metaflow_flyte2.run("my_flow.py", remote=True, follow=True,
                    inputs={"alpha": 0.5},
                    datastore="s3", datastore_root="s3://my-bucket/metaflow")

metaflow_flyte2.deploy("my_flow.py",
                       datastore="s3", datastore_root="s3://my-bucket/metaflow")
```

`compile_workflow()` returns the generated module path without importing it;
`environments()` returns the `TaskEnvironment`s for a custom `flyte.deploy` call.

## The datastore requirement

**Remote runs need `--datastore-root`.** Metaflow steps hand artifacts to each other
through the *datastore*, not through Flyte's task I/O. On a cluster every step is its own
pod, so a `local` datastore leaves step 2 unable to see anything step 1 wrote. Point
`--datastore-root` at an S3 prefix the task role can write to and all steps share it.

Local runs need nothing — the default `local` datastore is fine when every step is a
subprocess on one machine.

## How it works

Chaining the two libraries naively fails. Each of these is a real failure observed against
a live Flyte 2 tenant, and each is fixed in [`_transform.py`](src/metaflow_flyte2/_transform.py)
or [`_patch.py`](src/metaflow_flyte2/_patch.py):

1. **Entity names.** metaflow-flyte generates module-private task functions
   (`_step_start`). flyte-migrate names environments `f"{module}_{fn.__name__}_env"`, so a
   leading underscore yields `my_flow__step_start_env` — a double underscore, which Flyte 2
   rejects outright. The generated functions are renamed in the source (AST decides *which*
   names, a token pass does the substitution, so strings and comments are untouched).

2. **Signal handlers off the main thread.** The generated `_run_cmd` forwards SIGTERM to
   the step subprocess via `signal.signal`, which assumes Flyte 1's process-per-task model.
   Flyte 2 runs task functions on a worker thread, where CPython raises
   `ValueError: signal only works in main thread`. A `signal` proxy is injected that installs
   the handler when the task does hold the main thread and skips it otherwise.

3. **Run identity.** The Metaflow run id is derived from
   `flytekit.current_context().execution_id.name`, which reads `local` inside a v2 task
   container — every remote run would be `flyte-local-<random>`. The real execution name is
   in the environment, so it is seeded into `METAFLOW_FLYTE_LOCAL_RUN_ID`, restoring the
   intended `flyte-<execution>` linkage.

4. **Settings lost to re-serialization.** This one is subtle. The parent workflow container
   re-imports the generated module and rebuilds its children's task templates *from
   scratch*, discarding anything attached to the environment objects client-side. Setting
   env vars or resources on the `TaskEnvironment` therefore looks correct locally and
   silently vanishes on the cluster — surfacing as a step that cannot find its inputs, or an
   OOMKill. So the container environment (flow file location, datastore) and default
   resources are baked into the generated **source**, through the v1 `@task(environment=,
   requests=, limits=)` arguments that flyte-migrate already understands.

5. **Resource defaults.** A Metaflow step task runs *two* Python processes — the Flyte task
   runtime and the Metaflow subprocess it spawns — so the cluster's bare default is easily
   too small (a join over a wide foreach OOMKills). The default is 1 CPU / 4 GiB, matching
   what Metaflow's own `@batch`/`@kubernetes` backends give you. A step with its own
   `@resources` always wins.

6. **Images.** The task image needs `metaflow` and `metaflow-flyte` (the generated command
   line passes `--with=flyte_internal`, a decorator from that package), plus `boto3` for the
   S3 datastore. These are layered onto the images flyte-migrate builds. Add your flow's own
   dependencies with `--package`.

7. **Bundle integrity.** The Metaflow flow file is *executed as a subprocess*, not imported,
   so Flyte's module-based bundling cannot discover it; remote runs use `copy_style="all"`.
   Since Flyte honours `.gitignore`/`.flyteignore`, an ignore rule covering the flow file or
   the generated module would break the run in the cluster — so both are checked before
   submission and refused with an actionable message. A stray `.metaflow/` local datastore
   inside the bundle root is warned about (it can be thousands of files).

The net effect: the container needs nothing from this package. Everything it depends on is
either in the generated source or in a published dependency.

### One upstream workaround

`fix_condition_branch` repairs metaflow-flyte's single-level unwrap of Metaflow's
`_transition` artifact. On Metaflow 2.19 that artifact is `(['high'], None)`, so the
conditional router receives the string `"['high']"` and every `self.next({...},
condition=...)` flow fails. This is a metaflow-flyte bug independent of Flyte 2 — the same
flow fails under v1 `pyflyte run` — but conditionals are a core Metaflow graph shape, so the
composition is unusable without it. The patch no-ops if upstream changes.

## What's verified

Every example below was run both locally and against a live Flyte 2 tenant, and
produces the same result as plain Metaflow:

| Example | Covers | Local | Remote |
|---|---|:--:|:--:|
| `linear_flow.py` | `start → process → end` | ✅ | ✅ |
| `branch_flow.py` | split/join, `merge_artifacts` | ✅ | ✅ |
| `condition_flow.py` | `self.next({...}, condition=)` | ✅ | ✅ |
| `foreach_flow.py` | foreach fan-out + join | ✅ | ✅ |
| `param_flow.py` | `Parameter` | ✅ | ✅ |
| `resources_flow.py` | `@resources` | ✅ | ✅ |
| `retry_flow.py` | `@retry` | ✅ | ✅ |
| `timeout_flow.py` | `@timeout`, `@environment` | ✅ | ✅ |
| `showcase_flow.py` | parameters + foreach + join, end to end | ✅ | ✅ |

Step decorators land where they should: `@resources(cpu=4, memory=8000)` compiles to
`requests=Resources(cpu='4', mem='8000Mi')`, `@retry(times=2)` to `retries=2`, and
`@timeout(hours=1)` to `timeout=timedelta(seconds=3600)` — each overriding the injected
defaults, since the generator merges per-step arguments last. The retry and timeout flows
confirm the configuration propagates; they do not exercise an actual failure or expiry.

Also verified end to end:

- **Fan-out correctness.** `showcase_flow --count 5` returns `total=55` under plain
  Metaflow, under Flyte 2 locally, and on the cluster — artifacts crossing five separate
  pods through the shared S3 datastore.
- **Run identity.** A remote run reports `run=flyte-<flyte-execution-id>`, linking the
  Metaflow run to the Flyte execution.
- **The registered path.** `deploy` registers all entities, and launching the registered
  task from the platform (no client involved) succeeds — this is the path where anything
  attached client-side would be missing, and the reason for fix #4 above.

## Tests

```bash
pytest                                                            # unit + integration
pytest -m remote --datastore-root s3://my-bucket/metaflow         # against a live cluster
```

## Examples

[`examples/`](examples) holds one flow per graph shape, plus
[`showcase_flow.py`](examples/showcase_flow.py) (parameters + foreach + join) and
[`programmatic.py`](examples/programmatic.py). Every flow runs unmodified under plain
Metaflow (`python showcase_flow.py run`) and on Flyte 2.

## Known limitations

- **Metaflow decorators without a Flyte 2 equivalent** (`@conda`/`@pypi` environment
  management, `@card`) are passed through to metaflow-flyte's own handling and may not
  behave identically; prefer `--package` / `--image` for dependencies.
- **Signal forwarding is skipped off the main thread** (see #2 above), so a cancelled task's
  step subprocess is reaped by pod teardown rather than by a forwarded SIGTERM.
- **The generated `*_flyte2.py` module must not be gitignored** — it ships in the code
  bundle. This is checked before submission.
- Inherited from flyte-migrate: v1 features with no v2 equivalent (`failure_policy`,
  `on_failure`, some launch-plan options) are logged and ignored.
