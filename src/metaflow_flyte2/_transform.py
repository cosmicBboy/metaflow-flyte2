"""Rewrite metaflow-flyte's generated module so it runs correctly on Flyte 2.

The generator emits a Flyte 1 module that is valid on its own terms; three of its
assumptions stop holding once ``flyte_migrate`` turns it into Flyte 2 entities.
Each is fixed here, in the generated *source*, rather than by a client-side
monkeypatch — because the module is re-imported in two more places that a
client-side patch never reaches: the parent workflow container (which
re-serializes its child task templates from scratch) and each task container
(which imports the module to resolve the task it is about to run). Fixing the
source means those imports need nothing installed beyond published packages.

1. **Entity names.** Generated functions are module-private (``_step_start``,
   ``_mf_generate_run_id``). ``flyte_migrate`` derives environment names as
   ``f"{module_slug(module)}_{fn.__name__}_env"``, so a leading underscore yields
   ``linear_gen__mf_generate_run_id_env`` — a double underscore, which Flyte 2
   rejects with "must be in snake_case or kebab-case format".

2. **Thread affinity and run identity.** See :data:`_PRELUDE`.

3. **Datastore root.** Metaflow steps exchange artifacts through a datastore
   that every pod must share. Rather than making the user supply one, ``_run_cmd``
   is taught to default it to Flyte's own object store, read from
   ``flyte.ctx().raw_data_path`` at task runtime — see
   :func:`inject_datastore_resolution`.

4. **Container environment and default resources.** The Metaflow step subprocess
   needs to know where the flow file is and which datastore to use, and it needs
   enough memory for two Python processes. Both are injected through the v1
   ``@task`` arguments the generator already funnels through ``_TASK_KWARGS``
   (``environment=``, ``requests=``, ``limits=``) rather than set on the v2
   environment objects client-side — because the parent workflow container
   rebuilds its children's task templates from scratch and keeps only what the
   source itself declares. Anything attached client-side is silently dropped,
   which surfaces as a step that cannot find its inputs or an OOMKill.

The rename is done in two passes: an AST pass decides *which* names to change
(only module-level functions carrying a task/workflow decorator, never the plain
helpers around them), and a token pass performs the substitution, so comments,
strings, and formatting survive untouched.
"""

from __future__ import annotations

import ast
import io
import tokenize

from metaflow_flyte2._naming import sanitize

#: Decorators that turn a generated function into a Flyte entity. Anything else
#: at module level is a helper called from inside a task and needs no rename.
ENTITY_DECORATORS = frozenset({"_mf_task", "task", "dynamic", "workflow"})

#: Marker used to keep every injection idempotent.
_MARKER = "metaflow_flyte2"

#: Injected just below the generated module's imports.
#:
#: *Signals.* ``_run_cmd`` forwards SIGTERM/SIGINT to the step subprocess by
#: installing handlers with ``signal.signal``. That reflects Flyte 1's model,
#: where the task body owns the process's main thread. Flyte 2 drives tasks from
#: an asyncio controller and runs the function on a worker thread, where CPython
#: refuses the call: ``ValueError: signal only works in main thread of the main
#: interpreter``. Shadowing the module with a proxy keeps the handler when the
#: task does hold the main thread and skips it otherwise — the best available
#: behaviour, since CPython offers no way to install one from a worker thread.
#: When skipped, a cancelled task's subprocess is reaped by pod teardown rather
#: than by an explicitly forwarded signal.
#:
#: *Run identity.* The generator derives the Metaflow run id from
#: ``flytekit.current_context().execution_id.name``. ``flyte_migrate`` resolves
#: that through the v2 context, where the field reads ``local`` inside a task
#: container — so every remote run would be named ``flyte-local-<random>`` and
#: lose its link to the Flyte execution. The real execution name is in the
#: environment, and the generator already prefers ``METAFLOW_FLYTE_LOCAL_RUN_ID``
#: over the context, so seeding it restores the intended ``flyte-<execution>``.
_PRELUDE = '''
# --- injected by metaflow_flyte2 (see metaflow_flyte2._transform) -----------
import signal as _mf2_signal
import threading as _mf2_threading


class _Mf2MainThreadOnlySignal:
    """``signal`` proxy whose ``signal()`` is a no-op off the main thread."""

    def __getattr__(self, name):
        return getattr(_mf2_signal, name)

    def signal(self, signalnum, handler):
        if _mf2_threading.current_thread() is _mf2_threading.main_thread():
            return _mf2_signal.signal(signalnum, handler)
        return _mf2_signal.SIG_DFL


signal = _Mf2MainThreadOnlySignal()


# Metaflow's env var for each datastore backend it can share across pods.
_MF2_SYSROOT_VARS = {
    's3': 'METAFLOW_DATASTORE_SYSROOT_S3',
    'gs': 'METAFLOW_DATASTORE_SYSROOT_GS',
    'azure': 'METAFLOW_DATASTORE_SYSROOT_AZURE',
}


def _mf2_ensure_datastore_root(env):
    """Default Metaflow's datastore to Flyte's own object store.

    Only the *bucket* is taken from the raw data path — that part is identical
    for every action in every run, which is what the datastore needs. The rest of
    the raw data path is per-action and would leave each step writing somewhere
    the next one cannot find.
    """
    var = _MF2_SYSROOT_VARS.get(DATASTORE_TYPE)
    if not var or env.get(var):
        return  # local datastore, or an explicit root was configured
    try:
        import flyte

        raw = str(flyte.ctx().raw_data_path.path)
    except Exception:
        return
    scheme, _, remainder = raw.partition('://')
    bucket = remainder.split('/', 1)[0]
    if not remainder or not bucket:
        return  # a local raw data path: nothing to derive a shared root from
    env[var] = '{}://{}/metaflow-datastore/{}/{}'.format(
        scheme,
        bucket,
        env.get('FLYTE_INTERNAL_PROJECT', 'default'),
        env.get('FLYTE_INTERNAL_DOMAIN', 'default'),
    )


if not os.environ.get('METAFLOW_FLYTE_LOCAL_RUN_ID'):
    _mf2_execution = os.environ.get('FLYTE_INTERNAL_EXECUTION_ID')
    if _mf2_execution:
        os.environ['METAFLOW_FLYTE_LOCAL_RUN_ID'] = 'flyte-' + _mf2_execution
# --- end metaflow_flyte2 ---------------------------------------------------
'''


def _decorator_name(node: ast.expr) -> str | None:
    """Base name of a decorator expression: ``@f``, ``@f(...)``, ``@m.f`` -> ``f``."""
    while isinstance(node, ast.Call):
        node = node.func
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _is_entity(node: ast.stmt) -> bool:
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return False
    return any(_decorator_name(d) in ENTITY_DECORATORS for d in node.decorator_list)


def plan_renames(source: str) -> dict[str, str]:
    """Map each generated entity function to a name Flyte 2 will accept.

    Names that are already valid are left out, so an unchanged file maps to itself.
    """
    tree = ast.parse(source)

    # Every identifier bound anywhere in the module, so a rename cannot collide
    # with an existing helper, constant, or import.
    taken: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            taken.add(node.name)
        elif isinstance(node, ast.Name):
            taken.add(node.id)
        elif isinstance(node, ast.alias):
            taken.add(node.asname or node.name.split(".")[0])

    renames: dict[str, str] = {}
    for node in tree.body:
        if not _is_entity(node):
            continue
        name = node.name  # type: ignore[attr-defined]
        base = sanitize(name)
        if base == name:
            continue
        candidate, suffix = base, 2
        while candidate in taken:
            candidate = f"{base}_{suffix}"
            suffix += 1
        taken.add(candidate)
        renames[name] = candidate
    return renames


def apply_renames(source: str, renames: dict[str, str]) -> str:
    """Substitute *renames* over NAME tokens only, preserving everything else.

    Tokenizing rather than regex-replacing keeps ``_step_start`` inside a
    docstring or log message from being rewritten, and stops a rename of
    ``_step_end`` from also hitting ``_step_end_2``.
    """
    if not renames:
        return source

    # NAME tokens never span lines, so every edit is a splice within one line.
    edits: dict[int, list[tuple[int, int, str]]] = {}
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type != tokenize.NAME or tok.string not in renames:
            continue
        row, start_col = tok.start
        _, end_col = tok.end
        edits.setdefault(row, []).append((start_col, end_col, renames[tok.string]))

    if not edits:
        return source

    lines = source.splitlines(keepends=True)
    for row, spans in edits.items():
        line = lines[row - 1]
        # Right-to-left, so earlier spans keep their original column offsets.
        for start_col, end_col, replacement in sorted(spans, reverse=True):
            line = line[:start_col] + replacement + line[end_col:]
        lines[row - 1] = line
    return "".join(lines)


def _insert_after_line(source: str, lineno: int, block: str) -> str:
    lines = source.splitlines(keepends=True)
    if lines and not lines[-1].endswith("\n"):
        lines[-1] += "\n"
    lines.insert(lineno, block)
    return "".join(lines)


def inject_prelude(source: str) -> str:
    """Insert :data:`_PRELUDE` directly below the module's imports.

    Positioned via the AST rather than by matching text, so it lands after the
    last top-level import however the generator orders them — and, critically,
    after ``import signal``, which it shadows, and ``import os``, which it uses.
    """
    if "_Mf2MainThreadOnlySignal" in source:
        return source

    last_import_line = 0
    for node in ast.parse(source).body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            last_import_line = max(last_import_line, node.end_lineno or node.lineno)
    return _insert_after_line(source, last_import_line, _PRELUDE)


#: The two lines metaflow-flyte emits at the top of ``_run_cmd``.
_RUN_CMD_ANCHOR = """\
    env = os.environ.copy()
    env.setdefault('METAFLOW_DATASTORE_SYSROOT_LOCAL', os.path.expanduser('~'))
"""

#: The exact line inserted into ``_run_cmd``. Used as the idempotency marker —
#: matching on the bare call would also match the prelude's *definition* of the
#: same function, so the injection would silently never happen.
_RUN_CMD_CALL = "    _mf2_ensure_datastore_root(env)  # injected by metaflow_flyte2\n"


def inject_datastore_resolution(source: str) -> str:
    """Resolve the datastore root inside the task, when none was configured.

    ``_run_cmd`` is the one place every Metaflow subprocess environment is built
    — both the ``init`` command and each ``step`` — so hooking it covers all of
    them. It has to happen here rather than at compile time for two reasons: the
    client cannot discover the tenant's object store (the platform assigns it
    per-run, and the SDK's client-side default is a local ``/tmp`` path), and the
    value is only knowable from inside a task, where ``flyte.ctx()`` exists.

    Skipped silently if the generator's ``_run_cmd`` no longer matches; an
    explicit ``--datastore-root`` still works in that case.
    """
    if _RUN_CMD_CALL in source or _RUN_CMD_ANCHOR not in source:
        return source
    return source.replace(_RUN_CMD_ANCHOR, _RUN_CMD_ANCHOR + _RUN_CMD_CALL, 1)


def inject_task_defaults(
    source: str,
    env_vars: dict[str, str],
    cpu: str | float | None = None,
    memory: str | None = None,
) -> str:
    """Bake the container environment and default resources into every task.

    The generator funnels all of its task decorators through a single
    ``_TASK_KWARGS`` dict, so extending that one assignment reaches every step
    task and every ``@dynamic`` expander — and, because ``_mf_task`` merges as
    ``{**_TASK_KWARGS, **extra}``, a step whose Metaflow ``@resources`` produced
    its own ``requests``/``limits`` still overrides these defaults.
    """
    if "_MF2_ENVIRONMENT" in source or not (env_vars or cpu or memory):
        return source

    target_line = 0
    for node in ast.parse(source).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "_TASK_KWARGS" for t in node.targets
        ):
            target_line = node.end_lineno or node.lineno
    if not target_line:
        # No ``_TASK_KWARGS`` means an unrecognised generator layout; leaving the
        # source alone and failing loudly beats silently dropping the datastore
        # settings and watching every step fail to find its inputs.
        raise ValueError(
            "Generated module has no module-level _TASK_KWARGS assignment; "
            "cannot inject the Metaflow container environment."
        )

    defaults = ["'environment': _MF2_ENVIRONMENT"]
    resource_line = ""
    if cpu or memory:
        args = []
        if cpu:
            args.append(f"cpu={str(cpu)!r}")
        if memory:
            args.append(f"mem={str(memory)!r}")
        resource_line = "_MF2_RESOURCES = Resources(" + ", ".join(args) + ")\n"
        # requests and limits both, matching how metaflow-flyte renders @resources.
        defaults += ["'requests': _MF2_RESOURCES", "'limits': _MF2_RESOURCES"]

    header = (
        "\n# --- injected by metaflow_flyte2 (see metaflow_flyte2._transform) -----------\n"
        "# What the Metaflow step subprocess needs from its container: where the flow file\n"
        "# and datastore are, and room for two Python processes. Injected as v1 task\n"
        "# arguments so they survive re-serialization by the parent workflow container.\n"
        "# A step with its own @resources overrides the defaults via _mf_task(**extra).\n"
    )
    block = (
        header
        + f"_MF2_ENVIRONMENT = {env_vars!r}\n"
        + resource_line
        + "_TASK_KWARGS = {**_TASK_KWARGS, " + ", ".join(defaults) + "}\n"
        + "# --- end metaflow_flyte2 ---------------------------------------------------\n"
    )
    return _insert_after_line(source, target_line, block)


#: The single-level unwrap metaflow-flyte emits in ``_read_condition_branch``.
_BRANCH_UNWRAP_OLD = """\
        if isinstance(_transition, (list, tuple)) and _transition:
            return str(_transition[0])
        return str(_transition)
"""

#: A loop that unwraps however deeply the artifact happens to be nested.
_BRANCH_UNWRAP_NEW = """\
        # metaflow_flyte2: unwrap fully — see _fix_condition_branch.
        while isinstance(_transition, (list, tuple)) and _transition:
            _transition = _transition[0]
        return str(_transition)
"""


def fix_condition_branch(source: str) -> str:
    """Work around metaflow-flyte's single-level unwrap of Metaflow's ``_transition``.

    A Metaflow conditional (``self.next({...}, condition=...)``) is compiled into a
    ``@dynamic`` router that reads which arm was taken from the step's
    ``_transition`` artifact. metaflow-flyte unwraps one level, on the assumption
    that the artifact is a flat list of step names. On Metaflow 2.19 it is a
    *tuple* whose first element is that list — ``(['high'], None)`` — so the router
    receives the string ``"['high']"`` and every conditional flow dies with
    ``Unexpected branch "['high']" for step 'start'``.

    This is an upstream metaflow-flyte bug rather than anything to do with Flyte 2
    (the same flow fails under v1 ``pyflyte run``), but conditionals are a core
    Metaflow graph shape, so the composition is not usable without it. Remove this
    step once metaflow-flyte handles the nested form; the patch is skipped
    silently when the expected source is absent, so a fixed upstream is a no-op.
    """
    if _BRANCH_UNWRAP_OLD not in source:
        return source
    return source.replace(_BRANCH_UNWRAP_OLD, _BRANCH_UNWRAP_NEW, 1)


def transform(
    source: str,
    env_vars: dict[str, str] | None = None,
    cpu: str | float | None = None,
    memory: str | None = None,
) -> str:
    """Make a generated metaflow-flyte module run correctly under ``flyte_migrate``."""
    rewritten = apply_renames(source, plan_renames(source))
    rewritten = inject_prelude(rewritten)
    rewritten = inject_task_defaults(rewritten, env_vars or {}, cpu, memory)
    rewritten = inject_datastore_resolution(rewritten)
    rewritten = fix_condition_branch(rewritten)
    if rewritten == source:
        return source
    note = (
        f"# Post-processed by {_MARKER} to compose metaflow-flyte's code generator with\n"
        f"# flyte-migrate's v1-to-v2 shim. See metaflow_flyte2._transform for the why.\n"
    )
    return note + rewritten
