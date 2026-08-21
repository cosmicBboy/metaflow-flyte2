"""Programmatic entry points: compile a Metaflow flow and run it on Flyte 2.

Mirrors the shape of ``flyte_migrate``'s programmatic path — import the package to
apply the shim, then drive the v2 API yourself — but adds the compile step, since
a Metaflow ``FlowSpec`` is not a ``flytekit`` object and has to be turned into one
first::

    import flyte
    import metaflow_flyte2

    flyte.init_from_config()
    wf = metaflow_flyte2.load_workflow("linear_flow.py")
    run = flyte.with_runcontext(mode="local").run(wf)

or, for the common cases, the one-liners :func:`run` and :func:`deploy`.
"""

from __future__ import annotations

import importlib
import inspect
import json
import os
import sys
import typing
import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from metaflow_flyte2._compile import CompileError, compile_flow, flow_class_name, workflow_function_name
from metaflow_flyte2._patch import apply
from metaflow_flyte2._runtime import RuntimeConfig, set_config

#: Metaflow steps exchange artifacts through the datastore, and the flow file is
#: launched as a subprocess rather than imported — so neither is visible to
#: Flyte's module-based bundling. Everything under the root dir has to ship.
_REMOTE_COPY_STYLE = "all"


def _root_dir(explicit: os.PathLike | str | None, flow_path: Path) -> Path:
    """Directory that becomes the root of the code bundle.

    Defaults to the initialised ``flyte`` root dir when there is one, so that the
    module path this process imports matches the one the container will use, and
    falls back to the flow's own directory.
    """
    if explicit is not None:
        return Path(explicit).resolve()
    try:
        from flyte._initialize import get_init_config

        cfg = get_init_config()
        if cfg is not None and cfg.root_dir is not None:
            return Path(cfg.root_dir).resolve()
    except Exception:
        pass
    return flow_path.parent


def _warn_on_local_datastore(root: Path) -> None:
    """Warn when Metaflow's local datastore sits inside the code bundle root.

    Remote runs bundle everything under the root, and a ``.metaflow`` directory
    left behind by a local ``python flow.py run`` can be thousands of files. Flyte
    honours ``.gitignore``/``.flyteignore``, so the fix is one line in either.
    """
    local_store = root / ".metaflow"
    if local_store.is_dir():
        warnings.warn(
            f"{local_store} is inside the code-bundle root and will be uploaded with your flow. "
            "Add '.metaflow/' to .gitignore or .flyteignore to keep it out.",
            stacklevel=3,
        )


def _check_bundled(root: Path, *required: Path) -> None:
    """Fail early if a file the container needs would be filtered out of the bundle.

    Both the flow file and the generated module must reach the container: the
    module is what the task resolver imports, and the flow file is what the step
    subprocess executes. Neither is an imported dependency of the other, so
    Flyte's module-based discovery cannot infer them — they ride along only
    because ``copy_style="all"`` sweeps the root directory, and an ignore rule in
    the user's repo silently removes them again.

    Without this check the failure surfaces in the cluster as
    ``ModuleNotFoundError: No module named '<flow>_flyte2'``, minutes and one
    image build later.
    """
    try:
        from flyte._code_bundle._ignore import FlyteIgnore, GitIgnore, IgnoreGroup, StandardIgnore

        # Same set, in the same order, as flyte._code_bundle.bundle uses.
        ignore = IgnoreGroup(root.resolve(), StandardIgnore, GitIgnore, FlyteIgnore)
    except Exception:  # pragma: no cover - never block a run over a missing internal
        return

    ignored = [path for path in required if ignore.is_ignored(path.resolve())]
    if ignored:
        names = ", ".join(str(p.relative_to(root)) for p in ignored)
        raise CompileError(
            f"{names} would be excluded from the code bundle by a .gitignore/.flyteignore rule "
            f"under {root}, but the cluster needs these files to run your flow.\n"
            "Un-ignore them (e.g. add a '!' negation), or move the generated module "
            "somewhere that is not ignored with --output-file."
        )


def _import_generated(module_path: Path, root: Path) -> Any:
    """Import the generated module under the dotted name the container will use.

    Importing by bare file stem would name the environments differently on either
    side of the wire — the same trap ``flyte_migrate``'s CLI documents — and remote
    submission would fail with "Environment ... not found in image cache".
    """
    try:
        relative = module_path.resolve().relative_to(root)
        module_name = ".".join(relative.with_suffix("").parts)
    except ValueError:
        module_name = module_path.stem
        root = module_path.resolve().parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    # A previous load in this process would return a stale module whose
    # environments were built against the old runtime config.
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


def compile_workflow(
    flow_file: os.PathLike | str,
    *,
    datastore: str = "local",
    datastore_root: str | None = None,
    image: str | None = None,
    packages: Sequence[str] = (),
    env_vars: dict[str, str] | None = None,
    username: str | None = None,
    default_cpu: str | float | None = 1,
    default_memory: str | None = "4Gi",
    project: str = "flytesnacks",
    domain: str = "development",
    root_dir: os.PathLike | str | None = None,
    output_file: os.PathLike | str | None = None,
    **compile_kwargs: Any,
) -> tuple[Path, Path, RuntimeConfig]:
    """Compile *flow_file* into a Flyte-2-ready module without importing it.

    Returns ``(generated_module, bundle_root, runtime_config)``. Split out from
    :func:`load_workflow` so that every entry point — ``run``, ``deploy``, and the
    CLI's ``compile`` — produces a byte-identical artifact; a ``compile`` that
    skipped the injections would hand back a module that fails on the cluster.
    """
    flow_path = Path(flow_file).resolve()
    root = _root_dir(root_dir, flow_path)
    _warn_on_local_datastore(root)

    try:
        flow_file_in_container = str(flow_path.relative_to(root))
    except ValueError:
        raise CompileError(
            f"Flow file {flow_path} is outside the code-bundle root {root}; "
            "pass root_dir= (or run from a parent directory) so it can be shipped to the cluster."
        ) from None

    cfg = RuntimeConfig(
        flow_file_in_container=flow_file_in_container,
        datastore=datastore,
        datastore_root=datastore_root,
        extra_packages=tuple(packages),
        extra_env=dict(env_vars or {}),
        username=username,
        default_cpu=default_cpu,
        default_memory=default_memory,
    )

    generated = compile_flow(
        flow_path,
        output_file,
        datastore=datastore,
        datastore_root=datastore_root,
        image=image,
        project=project,
        domain=domain,
        container_env=cfg.env_vars(),
        default_cpu=cfg.default_cpu,
        default_memory=cfg.default_memory,
        **compile_kwargs,
    )
    return generated, root, cfg


def load_workflow(
    flow_file: os.PathLike | str,
    *,
    remote_ready: bool = True,
    keep_generated: bool = True,
    **kwargs: Any,
) -> Any:
    """Compile *flow_file* and return the Flyte 2 task that runs it.

    The returned object is a v2 ``TaskTemplate``: pass it to ``flyte.run``,
    ``flyte.deploy``, or ``flyte.with_runcontext(...).run``. See
    :func:`compile_workflow` for the compile-time options.

    Remote runs need ``datastore="s3"`` — every Metaflow step executes in its own
    pod, and a ``local`` datastore would leave each unable to read the previous
    step's artifacts. ``datastore_root`` is optional; left unset, the generated
    module points Metaflow at Flyte's own object store at task runtime.

    ``remote_ready`` gates the checks that only matter when the code is shipped to
    a cluster; :func:`run` clears it for local runs, where nothing is bundled.
    """
    flow_path = Path(flow_file).resolve()
    generated, root, cfg = compile_workflow(flow_path, **kwargs)

    # Must precede the import: the environments are built as the generated
    # module's decorators execute, and that is the only moment the injected
    # image layers can reach them.
    set_config(cfg)
    apply()

    if remote_ready:
        _check_bundled(root, flow_path, generated)

    module = _import_generated(generated, root)
    if not keep_generated:
        generated.unlink(missing_ok=True)

    wf_name = workflow_function_name(flow_class_name(flow_path))
    try:
        return getattr(module, wf_name)
    except AttributeError as exc:  # pragma: no cover - defensive
        raise CompileError(
            f"Generated module {generated.name} has no workflow named {wf_name!r}; "
            f"found: {sorted(n for n in vars(module) if not n.startswith('__'))}"
        ) from exc


def environments(flow_file: os.PathLike | str, **kwargs: Any) -> tuple[Any, ...]:
    """Compile *flow_file* and return the ``TaskEnvironment`` objects to deploy.

    ``flyte.deploy`` takes environments rather than tasks; ``flyte_migrate``
    accumulates one parent environment per generated module as the module is
    imported, so this loads the workflow and then reads them back out.
    """
    load_workflow(flow_file, **kwargs)
    from flyte_migrate._workflow import _parent_envs

    return tuple(_parent_envs.values())


def coerce_inputs(workflow: Any, inputs: dict[str, Any]) -> dict[str, Any]:
    """Convert string inputs to the types the generated workflow declares.

    Metaflow ``Parameter``s become typed inputs on the generated workflow, and
    Flyte 2 type-checks them strictly rather than coercing — so values arriving
    from a command line as strings have to be converted against the signature
    first. Values that are already non-strings are passed through, which is what
    callers of the programmatic API pass.
    """
    if not inputs:
        return {}

    try:
        hints = typing.get_type_hints(workflow.func)
    except Exception:  # pragma: no cover - defensive; fall back to no coercion
        hints = {}

    known = set(inspect.signature(workflow.func).parameters)
    unknown = sorted(set(inputs) - known)
    if unknown:
        raise CompileError(
            f"Unknown flow input(s): {', '.join(unknown)}. "
            f"Available: {', '.join(sorted(n for n in known if n != 'origin_run_id'))}"
        )

    coerced: dict[str, Any] = {}
    for name, value in inputs.items():
        target = hints.get(name)
        if not isinstance(value, str) or target in (None, str, Any):
            coerced[name] = value
        elif target is bool:
            coerced[name] = value.strip().lower() in ("1", "true", "t", "yes", "y", "on")
        elif target in (int, float):
            try:
                coerced[name] = target(value)
            except ValueError:
                raise CompileError(f"Input --{name} expects {target.__name__}, got {value!r}") from None
        else:
            # Metaflow's JSONType parameters and any container type land here.
            try:
                coerced[name] = json.loads(value)
            except ValueError:
                coerced[name] = value
    return coerced


def run(
    flow_file: os.PathLike | str,
    *,
    remote: bool = False,
    inputs: dict[str, Any] | None = None,
    copy_style: str | None = None,
    name: str | None = None,
    follow: bool = False,
    **kwargs: Any,
) -> Any:
    """Compile *flow_file* and run it, locally by default.

    ``flyte.init``/``flyte.init_from_config`` must have been called first, as with
    any other v2 program.
    """
    import flyte

    # Remote runs put every step in its own pod, so a local datastore cannot be
    # the default there; the root itself is resolved at task runtime when unset.
    if remote:
        kwargs.setdefault("datastore", "s3")
    wf = load_workflow(flow_file, remote_ready=remote, **kwargs)
    mode = "remote" if remote else "local"
    style = copy_style or (_REMOTE_COPY_STYLE if remote else "loaded_modules")
    runner = flyte.with_runcontext(mode=mode, copy_style=style, name=name)
    result = runner.run(wf, **coerce_inputs(wf, inputs or {}))
    if remote and follow:
        result.wait()
    return result


def deploy(
    flow_file: os.PathLike | str,
    *,
    version: str | None = None,
    dryrun: bool = False,
    copy_style: str | None = None,
    **kwargs: Any,
) -> Any:
    """Compile *flow_file* and register it on the cluster."""
    import flyte

    envs = environments(flow_file, **kwargs)
    if not envs:  # pragma: no cover - defensive
        raise CompileError(f"No Flyte environments were produced from {flow_file}")
    return flyte.deploy(
        *envs,
        version=version,
        dryrun=dryrun,
        copy_style=copy_style or _REMOTE_COPY_STYLE,
    )
