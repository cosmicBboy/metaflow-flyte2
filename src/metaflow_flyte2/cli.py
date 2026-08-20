"""``pyflyte-metaflow`` — run and deploy Metaflow flows on a Flyte 2 cluster.

Mirrors ``pyflyte-migrate`` (which in turn mirrors ``pyflyte``), but takes the
**Metaflow flow file** as its argument rather than a flytekit module:

.. code-block:: bash

    pyflyte-metaflow run linear_flow.py                       # local
    pyflyte-metaflow run --remote linear_flow.py              # on the cluster
    pyflyte-metaflow deploy --remote linear_flow.py           # register
    pyflyte-metaflow compile linear_flow.py -o generated.py   # inspect the codegen

Compilation and patching happen before anything is imported, so flow files need
no ``import metaflow_flyte2`` line — the same "auto-patch" property that
``pyflyte-migrate`` gives v1 files.
"""

from __future__ import annotations

import functools
import logging
import sys
from pathlib import Path
from typing import Any

import rich_click as click

from metaflow_flyte2._compile import CompileError

_LOG_LEVELS = (None, logging.WARNING, logging.INFO, logging.DEBUG)


def _version() -> str:
    import importlib.metadata

    try:
        return importlib.metadata.version("metaflow-flyte2")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def _parse_key_value(pairs: tuple[str, ...], what: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise click.BadParameter(f"{what} must be KEY=VALUE, got {pair!r}")
        out[key] = value
    return out


def _parse_inputs(args: tuple[str, ...]) -> dict[str, Any]:
    """Parse trailing ``--param value`` / ``--param=value`` pairs into flow inputs.

    Metaflow ``Parameter``s become typed inputs on the generated workflow, and
    Flyte coerces the strings, so no type handling is needed here.
    """
    inputs: dict[str, Any] = {}
    pending: str | None = None
    for arg in args:
        if arg.startswith("--"):
            if pending is not None:
                inputs[pending] = "true"
            key, sep, value = arg[2:].partition("=")
            key = key.replace("-", "_")
            if sep:
                inputs[key] = value
                pending = None
            else:
                pending = key
        elif pending is not None:
            inputs[pending] = arg
            pending = None
        else:
            raise click.BadParameter(f"Unexpected argument {arg!r}; flow inputs must be --name value")
    if pending is not None:
        inputs[pending] = "true"
    return inputs


def _clean_errors(fn):
    """Render compile/config problems as CLI errors rather than tracebacks.

    These are user-fixable conditions — a missing datastore root, an ignore rule
    hiding the generated module — so a stack trace only buries the fix.
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except CompileError as exc:
            raise click.ClickException(str(exc)) from None

    return wrapper


# --- shared options ---------------------------------------------------------

def _compile_options(fn):
    """Options controlling how the Metaflow flow is compiled and containerised."""
    for option in reversed(
        [
            click.option(
                "--datastore",
                type=click.Choice(["local", "s3"]),
                default=None,
                help="Metaflow datastore. Defaults to 'local' locally and 's3' with --remote.",
            ),
            click.option(
                "--datastore-root",
                type=str,
                default=None,
                envvar="METAFLOW_DATASTORE_SYSROOT_S3",
                help="Shared Metaflow datastore root, e.g. s3://bucket/metaflow. Required for --remote.",
            ),
            click.option("-i", "--image", type=str, default=None, help="Container image URI for the step tasks."),
            click.option(
                "--package",
                "packages",
                type=str,
                multiple=True,
                help="Extra pip package for the task image (repeatable) — your flow's own dependencies.",
            ),
            click.option(
                "-e",
                "--env",
                "env_vars",
                type=str,
                multiple=True,
                help="Extra container environment variable, KEY=VALUE (repeatable).",
            ),
            click.option("--cpu", type=str, default=None, help="Default CPU for steps with no @resources (default: 1)."),
            click.option("--memory", type=str, default=None, help="Default memory for steps with no @resources (default: 2Gi)."),
            click.option("--root-dir", type=str, default=None, help="Root of the code bundle. Defaults to the flow's directory."),
            click.option("-o", "--output-file", type=str, default=None, help="Where to write the generated workflow module."),
            click.option("--max-parallelism", type=int, default=None, help="Max parallel tasks for foreach expansions."),
            click.option("--workflow-timeout", type=int, default=None, help="Flow-level timeout in seconds."),
            click.option("--tag", "tags", type=str, multiple=True, help="Tag Metaflow run objects (repeatable)."),
            click.option("--with", "with_decorators", type=str, multiple=True, help="Inject a Metaflow decorator on every step (repeatable)."),
        ]
    ):
        fn = option(fn)
    return fn


def _load_kwargs(
    *,
    datastore: str | None,
    datastore_root: str | None,
    remote: bool,
    image: str | None,
    packages: tuple[str, ...],
    env_vars: tuple[str, ...],
    cpu: str | None,
    memory: str | None,
    root_dir: str | None,
    output_file: str | None,
    project: str | None,
    domain: str | None,
    max_parallelism: int | None,
    workflow_timeout: int | None,
    tags: tuple[str, ...],
    with_decorators: tuple[str, ...],
) -> dict[str, Any]:
    resolved_datastore = datastore or ("s3" if remote else "local")
    if resolved_datastore == "s3" and not datastore_root:
        raise click.UsageError(
            "--datastore-root is required for an s3 datastore (e.g. --datastore-root s3://bucket/metaflow).\n"
            "Every Metaflow step runs in its own pod, so they need a shared datastore to exchange artifacts."
        )
    if remote and resolved_datastore == "local":
        click.echo(
            "Warning: --datastore local with --remote gives each step its own empty datastore; "
            "steps after the first will fail to find their inputs.",
            err=True,
        )
    kwargs: dict[str, Any] = {
        "datastore": resolved_datastore,
        "datastore_root": datastore_root,
        "image": image,
        "packages": packages,
        "env_vars": _parse_key_value(env_vars, "--env"),
        "root_dir": root_dir,
        "output_file": output_file,
        "max_parallelism": max_parallelism,
        "workflow_timeout": workflow_timeout,
        "tags": tags,
        "with_decorators": with_decorators,
    }
    if cpu is not None:
        kwargs["default_cpu"] = cpu
    if memory is not None:
        kwargs["default_memory"] = memory
    if project:
        kwargs["project"] = project
    if domain:
        kwargs["domain"] = domain
    return kwargs


def _init_flyte(ctx: click.Context, project: str | None, domain: str | None, root_dir: str | None, flow_file: Path) -> None:
    """Initialise the v2 SDK, rooting the code bundle where the flow file lives."""
    import flyte

    parent = ctx.find_root().params
    root = Path(root_dir).resolve() if root_dir else Path(flow_file).resolve().parent
    flyte.init_from_config(
        parent.get("config_file"),
        root_dir=root,
        project=project,
        domain=domain,
        log_level=_LOG_LEVELS[min(parent.get("verbose") or 0, 3)],
    )


# --- commands ---------------------------------------------------------------

@click.group(cls=click.RichGroup)
@click.version_option(_version(), prog_name="pyflyte-metaflow")
@click.option("-c", "--config", "config_file", type=click.Path(exists=True), default=None, help="Path to a Flyte 2 config file (defaults to standard discovery).")
@click.option("-v", "--verbose", count=True, help="Increase verbosity (-v, -vv, -vvv).")
@click.option(
    "-o",
    "--output-format",
    type=click.Choice(["table", "table-simple", "json"]),
    default="table",
    help="How to render deployment results.",
)
@click.pass_context
def main(ctx: click.Context, config_file: str | None, verbose: int, output_format: str) -> None:
    """Run Metaflow flows on Flyte 2 — no changes to your flow files.

    Compiles the flow with metaflow-flyte and executes it through the
    flyte-migrate v1-to-v2 shim.
    """
    ctx.ensure_object(dict)


@main.command(context_settings={"ignore_unknown_options": True, "allow_extra_args": True})
@click.option("-r", "--remote", is_flag=True, default=False, help="Run on the Flyte 2 cluster instead of locally.")
@click.option("-p", "--project", type=str, default=None, help="Flyte project.")
@click.option("-d", "--domain", type=str, default=None, help="Flyte domain.")
@click.option("--name", type=str, default=None, help="Name for the run.")
@click.option("--follow", "--wait", is_flag=True, default=False, help="Wait for the run to finish.")
@click.option("--copy-style", type=click.Choice(["all", "loaded_modules", "none"]), default=None, help="Code bundle copy style. Defaults to 'all', which the flow file requires.")
@_compile_options
@click.argument("flow_file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("flow_inputs", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
@_clean_errors
def run(
    ctx: click.Context,
    remote: bool,
    project: str | None,
    domain: str | None,
    name: str | None,
    follow: bool,
    copy_style: str | None,
    flow_file: Path,
    flow_inputs: tuple[str, ...],
    **compile_opts: Any,
) -> None:
    """Compile FLOW_FILE and run it, locally by default.

    Metaflow Parameters are passed after the file name: ``... run flow.py --alpha 0.5``.
    """
    _init_flyte(ctx, project, domain, compile_opts.get("root_dir"), flow_file)
    import metaflow_flyte2

    kwargs = _load_kwargs(remote=remote, project=project, domain=domain, **compile_opts)
    result = metaflow_flyte2.run(
        flow_file,
        remote=remote,
        inputs=_parse_inputs(flow_inputs),
        copy_style=copy_style,
        name=name,
        follow=follow,
        **kwargs,
    )
    url = getattr(result, "url", None)
    if url:
        click.echo(f"Run: {url}")
    if not remote:
        click.echo(f"Outputs: {result.outputs()}")


@main.command()
@click.option("-p", "--project", type=str, default=None, help="Flyte project.")
@click.option("-d", "--domain", type=str, default=None, help="Flyte domain.")
@click.option("--version", type=str, default=None, help="Version to register under; defaults to a content-based version.")
@click.option("--dry-run", "--dryrun", is_flag=True, default=False, help="Do not call the backend.")
@click.option("--copy-style", type=click.Choice(["all", "loaded_modules", "none"]), default=None, help="Code bundle copy style. Defaults to 'all'.")
@_compile_options
@click.argument("flow_file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.pass_context
@_clean_errors
def deploy(
    ctx: click.Context,
    project: str | None,
    domain: str | None,
    version: str | None,
    dry_run: bool,
    copy_style: str | None,
    flow_file: Path,
    **compile_opts: Any,
) -> None:
    """Compile FLOW_FILE and register it on the cluster."""
    _init_flyte(ctx, project, domain, compile_opts.get("root_dir"), flow_file)
    import metaflow_flyte2

    kwargs = _load_kwargs(remote=True, project=project, domain=domain, **compile_opts)
    deployments = metaflow_flyte2.deploy(
        flow_file, version=version, dryrun=dry_run, copy_style=copy_style, **kwargs
    )
    # Reuse the v2 CLI's renderers so the output matches `pyflyte-migrate register`.
    from flyte.cli import _common as common

    fmt = ctx.find_root().params.get("output_format") or "table"
    for deployment in deployments:
        common.print_output(common.format("Environments", deployment.env_repr(), fmt), fmt)
        common.print_output(common.format("Entities", deployment.table_repr(), fmt), fmt)


@main.command("compile")
@click.option("-r", "--remote", is_flag=True, default=False, help="Compile as a remote run would (defaults the datastore to s3).")
@click.option("-p", "--project", type=str, default=None, help="Flyte project baked into the generated module.")
@click.option("-d", "--domain", type=str, default=None, help="Flyte domain baked into the generated module.")
@_compile_options
@click.argument("flow_file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@_clean_errors
def compile_cmd(
    remote: bool,
    project: str | None,
    domain: str | None,
    flow_file: Path,
    **compile_opts: Any,
) -> None:
    """Compile FLOW_FILE to a Flyte 2-ready workflow module and print its path.

    Produces exactly what ``run`` and ``deploy`` produce, so the result can be
    inspected, checked in, or handed to ``pyflyte-migrate run`` directly.
    """
    from metaflow_flyte2.api import compile_workflow

    kwargs = _load_kwargs(remote=remote, project=project, domain=domain, **compile_opts)
    generated, _root, _cfg = compile_workflow(flow_file, **kwargs)
    click.echo(str(generated))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
