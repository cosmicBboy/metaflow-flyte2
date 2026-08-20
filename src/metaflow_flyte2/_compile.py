"""Drive ``metaflow-flyte``'s code generator and post-process its output.

``metaflow-flyte`` exposes code generation as a Metaflow CLI subcommand
(``python my_flow.py flyte create``), not as a library call: the compiler needs a
fully initialised Metaflow context — datastore, environment, decorators, resolved
``@project``/config values — which Metaflow only builds while running its own
CLI. Shelling out is therefore the supported entry point, and it also keeps the
flow's imports out of this process, where ``flytekit`` has already been shimmed
into v2 and would change how the flow's own decorators behave.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from metaflow_flyte2._transform import transform


class CompileError(RuntimeError):
    """Raised when ``flyte create`` fails; carries the generator's own output."""


def workflow_function_name(flow_name: str) -> str:
    """CamelCase flow class name -> the generated workflow function's name.

    Mirrors ``metaflow_extensions.flyte.plugins.flyte._codegen._wf_fn``. Kept as a
    local copy so importing this module does not drag in metaflow-flyte's
    internals, which pull in Metaflow itself.
    """
    return re.sub(r"(?<!^)([A-Z])", r"_\1", flow_name).lower()


def flow_class_name(flow_file: Path) -> str:
    """Name of the ``FlowSpec`` subclass defined in *flow_file*.

    Parsed from the source rather than imported: importing the flow here would run
    it under a process whose ``flytekit`` is already shimmed, and Metaflow flows
    routinely execute code at module scope.
    """
    import ast

    tree = ast.parse(flow_file.read_text())
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for base in node.bases:
            base_name = base.attr if isinstance(base, ast.Attribute) else getattr(base, "id", None)
            if base_name == "FlowSpec":
                return node.name
    raise CompileError(f"No FlowSpec subclass found in {flow_file}")


def compile_flow(
    flow_file: os.PathLike | str,
    output_file: os.PathLike | str | None = None,
    *,
    datastore: str = "local",
    datastore_root: str | None = None,
    image: str | None = None,
    project: str = "flytesnacks",
    domain: str = "development",
    max_parallelism: int | None = None,
    workflow_timeout: int | None = None,
    tags: Sequence[str] = (),
    with_decorators: Sequence[str] = (),
    python_executable: str | None = None,
    extra_args: Sequence[str] = (),
    container_env: dict[str, str] | None = None,
    default_cpu: str | float | None = None,
    default_memory: str | None = None,
) -> Path:
    """Compile a Metaflow flow into a Flyte-2-ready workflow module.

    ``container_env`` is baked into every generated task's ``environment=``
    argument, and ``default_cpu``/``default_memory`` into its ``requests=``/
    ``limits=``; that is how the step subprocess learns where the flow file and
    datastore are, and gets enough memory, once it is running in a pod.

    Returns the path to the generated file. The file is a v1 ``flytekit`` module —
    it becomes a v2 workflow only once ``flyte_migrate``'s shim is active, which
    :func:`metaflow_flyte2.load_workflow` takes care of.
    """
    flow_path = Path(flow_file).resolve()
    if not flow_path.exists():
        raise CompileError(f"Flow file not found: {flow_path}")

    out_path = (
        Path(output_file).resolve()
        if output_file is not None
        else flow_path.with_name(f"{flow_path.stem}_flyte2.py")
    )
    if out_path == flow_path:
        raise CompileError("output_file must differ from the flow file")

    # Metaflow's own flags come before the subcommand; metaflow-flyte's after it.
    cmd: list[str] = [python_executable or sys.executable, str(flow_path)]
    cmd += ["--datastore", datastore, "--no-pylint", "--quiet"]
    cmd += ["flyte", "create", "--output-file", str(out_path)]
    cmd += ["--project", project, "--domain", domain]
    if image:
        cmd += ["--image", image]
    if max_parallelism is not None:
        cmd += ["--max-parallelism", str(max_parallelism)]
    if workflow_timeout is not None:
        cmd += ["--workflow-timeout", str(workflow_timeout)]
    for tag in tags:
        cmd += ["--tag", tag]
    for deco in with_decorators:
        cmd += [f"--with={deco}"]
    cmd += list(extra_args)

    env = dict(os.environ)
    if datastore == "s3":
        if not datastore_root:
            raise CompileError("datastore='s3' requires datastore_root (e.g. s3://bucket/prefix)")
        # The generator only records the datastore type; the root is read from the
        # environment both here and, via the injected task env vars, at run time.
        env["METAFLOW_DATASTORE_SYSROOT_S3"] = datastore_root

    result = subprocess.run(
        cmd,
        cwd=str(flow_path.parent),
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise CompileError(
            f"`flyte create` failed for {flow_path.name} (exit {result.returncode})\n"
            f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
        )
    if not out_path.exists():
        raise CompileError(f"`flyte create` reported success but wrote no file at {out_path}")

    out_path.write_text(
        transform(out_path.read_text(), container_env or {}, default_cpu, default_memory)
    )
    return out_path
