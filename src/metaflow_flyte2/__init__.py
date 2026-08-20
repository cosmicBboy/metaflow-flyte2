"""Run Metaflow flows on Flyte 2.

Composes two libraries that each solve half the problem:

* `metaflow-flyte <https://github.com/npow/metaflow-flyte>`_ compiles a Metaflow
  ``FlowSpec`` into a **Flyte 1 (flytekit)** workflow module, preserving Metaflow's
  execution model — every step runs as a Metaflow subprocess against a Metaflow
  datastore, so versioning, artifacts, and ``Task``/``Run`` APIs keep working.
* `flyte-migrate <https://github.com/flyteorg/flyte-migrate>`_ shims flytekit's
  namespace so a Flyte 1 module executes on **Flyte 2** infrastructure.

Chaining them gets you from ``FlowSpec`` to a Flyte 2 run, and this package is the
glue that makes the chain hold: it reconciles the generated entity names with
Flyte 2's naming rules, and teaches the v2 task environments what the Metaflow
step subprocess needs — the flow file, a shared datastore, and Metaflow itself in
the image.

Importing this module applies the shim, exactly as ``import flyte_migrate`` does::

    import flyte
    import metaflow_flyte2

    flyte.init_from_config()
    metaflow_flyte2.run("linear_flow.py", remote=True,
                        datastore="s3", datastore_root="s3://bucket/metaflow")

The ``pyflyte-metaflow`` CLI wraps the same calls with a ``pyflyte``-style UX and
applies the patches for you, so flow files need no import line at all.
"""

from metaflow_flyte2._compile import CompileError, compile_flow
from metaflow_flyte2._patch import apply
from metaflow_flyte2._runtime import RuntimeConfig
from metaflow_flyte2.api import compile_workflow, deploy, environments, load_workflow, run

apply()

__all__ = [
    "CompileError",
    "RuntimeConfig",
    "apply",
    "compile_flow",
    "compile_workflow",
    "deploy",
    "environments",
    "load_workflow",
    "run",
]

__version__ = "0.1.0"
