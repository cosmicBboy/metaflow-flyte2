"""Tests that drive the real metaflow-flyte generator and the real v2 shim.

These are slower than :mod:`tests.test_transform` because each one shells out to
Metaflow's CLI, but they are what catches drift in either upstream library — a
renamed generator helper, a changed ``@task`` signature — which unit tests over a
hand-written stub cannot see.

The ``remote`` marker gates the tests that need a live Flyte 2 cluster::

    pytest -m remote --datastore-root s3://bucket/metaflow
"""

import ast
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"

#: One flow per graph shape metaflow-flyte compiles differently.
SHAPES = ["linear_flow", "branch_flow", "condition_flow", "foreach_flow", "param_flow"]


@pytest.fixture
def workdir(tmp_path):
    """A scratch copy of the examples, so generated modules never litter the repo."""
    for flow in EXAMPLES.glob("*.py"):
        if not flow.name.endswith("_flyte2.py"):
            (tmp_path / flow.name).write_text(flow.read_text())
    return tmp_path


@pytest.mark.parametrize("flow", SHAPES)
def test_compiles_to_a_module_flyte2_accepts(workdir, flow):
    """Every generated entity name must satisfy Flyte 2's snake_case rule.

    Reproduces the first failure of the composition: metaflow-flyte's private
    ``_step_*`` names produced ``<module>__step_*_env``, which Flyte 2 rejects.
    """
    from metaflow_flyte2.api import compile_workflow

    generated, _root, _cfg = compile_workflow(
        workdir / f"{flow}.py",
        datastore="s3",
        datastore_root="s3://bucket/metaflow",
    )
    source = generated.read_text()
    ast.parse(source)

    module_slug = generated.stem
    for node in ast.parse(source).body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        decorators = {
            getattr(getattr(d, "func", d), "id", getattr(getattr(d, "func", d), "attr", None))
            for d in node.decorator_list
        }
        if decorators & {"_mf_task", "task", "dynamic"}:
            env_name = f"{module_slug}_{node.name}_env"
            assert "__" not in env_name, f"{env_name} would be rejected by Flyte 2"


def test_generated_module_carries_the_container_settings(workdir):
    """Baked into the source, not attached client-side — see _transform."""
    from metaflow_flyte2.api import compile_workflow

    generated, _root, _cfg = compile_workflow(
        workdir / "linear_flow.py",
        datastore="s3",
        datastore_root="s3://bucket/metaflow",
        default_memory="4Gi",
    )
    source = generated.read_text()
    assert "'METAFLOW_DATASTORE_SYSROOT_S3': 's3://bucket/metaflow'" in source
    assert "'METAFLOW_FLYTE_FLOW_FILE': 'linear_flow.py'" in source
    assert "mem='4Gi'" in source


def test_local_datastore_needs_no_root(workdir):
    from metaflow_flyte2.api import compile_workflow

    generated, _root, _cfg = compile_workflow(workdir / "linear_flow.py")
    assert "DATASTORE_TYPE: str = 'local'" in generated.read_text()


def test_s3_without_a_root_defers_to_runtime(workdir):
    """No root means the module derives one from Flyte's object store in-task.

    The compile-time placeholder must not survive into the output, and the
    generated module must not pin a SYSROOT of its own — otherwise the runtime
    resolution would be skipped as "already configured".
    """
    from metaflow_flyte2._compile import _COMPILE_TIME_PLACEHOLDER
    from metaflow_flyte2.api import compile_workflow

    generated, _root, _cfg = compile_workflow(workdir / "linear_flow.py", datastore="s3")
    source = generated.read_text()

    assert _COMPILE_TIME_PLACEHOLDER not in source
    assert "METAFLOW_DATASTORE_SYSROOT_S3" not in source.split("_MF2_ENVIRONMENT = ")[1].split("\n")[0]
    assert "_mf2_ensure_datastore_root(env)" in source
    assert "DATASTORE_TYPE: str = 's3'" in source


def test_explicit_root_wins_over_runtime_resolution(workdir):
    from metaflow_flyte2.api import compile_workflow

    generated, _root, _cfg = compile_workflow(
        workdir / "linear_flow.py", datastore="s3", datastore_root="s3://explicit/root"
    )
    source = generated.read_text()
    # Present in the baked environment, so the in-task helper sees it already set.
    assert "'METAFLOW_DATASTORE_SYSROOT_S3': 's3://explicit/root'" in source


def test_flow_outside_the_bundle_root_is_rejected(workdir, tmp_path):
    """The flow file has to be shippable, or the container cannot run any step."""
    from metaflow_flyte2 import CompileError
    from metaflow_flyte2.api import compile_workflow

    outside = tmp_path.parent / "outside_flow.py"
    outside.write_text((workdir / "linear_flow.py").read_text())
    with pytest.raises(CompileError, match="outside the code-bundle root"):
        compile_workflow(outside, root_dir=workdir)


def test_workflow_loads_as_a_flyte2_task(workdir):
    """The end of the chain: a Metaflow FlowSpec as a v2 TaskTemplate."""
    import flyte

    from metaflow_flyte2.api import load_workflow

    flyte.init(root_dir=workdir)
    workflow = load_workflow(workdir / "param_flow.py", remote_ready=False)
    assert workflow.name.endswith("param_flow")
    # The Metaflow Parameter survived as a typed workflow input.
    assert "greeting" in workflow.native_interface.inputs


def test_inputs_are_coerced_to_the_declared_types(workdir):
    import flyte

    from metaflow_flyte2.api import coerce_inputs, load_workflow

    flyte.init(root_dir=workdir)
    workflow = load_workflow(workdir / "showcase_flow.py", remote_ready=False)
    coerced = coerce_inputs(workflow, {"count": "7", "label": "x"})
    assert coerced == {"count": 7, "label": "x"}


def test_unknown_input_is_reported_clearly(workdir):
    import flyte

    from metaflow_flyte2 import CompileError
    from metaflow_flyte2.api import coerce_inputs, load_workflow

    flyte.init(root_dir=workdir)
    workflow = load_workflow(workdir / "showcase_flow.py", remote_ready=False)
    with pytest.raises(CompileError, match="Unknown flow input"):
        coerce_inputs(workflow, {"kount": "7"})


@pytest.mark.parametrize("flow", ["linear_flow", "condition_flow", "showcase_flow"])
def test_runs_locally_on_flyte2(workdir, flow):
    """Executes the Metaflow steps for real, through the v2 local controller."""
    import flyte

    import metaflow_flyte2

    flyte.init(root_dir=workdir)
    run = metaflow_flyte2.run(workdir / f"{flow}.py", remote=False)
    # A local v2 run raises on step failure, so reaching here means every
    # Metaflow step exited 0 — including the subprocess spawning and the
    # datastore hand-off between steps.
    assert run is not None


@pytest.mark.remote
def test_runs_on_a_live_cluster(workdir, datastore_root):
    """Fan-out and join across real pods, exchanging artifacts via the datastore.

    With no ``--datastore-root`` this also covers the in-task derivation of the
    default root, which is the path most users take.
    """
    import flyte

    import metaflow_flyte2

    flyte.init_from_config(root_dir=workdir)
    run = metaflow_flyte2.run(
        workdir / "showcase_flow.py",
        remote=True,
        follow=True,
        inputs={"count": 3, "label": "pytest"},
        datastore="s3",
        datastore_root=datastore_root,
    )
    assert run.url
    # follow=True already waited; a failed run would have raised.
