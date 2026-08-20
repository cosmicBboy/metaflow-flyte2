"""Tests for the source transform — the part of the glue with no cluster in it.

The transform is where the two libraries' assumptions are reconciled, so these
cover the specific breakages that were observed against a live Flyte 2 tenant:
invalid environment names, signal handlers on a worker thread, container settings
lost to re-serialization, and Metaflow's nested ``_transition`` artifact.
"""

import ast
import textwrap

import pytest

from metaflow_flyte2._naming import sanitize
from metaflow_flyte2._transform import (
    apply_renames,
    fix_condition_branch,
    inject_prelude,
    inject_task_defaults,
    plan_renames,
    transform,
)

# A miniature stand-in for metaflow-flyte's output: the same decorator funnel,
# the same private task names, and a helper that must not be renamed.
GENERATED = textwrap.dedent(
    '''\
    from __future__ import annotations

    import os
    import signal
    import subprocess

    import flytekit
    from flytekit import Resources, current_context, dynamic, task, workflow

    FLOW_FILE: str = os.environ.get("METAFLOW_FLYTE_FLOW_FILE", '/abs/flow.py')
    _TASK_KWARGS = {'enable_deck': True}


    def _mf_task(**extra):
        return task(**{**_TASK_KWARGS, **extra})


    def _run_cmd(cmd):
        """A helper, not a task: must keep its name."""
        proc = subprocess.Popen(cmd)
        _prev = signal.signal(signal.SIGTERM, None)
        return proc.wait()


    @_mf_task()
    def _mf_generate_run_id(origin_run_id: str = '') -> str:
        return 'x'


    @_mf_task(retries=0)
    def _step_start(run_id: str) -> str:
        # mentions _step_start in a comment and "_step_start" in a string
        return _run_cmd(['echo', '_step_start'])


    @dynamic(**_TASK_KWARGS)
    def _dyn_expand(run_id: str) -> list:
        return [_step_start(run_id=run_id)]


    @workflow
    def my_flow(origin_run_id: str = '') -> None:
        run_id = _mf_generate_run_id(origin_run_id=origin_run_id)
        _step_start(run_id=run_id)
    '''
)


class TestRenames:
    def test_only_decorated_functions_are_renamed(self):
        renames = plan_renames(GENERATED)
        assert renames == {
            "_mf_generate_run_id": "mf_generate_run_id",
            "_step_start": "step_start",
            "_dyn_expand": "dyn_expand",
        }
        # Helpers keep their names; they are never turned into Flyte entities.
        assert "_run_cmd" not in renames
        assert "_mf_task" not in renames

    def test_already_valid_names_are_left_alone(self):
        assert "my_flow" not in plan_renames(GENERATED)

    def test_rename_updates_definitions_and_call_sites(self):
        out = apply_renames(GENERATED, plan_renames(GENERATED))
        assert "def step_start(run_id: str) -> str:" in out
        assert "step_start(run_id=run_id)" in out
        assert "def _step_start" not in out

    def test_strings_and_comments_are_untouched(self):
        out = apply_renames(GENERATED, plan_renames(GENERATED))
        assert "# mentions _step_start in a comment" in out
        assert "'_step_start'" in out

    def test_rename_avoids_colliding_with_an_existing_name(self):
        source = textwrap.dedent(
            """\
            _TASK_KWARGS = {}

            def step_start():
                return 1

            @task
            def _step_start():
                return step_start()
            """
        )
        assert plan_renames(source)["_step_start"] == "step_start_2"

    def test_result_is_valid_python(self):
        ast.parse(apply_renames(GENERATED, plan_renames(GENERATED)))

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("_step_start", "step_start"), ("__a__b__", "a_b"), ("Foo-Bar", "foo_bar")],
    )
    def test_sanitize(self, raw, expected):
        assert sanitize(raw) == expected


class TestPrelude:
    def test_shadows_signal_after_its_import(self):
        out = inject_prelude(GENERATED)
        assert out.index("import signal") < out.index("signal = _Mf2MainThreadOnlySignal()")
        ast.parse(out)

    def test_signal_proxy_is_a_noop_off_the_main_thread(self):
        """The failure this exists to prevent: ValueError on a worker thread."""
        import signal as real_signal
        import threading

        module: dict = {}
        exec(compile(inject_prelude(_MINIMAL_SIGNAL_MODULE), "<gen>", "exec"), module)
        proxy = module["signal"]

        off_thread: list = []
        thread = threading.Thread(target=lambda: off_thread.append(proxy.signal(real_signal.SIGTERM, None)))
        thread.start()
        thread.join()
        assert off_thread == [real_signal.SIG_DFL]

        # On the main thread it still installs for real, and restores cleanly.
        previous = proxy.signal(real_signal.SIGTERM, real_signal.SIG_IGN)
        try:
            assert real_signal.getsignal(real_signal.SIGTERM) is real_signal.SIG_IGN
        finally:
            proxy.signal(real_signal.SIGTERM, previous)

        # Attributes other than signal() pass straight through.
        assert proxy.SIGTERM is real_signal.SIGTERM

    def test_is_idempotent(self):
        once = inject_prelude(GENERATED)
        assert inject_prelude(once) == once

    def test_seeds_run_id_from_the_flyte_execution(self, monkeypatch):
        monkeypatch.setenv("FLYTE_INTERNAL_EXECUTION_ID", "abc123")
        monkeypatch.delenv("METAFLOW_FLYTE_LOCAL_RUN_ID", raising=False)
        exec(compile(inject_prelude(_MINIMAL_SIGNAL_MODULE), "<gen>", "exec"), {})
        import os

        assert os.environ["METAFLOW_FLYTE_LOCAL_RUN_ID"] == "flyte-abc123"


_MINIMAL_SIGNAL_MODULE = "import os\nimport signal\n"


class TestTaskDefaults:
    def test_injects_environment_and_resources(self):
        out = inject_task_defaults(GENERATED, {"METAFLOW_USER": "x"}, cpu=2, memory="4Gi")
        assert "_MF2_ENVIRONMENT = {'METAFLOW_USER': 'x'}" in out
        assert "_MF2_RESOURCES = Resources(cpu='2', mem='4Gi')" in out
        assert "'requests': _MF2_RESOURCES" in out
        assert "'limits': _MF2_RESOURCES" in out
        ast.parse(out)

    def test_injection_lands_after_the_task_kwargs_assignment(self):
        out = inject_task_defaults(GENERATED, {"A": "b"}, memory="1Gi")
        assert out.index("_TASK_KWARGS = {'enable_deck': True}") < out.index("_MF2_ENVIRONMENT")

    def test_per_step_resources_still_win(self):
        """``_mf_task`` merges as ``{**_TASK_KWARGS, **extra}``, so extras override.

        This is what keeps a step's own Metaflow ``@resources`` authoritative once
        the glue has installed cluster-wide defaults.
        """
        source = textwrap.dedent(
            """\
            _TASK_KWARGS = {'enable_deck': True}

            def _mf_task(**extra):
                return task(**{**_TASK_KWARGS, **extra})
            """
        )
        out = inject_task_defaults(source, {"A": "b"}, memory="1Gi")
        namespace: dict = {"task": lambda **kw: kw, "Resources": lambda **kw: kw}
        exec(compile(out, "<gen>", "exec"), namespace)

        # No per-step override: the injected default applies.
        assert namespace["_mf_task"]()["requests"] == {"mem": "1Gi"}
        # With one: the step's own request wins.
        assert namespace["_mf_task"](requests={"cpu": "8"})["requests"] == {"cpu": "8"}

    def test_is_idempotent(self):
        once = inject_task_defaults(GENERATED, {"A": "b"}, memory="1Gi")
        assert inject_task_defaults(once, {"A": "b"}, memory="1Gi") == once

    def test_missing_task_kwargs_is_an_error_not_a_silent_drop(self):
        with pytest.raises(ValueError, match="_TASK_KWARGS"):
            inject_task_defaults("x = 1\n", {"A": "b"})

    def test_nothing_to_inject_is_a_noop(self):
        assert inject_task_defaults(GENERATED, {}) == GENERATED


class TestConditionBranch:
    SOURCE = textwrap.dedent(
        """\
        def _read_condition_branch(run_id, step_name, task_id, attempt=0):
            try:
                _transition = _tds['_transition']
                if isinstance(_transition, (list, tuple)) and _transition:
                    return str(_transition[0])
                return str(_transition)
            except Exception:
                return ''
        """
    )

    def test_unwraps_metaflow_2_19_nested_transition(self):
        out = fix_condition_branch(self.SOURCE)
        namespace = {"_tds": {"_transition": (["high"], None)}}
        exec(compile(out, "<gen>", "exec"), namespace)
        assert namespace["_read_condition_branch"](1, 2, 3) == "high"

    def test_still_handles_the_flat_list_form(self):
        out = fix_condition_branch(self.SOURCE)
        namespace = {"_tds": {"_transition": ["low"]}}
        exec(compile(out, "<gen>", "exec"), namespace)
        assert namespace["_read_condition_branch"](1, 2, 3) == "low"

    def test_noop_when_upstream_has_changed(self):
        assert fix_condition_branch("def other(): pass\n") == "def other(): pass\n"


class TestTransform:
    def test_end_to_end_output_is_valid_and_complete(self):
        out = transform(GENERATED, {"METAFLOW_USER": "x"}, cpu=1, memory="4Gi")
        ast.parse(out)
        assert out.startswith("# Post-processed by metaflow_flyte2")
        assert "def step_start" in out
        assert "_Mf2MainThreadOnlySignal" in out
        assert "_MF2_ENVIRONMENT" in out

    def test_unchanged_source_gets_no_banner(self):
        already = transform(GENERATED, {"A": "b"}, memory="1Gi")
        assert transform(already, {"A": "b"}, memory="1Gi").count("# Post-processed") == 1
