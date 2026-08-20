"""Apply the metaflow-flyte / flyte-migrate composition patches.

``apply()`` is the single entry point, called by ``import metaflow_flyte2`` — the
programmatic path, mirroring ``import flyte_migrate`` — and again by
:func:`metaflow_flyte2.api.load_workflow` before a generated module is imported.

These patches are **client-side only**, and deliberately so. The generated module
is re-imported in two places this process cannot reach: the parent workflow
container, and each task container resolving the task it is about to run. Neither
has this package installed. Everything those imports depend on is therefore
written into the generated source by :mod:`metaflow_flyte2._transform`; what is
left here is the one thing that does travel — the container image, which the
platform resolves by environment name from the cache this client builds.
"""

from __future__ import annotations

from typing import cast

_applied = False


def apply() -> None:
    """Patch ``flytekit`` (via ``flyte_migrate``) and ``flyte_migrate`` itself. Idempotent."""
    global _applied
    if _applied:
        return
    _applied = True

    # Importing flyte_migrate replaces flytekit's @task/@workflow/@dynamic/Deck/...
    # with v2-backed versions. Everything the generated module imports from
    # flytekit is covered by that shim; the patches below only reconcile the two
    # libraries' assumptions about naming, images, and the step subprocess.
    import flyte_migrate  # noqa: F401

    _patch_task_environments()
    _patch_parent_environments()


def _augment(env) -> None:
    """Layer the Metaflow runtime requirements onto a freshly built environment.

    Only the image is handled here. Images are resolved by environment name from
    the cache this client builds, so a client-side change reaches the pods. The
    container environment and default resources are *not* — the parent workflow
    container rebuilds its children's task templates from scratch, keeping only
    what the generated source declares — so those are baked into the source by
    :func:`metaflow_flyte2._transform.inject_task_defaults` instead.
    """
    import flyte

    from metaflow_flyte2._runtime import config

    packages = config.packages()
    if packages and isinstance(env.image, flyte.Image):
        # A user-supplied image URI is a plain string and must be taken as-is —
        # the caller is responsible for baking metaflow into it. Only the
        # SDK-built images are extendable.
        if getattr(env.image, "extendable", False):
            env.image = cast(flyte.Image, env.image).with_pip_packages(*packages)


def _patch_task_environments() -> None:
    """Augment every per-task environment ``flyte_migrate`` builds."""
    import flyte_migrate._task as _task_mod

    original = _task_mod._build_task_environment

    def _build_task_environment(task_function, **kwargs):
        env = original(task_function, **kwargs)
        _augment(env)
        return env

    _task_mod._build_task_environment = _build_task_environment


def _patch_parent_environments() -> None:
    """Augment the per-module parent (workflow) environment.

    ``parent_env_for`` memoises in ``_parent_envs``, so augment only on creation.
    """
    import flyte_migrate._task as _task_mod
    import flyte_migrate._workflow as _wf_mod

    original = _wf_mod.parent_env_for
    seen: set = set()

    def parent_env_for(module):
        env = original(module)
        if env.name not in seen:
            seen.add(env.name)
            _augment(env)
        return env

    _wf_mod.parent_env_for = parent_env_for
    # _task.py imported the symbol directly, so rebind it there too.
    _task_mod.parent_env_for = parent_env_for
