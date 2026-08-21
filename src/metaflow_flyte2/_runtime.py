"""Runtime configuration injected into the Flyte 2 task environments.

The generated workflow module runs each Metaflow step as a *subprocess*:

    python <FLOW_FILE> --datastore <TYPE> --metadata local step <name> \
        --run-id ... --task-id ... --input-paths ...

That shape imposes three requirements on the v2 container that neither library
sets up on its own, because neither knows the other exists:

1. ``FLOW_FILE`` is baked in as the *client's* absolute path. Inside the
   container the code bundle is extracted into the working directory, so the
   flow file is reachable by its path relative to the bundle root. The generated
   module already honours ``METAFLOW_FLYTE_FLOW_FILE``, so pointing that at the
   relative path is all that is needed.
2. Metaflow steps hand artifacts to each other through the *datastore*, not
   through Flyte's I/O. Each step is a separate pod, so a ``local`` datastore
   leaves step N unable to read step N-1's artifacts. Remote runs therefore need
   a shared datastore root, which defaults to Flyte's own object store — the one
   bucket the task role is guaranteed to be able to write to.
3. ``metaflow`` itself must be importable by the subprocess interpreter, so it
   has to be a pip package in the task image.

The extra process also shifts what "no resources requested" should mean, which is
why :attr:`RuntimeConfig.default_memory` exists.
"""

from __future__ import annotations

import dataclasses
import os

#: Metaflow's environment variable for each shared-datastore backend.
SYSROOT_VARS = {
    "s3": "METAFLOW_DATASTORE_SYSROOT_S3",
    "gs": "METAFLOW_DATASTORE_SYSROOT_GS",
    "azure": "METAFLOW_DATASTORE_SYSROOT_AZURE",
}

#: Packages every task image needs for the step subprocess to start at all.
#: ``metaflow-flyte`` is required because the generated command line passes
#: ``--with=flyte_internal``, a Metaflow decorator that ships in that package.
BASE_PACKAGES: tuple[str, ...] = ("metaflow", "metaflow-flyte")


@dataclasses.dataclass
class RuntimeConfig:
    """What the glue injects into each v2 ``TaskEnvironment``.

    A module-level singleton rather than a parameter threaded through
    ``flyte_migrate``: the patch points are inside that library's own call
    stack, so there is nowhere to pass it explicitly.
    """

    #: Path to the Metaflow flow file as seen from the container's working
    #: directory. ``None`` leaves the compile-time absolute path in place, which
    #: is correct for local runs.
    flow_file_in_container: str | None = None

    #: Metaflow datastore type used by the step subprocesses ("local" or "s3").
    datastore: str = "local"

    #: Root of the shared datastore, e.g. ``s3://bucket/metaflow``. Optional:
    #: when unset, the generated module derives one from Flyte's own object
    #: store at task runtime (see
    #: :func:`metaflow_flyte2._transform.inject_datastore_resolution`), so the
    #: datastore lands somewhere the task role can already write.
    datastore_root: str | None = None

    #: Extra pip packages layered onto every task image — the flow's own
    #: dependencies, which the glue cannot infer.
    extra_packages: tuple[str, ...] = ()

    #: Extra environment variables for every task container.
    extra_env: dict[str, str] = dataclasses.field(default_factory=dict)

    #: Metaflow username recorded on the run.
    username: str | None = None

    #: Fallback CPU/memory for step tasks whose Metaflow step declares no
    #: ``@resources``. A Flyte task here runs *two* Python processes — the task
    #: runtime and the Metaflow step subprocess it spawns — so the cluster's bare
    #: default is easily too small; a join over a wide foreach OOMKills on it.
    #: 4 GiB is what Metaflow's own remote backends (``@batch``, ``@kubernetes``)
    #: default to, so this keeps a flow's resource profile the same as it would be
    #: on any other Metaflow backend. A step that declares ``@resources`` keeps its
    #: own values — the generated ``_mf_task(**extra)`` merge puts them last.
    default_cpu: str | float | None = 1
    default_memory: str | None = "4Gi"

    def env_vars(self) -> dict[str, str]:
        """Environment variables to set on every task container."""
        env: dict[str, str] = {}
        if self.flow_file_in_container:
            env["METAFLOW_FLYTE_FLOW_FILE"] = self.flow_file_in_container
        if self.datastore != "local":
            env["METAFLOW_DEFAULT_DATASTORE"] = self.datastore
            if self.datastore_root:
                # Left unset on purpose otherwise: the generated module fills it
                # in from Flyte's object store once it is running in a task.
                env[SYSROOT_VARS[self.datastore]] = self.datastore_root
        # Metaflow refuses to run without a user; container images have no
        # passwd entry for the runtime UID, so set it explicitly.
        env["METAFLOW_USER"] = self.username or os.environ.get("USER", "flyte")
        # The step subprocess is non-interactive; Metaflow's update check adds
        # latency and a network dependency for no benefit.
        env["METAFLOW_DISABLE_UPDATE_CHECK"] = "1"
        env.update(self.extra_env)
        return env

    def packages(self) -> tuple[str, ...]:
        """Pip packages every task image needs."""
        pkgs = list(BASE_PACKAGES)
        if self.datastore == "s3":
            # Metaflow's S3 datastore shells out to boto3.
            pkgs.append("boto3")
        pkgs.extend(self.extra_packages)
        # Preserve order while removing duplicates so image layers stay stable.
        return tuple(dict.fromkeys(pkgs))


#: The active configuration. Set by the compile/run entry points on the client;
#: left at defaults inside the container, where the pod spec already carries the
#: env vars and the image is already built.
config = RuntimeConfig()


def set_config(cfg: RuntimeConfig) -> None:
    global config
    config = cfg
