"""Drive the composition from your own script, the way ``flyte_migrate`` does.

``import metaflow_flyte2`` applies the shim; from there you hold a normal Flyte 2
task and can use the v2 API directly::

    python programmatic.py            # local
    python programmatic.py --remote   # on the cluster
"""

import os
import sys
from pathlib import Path

import flyte

import metaflow_flyte2

HERE = Path(__file__).parent

# Optional. Left unset, the flow uses Flyte's own object store, which the task
# role can always write to. Set it only to keep artifacts somewhere specific —
# and note it must be writable by the *Flyte task role*, not by your local
# credentials.
DATASTORE_ROOT = os.environ.get("METAFLOW_DATASTORE_SYSROOT_S3")


def main(remote: bool) -> None:
    flyte.init_from_config(root_dir=HERE)

    if remote:
        # Every step is its own pod, so they need a shared Metaflow datastore.
        workflow = metaflow_flyte2.load_workflow(
            HERE / "showcase_flow.py",
            datastore="s3",
            datastore_root=DATASTORE_ROOT,
        )
        run = flyte.with_runcontext(mode="remote", copy_style="all").run(
            workflow, count=5, label="programmatic"
        )
        print("Run:", run.url)
        run.wait()
    else:
        workflow = metaflow_flyte2.load_workflow(HERE / "showcase_flow.py", remote_ready=False)
        run = flyte.with_runcontext(mode="local").run(workflow, count=5, label="programmatic")
        print("Outputs:", run.outputs())


if __name__ == "__main__":
    main(remote="--remote" in sys.argv)
