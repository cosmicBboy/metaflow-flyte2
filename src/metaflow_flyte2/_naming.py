"""Name normalisation shared by the source transform.

``metaflow-flyte`` emits private-looking task functions — ``_step_start``,
``_mf_generate_run_id`` — because they are implementation details of the module
it generates. ``flyte_migrate`` names each v2 ``TaskEnvironment``
``f"{module_slug(module)}_{fn.__name__}_env"``, so a leading underscore produces
``linear_gen__mf_generate_run_id_env``: a double underscore, which Flyte 2 rejects
with "must be in snake_case or kebab-case format".

:mod:`metaflow_flyte2._transform` uses :func:`sanitize` to rename those functions
in the generated source. Deliberately not a runtime monkeypatch: the same module
is re-imported inside the containers, where a client-side patch would not be
loaded, so a patch would paper over the client failure and leave the remote one.
"""

from __future__ import annotations

import re

_SANITIZE_RE = re.compile(r"[^0-9a-zA-Z]+")


def sanitize(name: str) -> str:
    """Collapse runs of non-alphanumerics to single underscores and lowercase.

    Mirrors ``flyte_migrate._workflow.module_slug`` so both halves of an
    environment name are normalised the same way.
    """
    return _SANITIZE_RE.sub("_", name).strip("_").lower()
