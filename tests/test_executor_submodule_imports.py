"""Regression tests for executor submodule imports (coclab-l6be).

The executor decomposition landed in commit fdcdfe6 with five new
modules plus ``executor_manifest``.  Each submodule originally imported
shared primitives back from ``coclab.recipe.executor`` while the
orchestrator eagerly re-imported symbols from the partially-initialized
submodule, producing a partial-initialization ImportError whenever a
caller imported a submodule directly (without going through
``coclab.recipe.executor`` first).

This test verifies every extracted submodule — including the shared
``executor_core`` — can be imported on its own in a fresh interpreter,
so the cycle cannot silently return.  Subprocesses are used so prior
imports of ``coclab.recipe.executor`` elsewhere in the session can't
mask a regression.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

# Submodule import order matters here only for test-ID readability: the
# shared base is listed first, then the orchestrator, then each
# extracted leaf module.
EXECUTOR_SUBMODULES: list[str] = [
    "coclab.recipe.executor_core",
    "coclab.recipe.executor",
    "coclab.recipe.executor_transforms",
    "coclab.recipe.executor_manifest",
    "coclab.recipe.executor_inputs",
    "coclab.recipe.executor_ct_alignment",
    "coclab.recipe.executor_resample",
    "coclab.recipe.executor_panel",
    "coclab.recipe.executor_panel_policies",
    "coclab.recipe.executor_persistence",
]


@pytest.mark.parametrize("module", EXECUTOR_SUBMODULES)
def test_executor_submodule_imports_directly(module: str) -> None:
    """Each executor submodule must load in a fresh interpreter.

    Runs ``python -c "import <module>"`` in a subprocess so the test
    reflects the real "first import" scenario.  If the partial-init
    cycle regresses, the subprocess exits non-zero with an ImportError
    naming the offending symbol.
    """
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"Direct import of {module} failed — probable executor import "
        f"cycle regression (coclab-l6be).\n"
        f"stdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
