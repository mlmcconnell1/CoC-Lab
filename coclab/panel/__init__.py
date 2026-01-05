"""Panel assembly module for CoC Lab Phase 3.

This module provides tools for constructing CoC x year panels by aligning
PIT years with boundary vintages and ACS vintages according to explicit
policies, plus diagnostics for validating panel integrity.
"""

from coclab.panel.assemble import (
    build_panel,
    save_panel,
)
from coclab.panel.diagnostics import (
    DiagnosticsReport,
    boundary_change_summary,
    coverage_summary,
    generate_diagnostics_report,
    missingness_report,
    weighting_sensitivity,
)
from coclab.panel.policies import (
    AlignmentPolicy,
    DEFAULT_POLICY,
    default_acs_vintage,
    default_boundary_vintage,
)

__all__ = [
    "AlignmentPolicy",
    "DEFAULT_POLICY",
    "DiagnosticsReport",
    "boundary_change_summary",
    "build_panel",
    "coverage_summary",
    "default_acs_vintage",
    "default_boundary_vintage",
    "generate_diagnostics_report",
    "missingness_report",
    "save_panel",
    "weighting_sensitivity",
]
