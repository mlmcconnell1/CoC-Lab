"""Panel assembly module for CoC Lab Phase 3.

This module provides tools for constructing CoC x year panels by aligning
PIT years with boundary vintages and ACS vintages according to explicit
policies.
"""

from coclab.panel.policies import (
    AlignmentPolicy,
    DEFAULT_POLICY,
    default_acs_vintage,
    default_boundary_vintage,
)

__all__ = [
    "AlignmentPolicy",
    "DEFAULT_POLICY",
    "default_acs_vintage",
    "default_boundary_vintage",
]
