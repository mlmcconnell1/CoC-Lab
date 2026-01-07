"""Export bundle utilities for CoC-PIT."""

from coclab.export.codebook import (
    VARIABLE_DESCRIPTIONS,
    generate_schema_md,
    generate_variables_csv,
    write_codebook,
)
from coclab.export.hashing import compute_sha256, hash_bundle_files, verify_file_hash
from coclab.export.manifest import (
    build_artifact_entry,
    build_manifest,
    extract_parquet_metadata,
    get_coclab_info,
    get_zillow_attribution,
    write_manifest,
)
from coclab.export.readme import generate_readme, write_readme
from coclab.export.selection import (
    build_selection_plan,
    select_diagnostics,
    select_inputs,
    select_panel,
)
from coclab.export.types import (
    ArtifactRecord,
    BundleConfig,
    ManifestSchema,
    SelectionPlan,
)
from coclab.export.validate import (
    ExportValidationError,
    run_all_validations,
    validate_panel_exists,
    validate_panel_schema,
    validate_selection_plan,
    validate_vintage_compatibility,
)

__all__ = [
    "ArtifactRecord",
    "BundleConfig",
    "ExportValidationError",
    "ManifestSchema",
    "SelectionPlan",
    "VARIABLE_DESCRIPTIONS",
    "build_artifact_entry",
    "build_manifest",
    "build_selection_plan",
    "compute_sha256",
    "extract_parquet_metadata",
    "generate_readme",
    "generate_schema_md",
    "generate_variables_csv",
    "get_coclab_info",
    "get_zillow_attribution",
    "hash_bundle_files",
    "run_all_validations",
    "select_diagnostics",
    "select_inputs",
    "select_panel",
    "validate_panel_exists",
    "validate_panel_schema",
    "validate_selection_plan",
    "validate_vintage_compatibility",
    "verify_file_hash",
    "write_codebook",
    "write_manifest",
    "write_readme",
]
