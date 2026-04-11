## Recipe Fixtures

Synthetic recipe files for pytest coverage of recipe execution.

Use this directory for small, deterministic recipes that run against
temporary parquet fixtures created inside tests. These are not
user-facing examples; keep them narrowly scoped to contract checks.

Rules:
- Keep paths relative, typically under `data/`, so tests can execute the
  recipe against a temporary project root.
- Prefer tiny fixtures with visible expected truth tables in the test
  module.
- Avoid network, curated-repo dependencies, or large artifact graphs.
