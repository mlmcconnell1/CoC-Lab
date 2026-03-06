"""Tests for artifact inventory and non-interactive CLI behavior."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from typer.testing import CliRunner

from coclab.cli.main import app
from coclab.registry.schema import RegistryEntry

runner = CliRunner()


def _create_build(base: Path, name: str = "demo") -> Path:
    build_dir = base / "builds" / name
    (build_dir / "data" / "curated" / "panel").mkdir(parents=True, exist_ok=True)
    (build_dir / "data" / "curated" / "xwalks").mkdir(parents=True, exist_ok=True)
    (build_dir / "data" / "raw").mkdir(parents=True, exist_ok=True)
    (build_dir / "base").mkdir(parents=True, exist_ok=True)

    manifest = {
        "schema_version": 1,
        "build": {"name": name, "years": [2024]},
        "base_assets": [],
        "aggregate_runs": [],
    }
    (build_dir / "manifest.json").write_text(json.dumps(manifest))
    return build_dir


class TestListArtifacts:
    def test_list_help_includes_artifacts(self):
        result = runner.invoke(app, ["list", "--help"])
        assert result.exit_code == 0
        assert "artifacts" in result.output

    def test_list_artifacts_json(self, tmp_path: Path):
        with runner.isolated_filesystem(temp_dir=tmp_path):
            build_dir = _create_build(Path("."), "demo")

            # Build-local artifacts
            pd.DataFrame({"geo_id": ["CO-500"], "year": [2024], "pit_total": [10]}).to_parquet(
                build_dir / "data" / "curated" / "panel" / "panel__Y2024-2024@B2024.parquet",
                index=False,
            )
            pd.DataFrame({"coc_id": ["CO-500"], "county_fips": ["08031"], "area_share": [1.0]}).to_parquet(
                build_dir / "data" / "curated" / "xwalks" / "xwalk__B2024xC2023.parquet",
                index=False,
            )

            # Global artifacts
            pit_dir = Path("data/curated/pit")
            pit_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame({"coc_id": ["CO-500"], "pit_year": [2024], "pit_total": [10]}).to_parquet(
                pit_dir / "pit__P2024.parquet",
                index=False,
            )

            acs_dir = Path("data/curated/acs")
            acs_dir.mkdir(parents=True, exist_ok=True)
            pd.DataFrame({"tract_fips": ["08031000100"], "total_pop": [5000]}).to_parquet(
                acs_dir / "acs5_tracts__A2023xT2023.parquet",
                index=False,
            )

            result = runner.invoke(app, ["list", "artifacts", "--build", "demo", "--json"])

        assert result.exit_code == 0
        json_start = result.output.find("{")
        assert json_start >= 0
        payload = json.loads(result.output[json_start:])
        assert payload["status"] == "ok"
        assert payload["build"] == "demo"
        assert payload["count"] >= 4

        roles = {(a["scope"], a["role"]) for a in payload["artifacts"]}
        assert ("build", "panel") in roles
        assert ("build", "crosswalk") in roles
        assert ("global", "pit") in roles
        assert ("global", "acs") in roles


class TestNonInteractiveMode:
    @patch("coclab.registry.registry.list_boundaries")
    @patch("coclab.registry.registry.delete_vintage")
    def test_delete_entry_requires_yes_when_non_interactive(
        self,
        mock_delete,
        mock_list,
    ):
        mock_list.return_value = [
            RegistryEntry(
                boundary_vintage="2024",
                source="hud_exchange",
                ingested_at=datetime.now(UTC),
                path=Path("data/curated/coc_boundaries/coc__B2024.parquet"),
                feature_count=1,
                hash_of_file="abc",
            )
        ]

        result = runner.invoke(
            app,
            ["--non-interactive", "registry", "delete-entry", "2024", "hud_exchange"],
        )

        assert result.exit_code == 2
        assert "Non-interactive mode requires '--yes'" in result.output
        mock_delete.assert_not_called()
