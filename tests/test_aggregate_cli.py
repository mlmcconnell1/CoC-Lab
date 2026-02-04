"""Tests for the ``coclab aggregate`` CLI command group."""

from pathlib import Path

from typer.testing import CliRunner

from coclab.cli.main import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Help output
# ---------------------------------------------------------------------------


def test_aggregate_help_shows_subcommands():
    result = runner.invoke(app, ["aggregate", "--help"])
    assert result.exit_code == 0
    for name in ("acs", "zori", "pep", "pit"):
        assert name in result.output


# ---------------------------------------------------------------------------
# Build validation
# ---------------------------------------------------------------------------


def test_aggregate_pep_missing_build():
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["aggregate", "pep", "--build", "nonexistent"])
        assert result.exit_code == 2
        assert "Build 'nonexistent' not found" in result.output


def test_aggregate_pit_missing_build():
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["aggregate", "pit", "--build", "nonexistent"])
        assert result.exit_code == 2
        assert "Build 'nonexistent' not found" in result.output


def test_aggregate_acs_missing_build():
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["aggregate", "acs", "--build", "nonexistent"])
        assert result.exit_code == 2
        assert "Build 'nonexistent' not found" in result.output


def test_aggregate_zori_missing_build():
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["aggregate", "zori", "--build", "nonexistent"])
        assert result.exit_code == 2
        assert "Build 'nonexistent' not found" in result.output


# ---------------------------------------------------------------------------
# Alignment validation
# ---------------------------------------------------------------------------


def _create_build(name: str = "demo") -> None:
    """Create a minimal build directory for testing."""
    build_dir = Path("builds") / name
    (build_dir / "data" / "curated").mkdir(parents=True)
    (build_dir / "data" / "raw").mkdir(parents=True)
    (build_dir / "base").mkdir(parents=True)
    (build_dir / "manifest.json").write_text('{"schema_version": 1}\n')


def test_aggregate_pep_invalid_align():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(
            app, ["aggregate", "pep", "--build", "demo", "--align", "bad_mode"]
        )
        assert result.exit_code == 2
        assert "Invalid alignment mode 'bad_mode' for pep" in result.output


def test_aggregate_pit_invalid_align():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(
            app, ["aggregate", "pit", "--build", "demo", "--align", "bad_mode"]
        )
        assert result.exit_code == 2
        assert "Invalid alignment mode 'bad_mode' for pit" in result.output


def test_aggregate_acs_invalid_align():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(
            app, ["aggregate", "acs", "--build", "demo", "--align", "bad_mode"]
        )
        assert result.exit_code == 2
        assert "Invalid alignment mode 'bad_mode' for acs" in result.output


def test_aggregate_zori_invalid_align():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(
            app, ["aggregate", "zori", "--build", "demo", "--align", "bad_mode"]
        )
        assert result.exit_code == 2
        assert "Invalid alignment mode 'bad_mode' for zori" in result.output


# ---------------------------------------------------------------------------
# Stub output (valid inputs produce "Would aggregate" + "Not yet implemented")
# ---------------------------------------------------------------------------


def test_aggregate_pep_stub_output():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(app, ["aggregate", "pep", "--build", "demo"])
        assert result.exit_code == 1  # stub exits with 1
        assert "Would aggregate pep for build 'demo' with alignment 'as_of_july'" in result.output
        assert "Not yet implemented" in result.output


def test_aggregate_pit_stub_output():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(app, ["aggregate", "pit", "--build", "demo"])
        assert result.exit_code == 1
        assert "Would aggregate pit" in result.output
        assert "point_in_time_jan" in result.output


def test_aggregate_acs_stub_output():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(app, ["aggregate", "acs", "--build", "demo"])
        assert result.exit_code == 1
        assert "Would aggregate acs" in result.output
        assert "vintage_end_year" in result.output


def test_aggregate_zori_stub_output():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(app, ["aggregate", "zori", "--build", "demo"])
        assert result.exit_code == 1
        assert "Would aggregate zori" in result.output
        assert "monthly_native" in result.output


# ---------------------------------------------------------------------------
# --years parsing
# ---------------------------------------------------------------------------


def test_aggregate_pep_with_years():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(
            app, ["aggregate", "pep", "--build", "demo", "--years", "2020-2022"]
        )
        assert result.exit_code == 1
        assert "[2020, 2021, 2022]" in result.output


def test_aggregate_pep_with_invalid_years():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(
            app, ["aggregate", "pep", "--build", "demo", "--years", "bad"]
        )
        assert result.exit_code == 2


# ---------------------------------------------------------------------------
# Dataset-specific options
# ---------------------------------------------------------------------------


def test_aggregate_pep_lagged_requires_lag_years():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(
            app, ["aggregate", "pep", "--build", "demo", "--align", "lagged"]
        )
        assert result.exit_code == 2
        assert "--lag-years is required" in result.output


def test_aggregate_pep_lagged_with_lag_years():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(
            app,
            ["aggregate", "pep", "--build", "demo", "--align", "lagged", "--lag-years", "1"],
        )
        assert result.exit_code == 1
        assert "lag-years: 1" in result.output


def test_aggregate_acs_with_acs_vintage():
    with runner.isolated_filesystem():
        _create_build()
        result = runner.invoke(
            app,
            ["aggregate", "acs", "--build", "demo", "--acs-vintage", "2019-2023"],
        )
        assert result.exit_code == 1
        assert "acs-vintage: 2019-2023" in result.output
