# Installation

## Prerequisites

- Python 3.12+
- `uv` package manager (recommended) or `pip`

## Quick Install

```bash
# Clone the repository
git clone https://github.com/your-org/coc-pit.git
cd coc-pit

# Install with uv (recommended)
uv sync

# Or install with pip
pip install -e .

# For development (includes pytest, ruff)
uv sync --extra dev
```

## Verify Installation

```bash
# Check CLI is available
coclab --help

# Run tests
pytest tests/test_smoke.py -v
```

## Working Directory

The CLI expects to be run from the CoC-PIT project root directory. If run from a different directory, you'll see a warning:

```
Warning: Current directory may not be the CoC-PIT project root. Missing: pyproject.toml, coclab, data
```

This warning appears when the current directory is missing expected markers (`pyproject.toml`, `coclab/`, `data/`). While commands may still work, file paths assume the project root as the working directory.

---

**Previous:** [[01-Overview]] | **Next:** [[03-Architecture]]
