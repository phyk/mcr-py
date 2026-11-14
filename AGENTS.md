# AGENTS.md

## Project
Structured multi-modal routing from N origins to N destinations. Multi-objective: for each node and each mode, stores the Pareto front of (cost, time).

## Layout
- `python/mcr_py/` – Python package (`mcr5`, `gtfs`, `osm`, `visualize`, `structs`, `utils`, ...). `_mcr_py.pyi` holds type stubs for the Rust extension.
- `src/` (+ root `Cargo.toml`, crate `_mcr_py`) – PyO3 binding code only; exposes Rust as `mcr_py._mcr_py`. Adapters live in `src/rs/`.
- `mcr-rust/` – separate crate with the core routing logic (network, mcr5, gtfs, pricing, I/O). Depends on the `mlc` sub-library.
- `mcr-rust/mlc/` – multi-label-correcting library (Pareto label handling) used by `mcr-rust`.
- `osmtools/` – OSM helper crate, also used by the bindings.
- `notebooks/` – runnable analysis scripts/config; `test/` – Python tests.

## Build / run
`uv` is used for syncing and building.
- Rebuild the extension after Rust changes: `TMPDIR=/home/ppeter/repo/mcr-py/tmp uv run maturin develop`
- Run a script while rebuilding the Rust core in one step: `uv run --reinstall python /path/to/notebook.py` (or a scratchpad script path).
