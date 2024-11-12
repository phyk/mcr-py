pipx install poetry
pipx install pre-commit

pre-commit install
poetry config virtualenvs.in-project true
poetry lock
poetry install
poetry shell
maturin develop
