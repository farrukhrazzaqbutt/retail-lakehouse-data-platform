## Summary

<!-- What does this PR change and why? -->

## Test plan

- [ ] `python scripts/run_ci_local.py` (or `make ci-local`)
- [ ] `pytest` passes locally
- [ ] Spark tests run when Java is available (`pytest -m spark`)
- [ ] Lint/format checks pass (`ruff check`, `ruff format --check`)
- [ ] Documentation updated when behavior changes

## Checklist

- [ ] No secrets or generated data committed
- [ ] Config changes documented in README or phase docs
- [ ] New scripts include `--help` and logging options
