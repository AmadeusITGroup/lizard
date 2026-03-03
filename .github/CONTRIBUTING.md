# Contributing to LIZARD

Thank you for your interest in contributing to LIZARD! This document provides guidelines and information for contributors.

## How to Contribute

### Reporting Bugs

Please use our [bug report template](.github/ISSUE_TEMPLATE/bug_report.md) to file a bug. Include as much detail as possible: steps to reproduce, expected vs actual behavior, environment details, and any relevant logs or screenshots.

### Suggesting Features

Use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md) to propose new features.

### Submitting Pull Requests

1. Fork the repository
2. Create a feature branch from `main`: `git checkout -b feature/my-feature`
3. Make your changes following our coding standards (see below)
4. Run tests: `make test`
5. Run linting: `make lint`
6. Commit with clear, descriptive messages
7. Push to your fork and open a Pull Request against `main`
8. Fill out the PR template completely

### Development Setup

See the [README.md](README.md) for detailed setup instructions for both the Python backend and React frontend.

**Backend quick start:**

    python -m venv .venv
    source .venv/bin/activate
    pip install -e ".[dev,test]"

**Frontend quick start:**

    cd ui-react
    npm install
    npm run dev

## Coding Standards

### Python

- We use `ruff` for linting and `black` for formatting (configured in `pyproject.toml`)
- Line length: 100 characters
- Target Python version: 3.12+
- Run `make lint-fix` before committing
- Type hints are expected — run `make type` to check
- Maintain test coverage for new code

### TypeScript / React

- Follow the existing code style in `ui-react/`
- Use TypeScript for all new components
- Run `npm run lint` in the `ui-react/` directory

## Code Review

- All pull requests require at least one approving review
- CI checks must pass (lint, tests, type check)
- Address all review comments before merging
- Keep PRs focused — one feature or fix per PR

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://github.com/AmadeusITGroup/.github/blob/main/CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Report unacceptable behavior to opensource@amadeus.com.

## License

By contributing to LIZARD, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).