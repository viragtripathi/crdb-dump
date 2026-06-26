# Contributing

Pull requests are welcome — star ⭐ the repo, file issues, or propose features at
<https://github.com/viragtripathi/crdb-dump/issues>.

## Development setup

```bash
git clone https://github.com/viragtripathi/crdb-dump
cd crdb-dump
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Before opening a PR

- Run unit tests: `pytest -m "not integration"`
- If your change touches export/import behavior, run the integration and
  end-to-end suites too — see [Testing](testing.md).
- Keep commit messages plain and descriptive.

For releases, see [Releasing](releasing.md).
