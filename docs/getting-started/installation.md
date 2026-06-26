# Installation

```bash
pip install crdb-dump
```

Requires **Python 3.10+**.

## From source (development)

```bash
git clone https://github.com/viragtripathi/crdb-dump
cd crdb-dump
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Verify the install:

```bash
crdb-dump version
```
