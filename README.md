# Tealet

A greenlet-like library using tealets for cooperative multitasking in Python.

## Project Structure

```
pytealet/
├── src/
│   └── tealet/
│       ├── __init__.py
│       └── greenlet.py
├── tests/
│   ├── test_tealet.py
│   └── test_greenlet.py
├── _tealet.c           # C extension source (to be compiled)
├── pyproject.toml
└── README.md
```

## Installation

This project uses `uv` for package management. To install:

```bash
# Install in development mode
uv pip install -e .

# Install with development dependencies
uv pip install -e ".[dev]"
```

## Development

### Running Tests

```bash
pytest tests/
```

### Building the C Extension

The C extension (`_tealet`) compilation setup will be configured separately.

## License

To be determined

## About

This is a modernized version of the pytealet project, restructured to follow modern Python packaging standards with a src-layout and using uv for dependency management.
