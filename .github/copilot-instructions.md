# GitHub Copilot Instructions for pytealet

## Project Overview

`pytealet` is a Python wrapper for
[libtealet](https://github.com/kristjanvalur/libtealet), providing efficient
stack-slicing coroutines to Python through a C extension.

Core intent:
- Keep `_tealet` runtime semantics correct and memory-safe.
- Provide a practical greenlet-compatible shim in `tealet.greenlet`.
- Prioritize deterministic runtime behavior over broad parity claims.

## Current Project Structure

```
pytealet/
├── src/
│   ├── tealet/
│   │   ├── __init__.py
│   │   └── greenlet/
│   │       ├── __init__.py
│   │       └── _greenlet.py
│   ├── _tealet/
│   │   ├── pytealet.c
│   │   ├── pytealet_module.c
│   │   ├── tstate_state.c
│   │   ├── frame_info.c
│   │   ├── pytealet_common.h
│   │   ├── pytealet.h
│   │   ├── pytealet_module.h
│   │   └── libtealet/    # vendored release archive (currently 0.7.6)
│   └── tealet_examples.py
├── tests/
│   ├── test_tealet.py
│   ├── test_greenlet_legacy.py
│   ├── test_examples.py
│   └── compat_greenlet/
├── docs/
│   ├── ARCHITECTURE.md
│   ├── ISSUES.md
│   └── GREENLET_COMPATIBILITY_PLAN.md
├── scripts/
│   └── fast_build.sh
├── pyproject.toml
├── setup.py
└── README.md
```

## Development Environment

Preferred tooling is `uv`.

### Standard dev setup

```bash
uv venv --python 3.13
source .venv/bin/activate
uv sync --active --dev
```

### Debug CPython setup

When using a debug interpreter (`python3.xd`), prefer explicit `uv pip`:

```bash
uv venv --python /path/to/cpython-debug/python .venv-cpython313-debug
uv pip install --python .venv-cpython313-debug/bin/python -e .[dev]
```

When the debug venv is activated, prefer `uv --active` forms.

## Build Workflow

Recommended rapid rebuild:

```bash
./scripts/fast_build.sh
./scripts/fast_build.sh debug
```

Manual rebuild if needed:

```bash
rm -rf build/ src/_tealet*.so
uv sync --active --reinstall-package tealet
```

## Test Workflow

Core suite:

```bash
uv run --active python -m pytest tests/
```

Targeted examples:

```bash
uv run --active python -m pytest tests/test_tealet.py -v
uv run --active python -m pytest tests/test_greenlet_legacy.py -v
```

Upstream compat tests are opt-in and can be expensive:

```bash
PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=1 \
  uv run --active python -m pytest tests/compat_greenlet -v
```

CI defaults to `PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=0`.

## Current Status Snapshot

- Core in-repo test baseline is healthy.
- `_tealet` has active hardening around GC traversal/clear semantics.
- Greenlet compatibility is functional for in-repo coverage, but upstream
  parity is still an ongoing effort.

## Coding Guidelines

### Runtime safety first

- Treat segfaults, aborts, and debug-assert crashes as highest priority.
- Do not hide runtime crashes by skipping tests unless explicitly requested.
- Prefer root-cause fixes over behavioral workarounds.

### C extension practices

- Preserve ownership and refcount invariants.
- Keep changes narrow and localized.
- Add tests for behavior changes when practical.
- Use C89-style comments in C code.

### Vendored libtealet policy

- Do not modify `src/_tealet/libtealet/` unless explicitly asked.
- Treat vendored contents as external dependency snapshots.

## Greenlet Compatibility Guidance

- Keep `tealet.greenlet` behavior aligned with tests in this repo first.
- Use `tests/compat_greenlet/` as opt-in parity tracking.
- Maintain explicit skip reasons for unsupported upstream scenarios.

## Documentation and Change Hygiene

- Update `docs/ARCHITECTURE.md` when design or API contracts change.
- Update `docs/ISSUES.md` for major resolved issues or active hardening work.
- Keep `README.md` focused on accurate setup/run guidance.

## Prose Style Guidelines

When writing documentation, changelog entries, docstrings, or comments, follow this style:

### General Principles
- **Clear and technical**: Write in a clear, direct style that assumes technical competence
- **Conversational yet precise**: Use a friendly, approachable tone while maintaining technical accuracy
- **British English spelling**: Prefer British English spelling in documentation and user-facing prose, unless quoting APIs, filenames, third-party terms, or existing text
- **Explain motivation**: Don't just state what something does--explain why it's useful or what problem it solves
- **Use examples liberally**: Concrete code examples clarify abstract concepts
- **Emphasize with formatting**: Use **bold** for emphasis, _italics_ for terms, and `backticks` for code

### Documentation Style (README, Guides)
- **Lead with the problem**: Start sections by describing the pain point or use case
  - Example: "Did you ever wish that your _coroutines_ started right away...?"
- **Use rhetorical questions**: Engage readers by posing questions they might have
  - Example: "Now they can. Just decorate or convert them with..."
- **Show before-and-after**: When introducing improvements, contrast old approaches with new ones
- **Casual connectives**: Use phrases like "Notice how...", "Needless to say...", "In effect..."
- **Code speaks**: Let code examples carry the narrative, with brief explanatory text
- **Highlight key points**: Use phrases like "__right away__", "__directly__", "__as soon as possible__"

### Changelog Style (CHANGELOG.md)
- **Structured and scannable**: Use clear section headers (Breaking Changes, Build System, Code Modernization)
- **Lead with impact**: State user-facing changes first, then technical details
- **Bullet hierarchy**: Use sub-bullets to provide context and rationale under main points
- **Specific and concrete**: Name exact tools, versions, and what changed
  - Example: "Migrated from Poetry to uv (0.14.0)" not just "Changed build system"
- **Explain reasoning**: Add brief context for why changes were made
  - Example: "Python 3.8 reached end-of-life in October 2024"
- **Quantify when relevant**: Include numbers that show impact
  - Example: "10-100x speedup", "Removed 79 lines of redundant code"

### Code Comments Style
- **Lowercase, conversational**: Comments are lowercase and read naturally
  - Example: `# we can just merge them and don't need to heapify`
- **Explain algorithms**: Describe the reasoning behind implementation choices
  - Example: `# reversed is a heuristic because we are more likely to be looking for`
- **Note tradeoffs**: Mention alternative approaches and why they weren't chosen
  - Example: `# could mark the old entry as removed and re-add a new entry, that will be O(logn) instead of O(n) but lets not worry.`
- **Implementation rationale**: Explain why code is structured a particular way
  - Example: `# use only the __lt__ operator to determine if priority has changed since that is the one used to define priority for the heap`

### Docstrings Style
- **Start with action**: Begin with what the function/class does
  - Example: "Returns True if the coroutine has finished execution"
- **Brief first line**: First line is a concise summary (no "This function..." or "This method...")
- **Add context when needed**: Follow with detailed explanation if the behavior is subtle
- **Keep it minimal**: Don't over-document obvious behavior

### Technical Writing Patterns
- Use "Now they can" rather than "This can now be done"
- Prefer "allows you to" over "enables" or "permits"
- Say "right away" rather than "immediately" for emphasis
- Use "just" to make things sound simple: "just decorate", "just apply it"
- Employ contrast words: "Instead", "Unlike", "Needless to say"
- Phrase improvements as discoveries: "Did you ever wish...", "Now they can"

### Formatting Conventions
- **Bold**: For emphasis and key concepts
- _Italics_: For technical terms on first use, or for subtle emphasis
- `Backticks`: For all code elements (functions, classes, variables, types)
- __Double underscores__: For strong emphasis in Markdown
- Capitalize proper nouns: Python, C#, asyncio (lowercase), Task (when referring to asyncio.Task)

### What to Avoid
- Passive voice: Not "can be done" but "you can do"
- Overly formal: Not "utilizes" but "uses"
- Redundancy: Don't say "mypy type checking" (just "mypy")
- Qualification overkill: Trust the reader's intelligence
- Apologetic tone: Be confident about design decisions.

## References

- libtealet: https://github.com/kristjanvalur/libtealet
- greenlet: https://github.com/python-greenlet/greenlet
- stackman: https://github.com/stackless-dev/stackman
