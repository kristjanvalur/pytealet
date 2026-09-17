---
name: prose
description: >-
  Use when writing or rewriting user-facing docs: README, PYTHON_API.md,
  guides under docs/ or packages/*/docs/, CHANGELOG.md, ROADMAP.md. Also when
  rewriting internal design docs (ARCHITECTURE.md, OPERATION_CALLBACKS.md,
  IO_MANAGER_DESIGN.md, ISSUES.md). Do not load for ordinary code comments,
  docstrings, AGENTS.md, or code-only PRs.
---

# Prose

Comment and docstring style lives in root `AGENTS.md`. This skill is for
markdown people read: README, API guides, changelogs, and internal design
notes.

Shared across both kinds:

- British English, except APIs, filenames, third-party terms, and quoted text
- Clear, technical, conversational; assume competence
- Explain why something is useful, not only what it does
- No passive or apologetic tone; no redundant qualifiers

## User-facing (README, guides, PYTHON_API.md, CHANGELOG, ROADMAP)

- Lead with the problem or use case
- Rhetorical questions only where they help orientation
- Show before-and-after when introducing an improvement
- Let code examples carry the narrative; keep the surrounding prose short
- **Bold** for emphasis, _italics_ for terms, `backticks` for code

Changelogs: scannable section headers; user-facing impact first, then
technical detail; name exact tools and versions; quantify when it matters.

## Internal design docs

Same spelling and tone. Skip rhetorical questions and before-and-after
marketing. State invariants, the choice that was made, and why the
alternative was rejected. Examples only when they pin down a contract.
