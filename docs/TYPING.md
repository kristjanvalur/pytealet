# Typing policy

This repository uses **gradual typing**: library **users** see accurate types
on the public surface, while **internals** may omit annotations when that keeps
hot paths simple. CI runs [ty](https://docs.astral.sh/ty/) (`make check`); we
do **not** enable mypy-style “every function must be fully annotated” strictness.

## Goals

1. **Public API is typed.** Callers and editors get useful completions and
   checks via package sources plus `py.typed`.
2. **Internals stay free of typing tax.** Private helpers need not restate types
   that only exist for the checker, and must not pay runtime costs for
   annotations that are not needed.
3. **Trust boundaries are explicit.** Where our code alone establishes an
   invariant (for example CQE `user_data` is always our waitable), prefer
   `assert` (see root `AGENTS.md`) over `typing.cast` on hot paths.

## What counts as the boundary

**Annotate carefully:**

- Public classes and methods (no leading `_` on the name, or names exported in
  `__all__` / documented API modules)
- `Protocol` and `TypedDict` definitions used by callers
- Module-level factories and helpers that appear in docs (`PYTHON_API.md`,
  package READMEs)
- User-visible exceptions and result types

**May be lightly typed or untyped:**

- Names with a leading `_` (private methods, module helpers)
- Proactor ring recipes (`_sq_*`), CQE complete/deliver helpers, freelist and
  cargo plumbing
- Local variables inside hot loops when inference is enough

Users do not import those symbols; omitting annotations there does not remove
types from `UringProactor.send` or `scheduler.io.sock_sendall`.

## Checker behaviour (ty)

ty is gradual by default:

- Missing parameter/return annotations are allowed.
- Diagnostics fire when code **contradicts** an annotation or a solid inference
  (`invalid-return-type`, `unresolved-attribute`, and similar).

We intentionally do **not** require a config that forces annotations on every
def (unlike mypy `disallow_untyped_defs` under `--strict`). Policy is enforced
by review and by keeping public methods annotated, not by turning off type
checking for whole modules.

## Runtime and `typing.cast`

- **Annotations** (with `from __future__ import annotations` where used) do not
  rebuild complex types on every call.
- **`typing.cast(T, x)` is a real function call.** Evaluating an expensive `T`
  (for example `Foo[T] | Bar[U]`) on a hot path is a performance bug. Prefer:
  - no cast (correct public types or untyped private code),
  - `assert isinstance(...)` for internal trust boundaries, or
  - a **module-level** prebound alias if a cast truly cannot be avoided.
- Do not introduce cast-shaped helpers whose only job is `return x  # type: ignore`.

## Trust zones (examples)

In `tealetio`’s uring proactor:

| Zone | Practice |
|------|----------|
| Public `Proactor` methods | Full annotations |
| Ring-leg cargo (`sq0`…`cq3`) | `Any`; prepare owns layout |
| `_sq_*` submit helpers | Forward slots; no `cast` |
| CQE `user_data` / continuous complete | `assert isinstance` then use |

That mirrors the *effect* of how applications experience asyncio (rich types at
the boundary, dynamic guts), while keeping types in our public methods rather
than only in external stubs.

## What library users need

1. Packages ship **`py.typed`** where applicable.
2. **Public** methods and types stay annotated in the `.py` sources (or in
   `.pyi` if we ever split stubs — not required by default).
3. CI `ty check` keeps the tree consistent; it does not demand annotations on
   every private helper.

## Related

- Root `AGENTS.md` — coding guidelines, `assert` for internal contracts, quality
  workflow
- `packages/tealetio/AGENTS.md` — package-local notes
- [ty configuration](https://docs.astral.sh/ty/reference/configuration/) — rules
  and path overrides (use sparingly; prefer policy over silencing whole modules)
