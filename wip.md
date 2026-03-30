# WIP report: exploratory crash-debug work

## Context and goal

This branch captures exploratory work to diagnose deep crashes likely related to stack save/restore behavior, tealet switching, and Python thread-state handling.

Primary questions we were chasing:

- How could saved stacks appear to be overwritten?
- Are partial-stack save boundaries and growth logic correct?
- Are we preserving Python thread/interpreter state safely across switches?

---

## 1) Python binding refactor and diagnostics (`src/_tealet/pytealet.c`)

### A. Tealet/Python thread-state work

We introduced a formal `PyTealetTstate` abstraction and helper operations to make ownership and move semantics explicit:

- `PyTealetTstate_Init`
- `PyTealetTstate_Capture`
- `PyTealetTstate_Restore`
- `PyTealetTstate_IncRef`
- `PyTealetTstate_DecRef`

And we wired save/restore paths through those helpers.

This was meant to reduce ambiguity around frame/exception/context transfer between active tealet state and `PyThreadState`, and to make symmetry between create/switch/dealloc easier to reason about.

### B. Runtime diagnostics in the binding layer

Additional debug instrumentation was added around switching/stub execution:

- Optional debug logging via `TEALET_DEBUG`/`LOG`.
- Extra stack-boundary diagnostics in `stub_main`.
- Calls to `tealet_validate_stack(...)` after selected switches to catch overwritten unsaved regions sooner.

---

## 2) Local libtealet exploratory modifications (`src/_tealet/libtealet/src/tealet.c`)

This is the largest exploratory area. Key themes:

### A. Stack safety and overwrite detection

- Added per-tealet magic cookies to detect use-after-free/corruption (`TEALET_MAGIC_ALIVE`, `TEALET_MAGIC_FREED`).
- Added optional forced full-save toggle (`TEALET_FORCE_FULL_SAVE`) to compare behavior against incremental-save mode.
- Added guard bytes for grown chunks and checks for guard corruption (overflow detection).

### B. Stack snapshot consistency probes

- Added debug `initial_copy` snapshots for saved stacks in debug builds.
- Added comparisons of chunk contents against expected snapshot segments.
- Added pre-restore and post-restore integrity checks of chunk ordering, contiguity, and restored data.

### C. Partial-save chain correctness checks

- Added validation for stacks in the `g_prev` chain before restore.
- Added focused diagnostics in and around stack growth/list traversal.
- The exploratory notes in code indicate a previously suspected issue in `tealet_stack_grow_list` (target-handling / early-return behavior), then further instrumentation to verify assumptions.

### D. Public/diagnostic helper

- Added `tealet_validate_stack(...)` helper to validate unsaved stack areas against expected backup data (debug builds), to detect corruption earlier and closer to source.

---

## 3) Build and packaging workflow changes (`setup.py`, vendored trees)

### A. Source-vs-prebuilt libtealet path

`setup.py` was updated to support building libtealet from source (with debug-friendly flags) and to fallback to prebuilt binaries when requested.

### B. Vendored content growth

We added/expanded local trees:

- `src/_tealet/libtealet/src/*` and docs/tests/CI metadata
- `src/_tealet/libtealet-bin/*` with stackman/platform assets

This gave us direct local control for instrumentation and crash probing, at the cost of a much larger working diff.

---

## 4) Debug tooling and scripts

Added helper scripts and debugger configs:

- `scripts/build_asan.sh` for ASan builds
- `scripts/debug_test.sh` to run focused tests under gdb
- `debug_crash.gdb`, `debug_test.gdb`

These were used to reproduce and inspect hard failures with tighter loops.

---

## 5) Test-suite exploratory changes

### A. Focused random-stress diagnostics

`tests/test_tealet.py` got extra tracing prints in the random stress path (`TestRandom2`) to expose run/switch chronology and target selection while chasing crash timing.

### B. Clone-path focus reduction

In current test harness behavior, `newmode = 0` means default creation path is `tealet_new_rnd` (non-clone path), while clone/stub duplication paths remain implemented (`stub_new2`, `stub_new3`) but are not used by default in this exploration mode.

This effectively deprioritized clone-based stress paths while we investigated baseline stack-save/restore stability.

---

## 6) Current caveats in this snapshot

- The branch intentionally contains exploratory/debug-oriented code and verbosity.
- Some generated/debug artifacts are currently committed (for example temp/binary helper outputs); these should be cleaned before a production-quality series.
- There are mixed concerns (core runtime changes + tooling + vendoring + docs) in one snapshot commit, by design for fast iteration.

---

## 7) Suggested next cleanup/debug sequence

1. Keep this snapshot as the immutable exploration baseline.
2. Reproduce crash on a minimal deterministic test target.
3. Compare behavior with incremental-save vs forced full-save mode.
4. Narrow instrumentation to the smallest checks that still catch corruption.
5. Split into clean commit series:
   - Runtime correctness fixes
   - Optional diagnostics
   - Build/tooling updates
   - Test adjustments
6. Remove generated artifacts from git and tighten ignore rules.

---

## Snapshot references

- Branch: `exploration/tstate-wip-snapshot`
- Baseline commit for this report: `1675fad`

---

## 8) Latest delta (2026-03-22): assert analysis + central macro control

### A. `tealet_stack_growto` assert diagnosis

We analyzed the abort at:

- `src/_tealet/libtealet/src/tealet.c:tealet_stack_growto`
- assert: `STACKMAN_SP_DIFF(stack->stack_far, stack->chunk.stack_near) > saved`

Interpretation: this assert expects `tealet_stack_growto()` to run only for bounded stacks that are still partially unsaved.

Observed failure mode in local instrumentation logs:

- a stack in `g_prev` could already be effectively fully saved (`unsaved=0`) but still remain linked as if partial,
- then a later grow attempt on that entry hit the assert.

### B. Fix applied for local minimum-save heuristic path

In `tealet_stack_saveto`, after applying the optional minimum-initial-save heuristic, we now explicitly mark bounded stacks as `full` when effective saved size reaches the bounded span.

This closes the bookkeeping gap where a full save could be treated as partial.

### C. Macro-gating and explicit local-change markers

`src/_tealet/libtealet/src/tealet.c` now clearly tags pytealet-specific divergence points as `PYTEALET_LOCAL_CHANGE` and guards them with macros.

Added/used toggles:

- `TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS`
- `TEALET_PYTEALET_ENABLE_MAGIC_COOKIES`
- `TEALET_PYTEALET_VALIDATE_PRE_RESTORE`
- `TEALET_PYTEALET_MIN_INITIAL_SAVE`
- existing `TEALET_FORCE_FULL_SAVE`

Default values are set to upstream-like behavior (`0`) unless intentionally enabled.

### D. Centralized compile-time control file

Added:

- `src/_tealet/pytealet_build_config.h`

`setup.py` now force-includes this header for:

1. libtealet source builds (`BUILD_LIBTEALET_FROM_SOURCE=1` path), and
2. `_tealet` extension compilation.

This allows centralized test-variant control without editing multiple source files.

### E. Validation after this delta

- `./scripts/fast_build.sh` succeeds.
- Focused regression:
   - `tests/test_tealet.py::TestSwitch::test_switch_new` passes.
   - focused set (`TestModule`, `TestSimple`, `TestStatus::test_status_run`, `TestSwitch::test_switch_new`) passes (`6 passed`).

### F. Note on upstream comparison

Quick compare against `/mnt/d/git/libtealet/src/tealet.c` indicates the specific local failure path is tied to pytealet-side heuristic/instrumentation divergence, not present as-is in upstream source.

---

## 9) Latest delta (2026-03-23): heap-saved integrity verification (exclude heap corruption)

Goal of this round: verify whether data we park/save on heap is being mutated unexpectedly, so we can either confirm or exclude heap corruption as root cause.

### A. Binding-layer heap integrity instrumentation (`src/_tealet/pytealet.c`)

Added targeted diagnostics around saved ranges and allocations:

- Per-tealet saved-heap snapshot metadata:
   - `dbg_saved_heap_has_snapshot`
   - `dbg_saved_heap_addr`
   - `dbg_saved_heap_len`
   - `dbg_saved_heap_hash`
- Env toggles:
   - `PYTEALET_HEAP_SAVED_WATCH`
   - `PYTEALET_HEAP_SAVED_ABORT`
- Heap allocation guards in allocator wrappers:
   - head/tail canaries (`DBG_HEAP_MAGIC_HEAD`, `DBG_HEAP_MAGIC_TAIL`)
   - tracked block list
   - guard verification on free
- Range classifier helper:
   - `dbg_heap_range_in_single_block(...)` ensures watched ranges are truly inside one tracked heap allocation.

### B. Watcher safety refinements

Initial watcher versions could touch unstable/live ranges and crash early. We narrowed scope to avoid false/unsafe reads:

- checksum only parked and uninvolved tealets,
- skip ranges that are not tracked heap blocks,
- emit explicit skip reason (`reason=range_not_heap`) instead of hashing anyway.

### C. Repro and result summary

Deterministic repro remained seed-6 (`TestRandom2Minimal::test_random`) under debug instrumentation.

Most informative run (failfast-any disabled to allow watcher progress) produced:

- `HEAP_ABORTS:0`
- `HEAP_SKIPS:792`
- `ALLOC_GUARD_HITS:0`
- process still ended with segfault (`EXIT:139`).

Interpretation of counters:

- no observed guard-canary corruption in tracked heap blocks,
- no confirmed mutation of verified heap-backed saved ranges,
- many candidate ranges were actually stack-address ranges and correctly skipped.

### D. Current interpretation and caveat

Working conclusion from this pass: we currently have no positive evidence that heap-stored saved data is being corrupted by allocator/heap writes.

Corruption signal still aligns more with parked-tealet saved-chain/cframe validity issues (including stale/invalid chain traversal), not a demonstrated heap overwrite.

Caveat: this excludes the checked heap-corruption class, but does not yet prove the exact mutator for the parked-chain corruption.

### E. Next experiment queued

Next discriminator: force full-save behavior in `tealet.c` (including main boundary handling if possible) and compare first-failure signature/timing against incremental-save mode.

---

## 10) Latest delta (2026-03-23): full-save vs incremental-save A/B

### A. Runtime toggle for full-save experiment

To avoid rebuild churn per mode, `src/_tealet/libtealet/src/tealet.c` now supports runtime override:

- env: `PYTEALET_FORCE_FULL_SAVE`
- behavior: if enabled and current stack is bounded, `save_limit` is forced to `g_current->stack_far`
- diagnostics: `[SAVE_SCOPE]` now prints `force_full=<0|1>`

Default behavior remains tied to existing compile-time `TEALET_FORCE_FULL_SAVE` unless env override is set.

### B. Deterministic A/B setup

Common settings (both modes):

- seed-6 minimal repro: `tests/test_tealet.py::TestRandom2Minimal::test_random`
- `PYTEALET_FAR_PAD_BYTES=0`
- `TEALET_PAGE_GUARD=1`
- same random limits used in prior deterministic runs.

Modes compared:

- incremental: `PYTEALET_FORCE_FULL_SAVE=0`
- forced full-save: `PYTEALET_FORCE_FULL_SAVE=1`

### C. A/B outcome with failfast-any enabled

With `PYTEALET_CFRAME_FAILFAST_ANY=1`, both modes abort at the same first invalid saved-chain signature:

- phase: `run-after-transfer-before-restore`
- reason: `bad_use_tracing`
- failing owner: parked/saved tealet (`id=2` in this trace set)
- exit: `134` (abort)

Observed difference was only expected `save_limit` selection (`force_full=1` uses bounded current `stack_far`); first invalid-chain signature remained unchanged.

### D. A/B outcome with failfast-any disabled

With `PYTEALET_CFRAME_FAILFAST_ANY=0`, both modes still ended in `SIGSEGV` (`EXIT:139`).

The earliest visible invalid-chain marker in both modes is again `bad_use_tracing` for the same parked/saved-chain context before eventual segfault.

### E. Interpretation update

For this seed and configuration, forcing full-save (via save-limit forcing) does not change the first corruption signature nor eliminate the crash.

This weakens (but does not fully eliminate) the hypothesis that incremental-save boundary choice alone is the primary trigger.

---

## 11) Simplified read of logs (2026-03-23): what happens step-by-step

### A. Tealets involved (from seed-6 trace)

- `id=1`: main tealet (unbounded stack far = `STACKMAN_SP_FURTHEST` in logs as `0xffff...fff0`)
- `id=2`: first child tealet created by main (bounded stack; this is first failing parked owner)
- later ids (`3+`): deeper descendants during random recursion.

### B. Concrete sequence in plain terms

1. Main (`id=1`) runs and creates/switches to `id=2`.
2. Main is saved (`[SAVE_RESULT] ... full=0`) because main is unbounded and can only be incrementally parked.
3. `id=2` later switches back toward main; with full-save mode enabled, `id=2` save shows:
   - `[SAVE_SCOPE] ... force_full=1 ... save_limit=current_far`
   - `[SAVE_RESULT] ... full=1 ... saved=4848`
4. Immediately after transfer (before restore), parked `id=2` chain inspection hits:
   - `chain_valid=0`, `bad_reason=bad_use_tracing`
   - failfast run: `[CFRAME_FAILFAST_ANY] ... action=abort`
   - non-failfast run: same marker appears, then eventual `SIGSEGV`.

### C. Memory regions implicated (from the same failing lines)

For failing parked `id=2` in full-save mode:

- `saved_near=0x7ffe...3907`
- `saved_far =0x7ffe...4b70`
- `saved_cframe/head=0x7ffe...3b60`
- status flags during inspection: `in_live=0`, `in_saved=1`.

Interpretation of those flags:

- the inspected cframe pointer is not inside the *currently active* stack window,
- but it is inside the parked tealet's saved-address range.

### D. Key reasoning: why can “full save” still show corruption?

Most likely explanation from current evidence:

- We are validating parked cframe chains by dereferencing raw cframe pointers while the tealet is parked.
- Those pointers are stack addresses, not relocated handles.
- While parked, those addresses can contain unrelated live data from currently running execution.
- So `bad_use_tracing` observed at this phase can be a *parked-pointer interpretation hazard*, not proof that heap-saved bytes were overwritten.

This is consistent with:

- heap-watch/guard checks finding no heap corruption,
- full-save vs incremental showing same first signature,
- failing checks happening specifically in `run-after-transfer-before-restore` / parked-owner dumps.

### E. What this means for the “are we corrupting the live stack?” question

Based on current logs, there is no direct proof yet that we are actively corrupting the currently live stack memory.

What is directly shown is that parked-owner cframe traversal sees invalid values before restore. That can happen if we inspect stack-address pointers at parked time, even without an actual write corruption of the saved heap copy.

So at this point:

- **proven:** parked-chain inspection sees invalid cframe values,
- **not yet proven:** live-stack overwrite as root cause,
- **still open:** an actual restore-path corruption bug vs diagnostics traversing pointers in an invalid phase.

### F. Correction adopted

Important correction: after transfer (stack-pointer switch), we must **not** inspect/walk cframe chains for parked tealets (e.g. `id=2` in this trace). Their saved cframe pointers are stack-address pointers and are not valid for post-transfer dereference until that tealet is active again.

Applied in diagnostics:

- `log_switch_cframes(...)` now computes chain hash / failfast-any only for active owner (`prev_owner`).
- `log_cframe_chain_short(..., "next-saved", ...)` now logs `chain_walk=skipped reason=not_active` instead of traversing.

Validation after applying correction:

- seed-6 run with `PYTEALET_CFRAME_FAILFAST_ANY=1` no longer aborts at the old `run-after-transfer-before-restore` parked-owner (`id=2`) check.
- failfast now triggers later at an active-chain check:
   - phase: `before-save`
   - owner: `id=1` (active/main in that point)
   - reason: `out_of_bounds_obj`
   - representative bad frame: heap-like pointer (`0x612f...`) outside active stack object bounds.

This is a cleaner signal than the previous transfer-window parked-pointer failures.

### G. Deeper-chain jump clarification (user hypothesis validated)

Observation: a deeper-chain jump to heap-like addresses did not necessarily mean the stub-local copy (`previous=NULL`) was broken.

What was happening in diagnostics:

1. `cframe_chain_hash(...)` originally required every traversed cframe object to be inside active stack window.
2. CPython `root_cframe` is part of `PyThreadState` (heap-resident), so this produced false `out_of_bounds_obj` when chains legitimately reached root.
3. Some checks were still running in transfer-before-restore windows, where active `tstate` can be transient and not yet a stable post-restore state.

Fixes applied:

- `cframe_chain_hash(...)` now treats `tstate->root_cframe` as valid terminal when supplied.
- active-chain hashing/failfast runs only when:
   - owner is the actual active owner,
   - head is in active live stack object bounds,
   - and active owner is in restored state (`has_state==0`).

Result:

- Deterministic seed-6 run with `PYTEALET_CFRAME_FAILFAST_ANY=1` now completes successfully (`1 passed`) under this diagnostic model (`seed6-activeonly-failfast1-v5.log`).

This strongly supports the correction: parked/transitional cframe walking was generating false corruption signals.

### H. Policy update: validate only after restore completes

Adopted rule from this point forward:

- do not run cframe/stack validation in pre-save, post-save, transfer, or pre-restore phases,
- run validation only in explicit `after-restore` phases.

Implementation result in `pytealet.c`:

- `dbg_validate_all_tealets(...)` now returns unless phase contains `after-restore`.
- `dbg_failfast_validate_active_cframe(...)` now returns unless phase contains `after-restore`.
- active chain hashing in `log_switch_cframes(...)` is similarly gated to `after-restore` and active/restored owner conditions.

Verification:

- deterministic seed-6 run with validation toggles enabled (`PYTEALET_CFRAME_FAILFAST_ANY=1`, `PYTEALET_CFRAME_FAILFAST=1`, `PYTEALET_VALIDATE_ALL_STACKS=1`) completed with `EXIT:0`.
- log confirms `[STACK_VALIDATE_ALL]` appears only at `py-run-after-restore` phases.

---

## 12) Latest delta (2026-03-24): pre-restore noise cleanup, wider reruns, and artifact cleanup

### A. Diagnostic noise cleanup in binding path

Following the policy above, we removed remaining transient pre-restore inspection callsites from `src/_tealet/pytealet.c`:

- removed pre-restore compare in `restore_tstate` path,
- removed run-path transfer-before-restore validate/failfast/compare/log hooks,
- removed switch-path after-switch-before-restore validate/failfast/compare/log hooks.

`restore_tstate` now explicitly logs skip intent in diagnostics:

- `chain_walk=skipped reason=transient_pre_restore_state`

This keeps signal focused on stable post-restore ownership windows.

### B. Wider rerun summary (current environment)

Using debug venv + page guard, bounded stress reruns were green:

- `TestRandom2Minimal` seeds `0..20` (bounded params): pass,
- `TestRandom1::test_random`: pass,
- `TestRandom2::test_random` (bounded params): pass.

### C. Workspace hygiene

Removed stale generated root artifacts from this crash-debug cycle:

- old `*.log` and `gdb-*.txt` outputs,
- temporary `gdb` command files used for targeted runs,
- stale scratch summary file.

### D. Snapshot

- branch: `exploration/tstate-wip-snapshot`
- checkpoint commit created for this delta: `e4d3a68`

---

## 13) Branch handoff plan executed (2026-03-25)

Goal: preserve validated crash-fix work and libtealet enhancements for ongoing development, while creating clean handoff branches and avoiding accidental loss.

### A. Safety snapshot created

- backup branch: `backup/bughunt-current`
- backup tag: `backup-bughunt-20260325`
- points to: `cce0c17`

### B. Significant pytealet fix commit captured

Committed from exploration branch:

- `a101bde` — `pytealet: gate and preserve crash fixes via build toggles`

This includes the validated fix toggles in:

- `src/_tealet/pytealet.c`
- `src/_tealet/pytealet_build_config.h`

### C. libtealet enhancement branch (clean, master-based)

Created:

- `topic/libtealet-enhancements`

Commits on this branch:

- `171a5da` — Add libtealet source tree prerequisite
- `9bca580` — boundary-hint enhancement
- `ccc77eb` — Linux page-guard debug mode

Notes:

- `master` in this repo is binary-oriented for vendored libtealet; source-level enhancement commits require the source-tree prerequisite.
- One conflict in `src/_tealet/libtealet/src/tealet.c` (macro block near file top) was resolved by keeping the local instrumentation block.

### D. pytealet fix branch

Created:

- `topic/pytealet-fixes` at `a101bde`

Reasoning:

- The pytealet fix commit is validated against the modern exploration line and does not apply cleanly to `master`'s older `_tealet` layout.

### E. Integration branch for ongoing work

Created:

- `integration/stable-fixes`

Base and applied commit:

- base: `cce0c17`
- applied: `638dddb` (cherry-pick of `a101bde`)

This branch is the recommended continuation branch for near-term development because it contains:

- the modern working `_tealet` layout,
- the validated pytealet crash-fix toggles,
- the same behavior used in the 2x2 matrix validation.

### F. Validation snapshot tied to this handoff

2x2 matrix results (already generated under `tmp/bughunt/`) still stand:

- either individual mitigation prevented crashes in tested probes,
- disabling both reproduces crashes strongly.

### G. Practical branch usage

- Use `integration/stable-fixes` for day-to-day feature/fix work.
- Keep `topic/libtealet-enhancements` as the source-level libtealet patch branch.
- Keep backup pointers (`backup/bughunt-current`, `backup-bughunt-20260325`) untouched as rollback anchors.

---

## 14) Revised branch model executed (2026-03-25)

User-requested strategy: keep a `master`-based branch alive for in-repo libtealet/source-style work and local libtealet enhancements, then layer pytealet fixes as the active development line.

### A. New long-lived base branch (master-derived)

Created:

- `base/libtealet-inrepo-enhanced`

Applied sequence on top of `master`:

- `a24c72d` — add in-repo libtealet source tree prerequisite
- `8baf5b2` — boundary hint support
- `6b32ab4` — Linux page-guard debug mode

This branch is intended to stay alive as the ongoing local libtealet/source baseline.

### B. New development branch layered on base

Created:

- `development` (from `base/libtealet-inrepo-enhanced`)

Applied pytealet fix layer:

- `3455e63` — `pytealet: gate and preserve crash fixes via build toggles`

Conflict resolution notes for this transplant:

- took pytealet-side versions of `src/_tealet/pytealet.c` and `src/_tealet/pytealet_build_config.h` from the fix lineage.

### C. Build-plumbing alignment for the revised topology

After layering onto the master-derived base, additional alignment was required:

- `setup.py` now defaults to `BUILD_LIBTEALET_FROM_SOURCE=1` and supports prebuilt fallback via `src/_tealet/libtealet-bin`.
- `setup.py` force-includes `src/_tealet/pytealet_build_config.h` for both libtealet source build and extension compilation.
- `src/_tealet/pytealet.c` includes a compatibility wrapper for `tealet_validate_stack` so this branch links cleanly when that diagnostic symbol is unavailable in the base libtealet surface.

### D. Sanity validation on `development`

Validation run after the above alignment:

- `./scripts/fast_build.sh` ✅
- `uv run pytest tests/test_tealet.py::TestSwitch::test_switch_new -q` ✅ (`1 passed`)
- `uv run pytest tests/test_tealet.py::TestModule tests/test_tealet.py::TestSimple tests/test_tealet.py::TestStatus::test_status_run tests/test_tealet.py::TestSwitch::test_switch_new -q` ✅ (`6 passed`)

Status: revised two-layer branch model is now in place and operational.
