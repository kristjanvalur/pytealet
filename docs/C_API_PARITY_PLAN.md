# C API Surface Review and Parity Plan

Date: 2026-06-08
Branch: c-api

## Goal

Design a stable parallel C API architecture that tracks the meaningful Python API surface without blindly cloning Python-only conveniences.

## Current Surface Snapshot

### Python module-level API (_tealet)

- current()
- main()
- previous()
- thread_reap(cleanup_passes=..., kill_exc=...)
- thread_sweep()
- thread_active()
- thread_kill(cleanup_passes=..., kill_exc=...)
- error_was_remote()
- hide_frame(callable, args=(), kwargs=None)
- frame_introspection([enabled])

### Python tealet methods

- stub()
- duplicate()
- current()
- previous()
- main()
- is_foreign()
- prepare(function)
- run(function, arg=None)
- switch(arg=None, panic=False)
- set_exception(exception, fallback=None)
- throw(exception)

### Python tealet properties

- state
- frame
- context (get/set)
- thread_id

### Capsule C API currently exported

- ctx_new/ctx_free
- current
- main
- previous
- thread_active
- thread_kill
- thread_reap
- thread_sweep
- error_was_remote
- frame_introspection_get/frame_introspection_set
- check_tealet
- create
- duplicate
- stub
- prepare (py or c callable mode)
- run (py or c callable mode)
- switch_(target, arg, flags)
- throw_(target, exception, flags)
- set_exception(target, exception, fallback)
- is_foreign(target)
- state_get(target, state_out)
- thread_id_get(target, thread_id_out)

## Gap Analysis

### Closed parity gaps

Implemented in current capsule API:
- Thread lifecycle/control: thread_active/thread_kill/thread_reap/thread_sweep
- Exception injection: set_exception and throw_ with flags
- Diagnostics/control knobs: error_was_remote and frame_introspection get/set
- Module traversal parity: previous()
- Execution flags: switch_ now uses flags (including panic)
- Metadata helpers: is_foreign/state_get/thread_id_get

### Remaining intentional non-parity

- hide_frame remains Python-only.
- Method-style tealet.current()/main()/previous() remain Python conveniences.
- Frame/context property manipulation remains Python-level (direct C API surface deferred).

Note:
- Method-style tealet.current(), tealet.previous(), and tealet.main() are treated as
  Python convenience accessors and are not required parity targets for the capsule C API.

## Naming Recommendation

Use plain names for traversal helpers in Python and C surfaces:
- current
- main
- previous

Do not introduce get_current/get_main/get_previous aliases unless there is a
compatibility requirement.

Rationale:
- Existing _tealet module API already uses plain current/main and now previous.
- Python stdlib and ecosystem patterns vary, but preserving local consistency is
  more valuable than retrofitting get_* naming.
- Avoids parallel synonym sets that complicate docs and ABI evolution.

### Likely Python-only (do not force C parity)

1. hide_frame:
- This is a CPython frame-shaping helper with shim-specific purpose.
- Exporting to C API risks hard-coding implementation details into ABI.

2. Python call-shape-specific ergonomics:
- Keyword parsing and nuanced Python TypeError wording should stay Python wrapper concerns.

## Overlaps and Potential API Smells (Python side)

1. throw vs set_exception + switch or run:
- throw is a first-class primitive in the C API and should support flags.
- set_exception remains necessary for return-path semantics when a top-level
  function exits by sending an exception to the target.

2. switch(panic=...) in Python vs flags in C:
- Keyword-driven mode toggles are natural in Python but awkward in C ABI.
- C should use a flags bitmask on switch_ for forward-compatible option growth.

3. current/main duality (module-level and method-level):
- Method-level accessors are convenience-oriented for Python ergonomics.
- C parity keeps module-level current/main only; method-style current/main/previous are out of scope.

4. run special-case when pending throw exists:
- Behavior is correct but subtle.
- Must be documented explicitly in C API contract to avoid client surprises.

## Parity Strategy

### Implemented parity tiers

- Tier 1 (control plane) is implemented.
- Tier 2 (switch flags) is implemented.
- Tier 3 (metadata helpers) is implemented.

### Current strategy focus

- Keep Python and C wrappers aligned on shared dispatch/impl blocks.
- Continue negative testing for unknown flags and type misuse.
- Keep deferred frame/context C API exposure out of scope unless a concrete
  downstream need appears.

## ABI and Layout Plan

1. ABI policy:
- Keep PYTEALET_CAPI_ABI_VERSION at 1 for now (pre-release; no external ABI
  commitment yet).
- Reset any in-progress higher version markers back to 1.
- Until 0.1.0 is published, ABI layout may evolve (including slot reordering)
  while we converge on the first stable shape.
- Once 0.1.0 is published, freeze slot order and use append-only table growth
  for compatible ABI evolution.

2. Runtime code layout policy:
- Keep operation-local grouping in pytealet.c:
  - shared impl or dispatch
  - Python wrapper
  - C API wrapper

3. Module table forwarding:
- Keep forwarders in pytealet_module.c minimal and mechanical.
- Keep behavior in pytealet.c to avoid duplicated semantics.

## Architecture Review: Complementary API Layout

Goal:
- Keep Python and capsule C APIs complementary, not competing, by sharing one
  behavioral core and minimizing wrapper-specific logic.

File ownership and responsibilities:
- src/_tealet/pytealet.c:
  - Canonical runtime behavior for operations.
  - Shared dispatch/impl blocks that both Python methods and C API wrappers call.
  - Target for state/thread validation and transfer semantics.
- src/_tealet/pytealet_module.c:
  - Capsule table lifecycle and thin forwarding shims only.
  - No semantic forks or bespoke behavior.
- src/_tealet/pytealet_capi.h:
  - Public ABI shape, flags, constants, and import contract.
  - Keep names/flag bits stable once first release is cut.
- src/capi_client/tealet_capi_client.c + tests/test_tealet_capi_client.py:
  - Downstream-consumer validation for ABI and behavior parity.

Implementation harmony checklist (per new API slice):
1. Add or update shared impl/dispatch in pytealet.c first.
2. Wire Python wrapper to that impl.
3. Wire capsule C API wrapper/forwarder to the same impl.
4. Update pytealet_capi.h signatures/constants and keep header/table ordering in
   lockstep.
  (Pre-0.1.0: table/struct reordering is allowed while ABI is unreleased.)
5. Add capi_client coverage and parity tests in the same change.
6. Keep operation-local code grouping so each feature is reviewable end-to-end.

Practical review gates:
- No duplicated state-machine logic between Python and C wrappers.
- No feature implemented only in one surface unless explicitly documented as
  Python-only.
- Error classes/messages may differ at wrapper boundaries, but behavior and
  state transitions must match.
- Every new flag has negative tests for unknown bits and compatibility tests
  against Python behavior when applicable.

## Validation Plan

1. Add capi-client tests for each new C API entrypoint.
2. Add behavior parity tests that compare Python method path vs C API path for:
- state transitions
- thread mismatch errors
- pending throw behavior
- panic mode (after switch_ flags)
3. Add negative tests for bad argument/type cases and remote-error flag semantics.

## Open Decisions

Resolved decisions:
- Throw and switch use uint32_t flags with explicit public constants.
- Module-level helpers beyond thread_sweep are part of the current pre-0.1.0
  surface and may still evolve until release freeze.
- No future need to expose method-style tealet.current()/main()/previous() in the C API.
- Frame/context C API exposure is deferred.

## Recommended Next Slice

1. Keep parity tests in sync with every struct/table reorder (header + module table + capi client assertions).
2. Expand thread_sweep behavior coverage with a dead-thread scenario fixture (not just availability/parity checks).
3. Decide and document the 0.1.0 freeze criteria for C API ordering and feature guarantees.
