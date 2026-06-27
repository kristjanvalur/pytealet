.PHONY: cext-clean cext-cc cext-cc-debug cext-cc-warnings cext-cc-ci check fix format format-check format-c format-c-check lint rebuild-ext test test-compat typecheck

CLANG_FORMAT ?= clang-format-14
EXT_SRC := $(sort $(wildcard src/_tealet/*.c))
EXT_SRC_ALL := $(sort $(EXT_SRC) $(wildcard src/_tealet/*.h))
C_FORMAT_FILES ?= $(EXT_SRC_ALL)

PYTHON ?= $(if $(VIRTUAL_ENV),$(VIRTUAL_ENV)/bin/python,python)
TEST ?= tests/
PYTEST_ARGS ?=
LIBTEALET_MODE ?= release
REBUILD_EXT ?= 0
REBUILD_EXT_TRUE := 1 yes true TRUE
PYTEST_REBUILD_FLAG := $(if $(filter $(REBUILD_EXT),$(REBUILD_EXT_TRUE)),--rebuild-ext,)

PY_CC := $(shell uv run python -c "import sysconfig; print((sysconfig.get_config_var('CC') or 'cc').split()[0])")
PY_CFLAGS := $(shell uv run python -c "import sysconfig; print(sysconfig.get_config_var('CFLAGS') or '')")
PY_INCLUDE_FLAGS := $(shell uv run python -c "import sysconfig; p=sysconfig.get_paths(); inc=[p.get('include'), p.get('platinclude')]; print(' '.join('-I'+x for x in inc if x))")

ifeq ($(LIBTEALET_MODE),source)
EXT_INCLUDE_FLAGS := -Isrc/_tealet -Isrc/_tealet/libtealet-src/src -Isrc/_tealet/libtealet-src/stackman
else
EXT_INCLUDE_FLAGS := -Isrc/_tealet -Isrc/_tealet/libtealet/tealet -Isrc/_tealet/libtealet/stackman
endif
EXT_BASE_FLAGS := -std=c17 -pedantic-errors -Wall -Wno-unused-function -include $(abspath src/_tealet/pytealet_build_config.h)
EXT_DEBUG_FLAGS := -g -O0 -UNDEBUG
EXT_CI_FLAGS := -Werror
EXT_OBJ := $(patsubst src/_tealet/%.c,tmp/build/%.o,$(EXT_SRC))

EXT_MODE_FLAGS :=

tmp/build:
	mkdir -p $@

tmp/build/%.o: src/_tealet/%.c | tmp/build
	$(PY_CC) $(PY_CFLAGS) $(PY_INCLUDE_FLAGS) $(EXT_INCLUDE_FLAGS) $(EXT_BASE_FLAGS) $(EXT_MODE_FLAGS) -c $< -o $@

cext-clean:
	rm -rf tmp/build

cext-cc: cext-clean $(EXT_OBJ)

cext-cc-debug: EXT_MODE_FLAGS := $(EXT_DEBUG_FLAGS)
cext-cc-debug: cext-clean $(EXT_OBJ)

cext-cc-warnings: cext-clean $(EXT_OBJ)
	@$(PY_CC) $(PY_CFLAGS) $(PY_INCLUDE_FLAGS) $(EXT_INCLUDE_FLAGS) $(EXT_BASE_FLAGS) -fsyntax-only $(EXT_SRC) 2>&1 | grep -Ei "warning:|deprecated" || true

cext-cc-ci: EXT_MODE_FLAGS := $(EXT_CI_FLAGS)
cext-cc-ci: cext-clean $(EXT_OBJ)

format:
	uvx ruff format .

format-check:
	uvx ruff format --check .

lint:
	uvx ruff check .

typecheck:
	uv run --all-packages --with ty ty check

check: format-check lint typecheck

fix:
	uvx ruff format .
	uvx ruff check . --fix

format-c:
	$(CLANG_FORMAT) -i $(C_FORMAT_FILES)

format-c-check:
	$(CLANG_FORMAT) --dry-run --Werror $(C_FORMAT_FILES)

rebuild-ext:
	./scripts/fast_build.sh

test:
	$(PYTHON) -m pytest $(TEST) $(PYTEST_REBUILD_FLAG) $(PYTEST_ARGS)

test-compat:
	PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=1 $(PYTHON) -m pytest tests/compat_greenlet $(PYTEST_REBUILD_FLAG) $(PYTEST_ARGS)
