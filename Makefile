.PHONY: cext-clean cext-cc cext-cc-debug cext-cc-warnings cext-cc-ci

PY_CC := $(shell uv run python -c "import sysconfig; print((sysconfig.get_config_var('CC') or 'cc').split()[0])")
PY_CFLAGS := $(shell uv run python -c "import sysconfig; print(sysconfig.get_config_var('CFLAGS') or '')")
PY_INCLUDE_FLAGS := $(shell uv run python -c "import sysconfig; p=sysconfig.get_paths(); inc=[p.get('include'), p.get('platinclude')]; print(' '.join('-I'+x for x in inc if x))")

EXT_INCLUDE_FLAGS := -Isrc/_tealet -Isrc/_tealet/libtealet/src -Isrc/_tealet/libtealet/stackman
EXT_BASE_FLAGS := -std=c17 -pedantic-errors -Wall -Wno-unused-function -include $(abspath src/_tealet/pytealet_build_config.h)
EXT_DEBUG_FLAGS := -g -O0 -UNDEBUG
EXT_CI_FLAGS := -Werror
EXT_SRC := src/_tealet/pytealet.c
EXT_OBJ := tmp/build/pytealet.o

cext-clean:
	rm -rf tmp/build

cext-cc: cext-clean
	mkdir -p tmp/build
	$(PY_CC) $(PY_CFLAGS) $(PY_INCLUDE_FLAGS) $(EXT_INCLUDE_FLAGS) $(EXT_BASE_FLAGS) -c $(EXT_SRC) -o $(EXT_OBJ)

cext-cc-debug: cext-clean
	mkdir -p tmp/build
	$(PY_CC) $(PY_CFLAGS) $(PY_INCLUDE_FLAGS) $(EXT_INCLUDE_FLAGS) $(EXT_BASE_FLAGS) $(EXT_DEBUG_FLAGS) -c $(EXT_SRC) -o $(EXT_OBJ)

cext-cc-warnings: cext-clean
	mkdir -p tmp/build
	$(PY_CC) $(PY_CFLAGS) $(PY_INCLUDE_FLAGS) $(EXT_INCLUDE_FLAGS) $(EXT_BASE_FLAGS) -c $(EXT_SRC) -o $(EXT_OBJ) 2>&1 | grep -Ei "warning:|deprecated" || true

cext-cc-ci: cext-clean
	mkdir -p tmp/build
	$(PY_CC) $(PY_CFLAGS) $(PY_INCLUDE_FLAGS) $(EXT_INCLUDE_FLAGS) $(EXT_BASE_FLAGS) $(EXT_CI_FLAGS) -c $(EXT_SRC) -o $(EXT_OBJ)
