"""Setup script for building the _tealet C extension."""

import os
import platform
import subprocess
import sys
from setuptools import Extension, setup

# Paths to libtealet
LIBTEALET_DIR = "src/_tealet/libtealet"
LIBTEALET_BIN_DIR = "src/_tealet/libtealet-bin"  # Pre-built binaries fallback
LIBTEALET_HEADERS = os.path.join(LIBTEALET_DIR, "src")
STACKMAN_HEADERS = os.path.join(LIBTEALET_DIR, "stackman")
PYTEALET_BUILD_CONFIG_HEADER = os.path.abspath("src/_tealet/pytealet_build_config.h")
DEFAULT_C_STD_FLAGS = "-std=c17 -pedantic-errors"

# Default to source builds, with fallback support for prebuilt binaries.
BUILD_LIBTEALET_FROM_SOURCE = os.environ.get("BUILD_LIBTEALET_FROM_SOURCE", "1") == "1"
LIBTEALET_DEBUG = os.environ.get("LIBTEALET_DEBUG", "1") == "1"
PYTEALET_EXT_DEBUG = os.environ.get("PYTEALET_EXT_DEBUG", "0") == "1"

def get_abi_name():
    """Determine the ABI name for pre-built libraries using libtealet's abiname utility."""
    # Use the Makefile's abiname target to detect platform
    try:
        result = subprocess.run(
            ["make", "-C", LIBTEALET_DIR, "--no-print-directory", "abiname"],
            capture_output=True,
            text=True,
            check=True
        )
        abi = result.stdout.strip()
        if abi:
            return abi
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    
    # Fallback: manual detection
    system = platform.system()
    machine = platform.machine().lower()
    
    if system == "Linux":
        if machine in ("x86_64", "amd64"):
            return "sysv_amd64"
        elif machine in ("i386", "i686"):
            return "sysv_i386"
        elif machine in ("aarch64", "arm64"):
            return "aarch64"
        elif machine.startswith("arm"):
            return "arm32"
        elif machine == "riscv64":
            return "riscv64"
    elif system == "Darwin":
        if machine in ("arm64", "aarch64"):
            return "darwin_arm64"
        elif machine in ("x86_64", "amd64"):
            return "darwin_x86_64"
    elif system == "Windows":
        if machine in ("x86_64", "amd64"):
            return "win_x64"
        elif machine in ("i386", "i686"):
            return "win_x86"
        elif machine in ("arm64", "aarch64"):
            return "win_arm64"
    
    raise RuntimeError(f"Unsupported platform: {system} {machine}")


def build_libtealet_from_source():
    """Build libtealet from source with debug-friendly flags."""
    if not os.path.exists(LIBTEALET_DIR):
        raise RuntimeError(
            f"libtealet source not found at {LIBTEALET_DIR}\n"
            "Clone it with: git clone --depth 1 --branch v0.3.2 "
            "https://github.com/kristjanvalur/libtealet.git src/_tealet/libtealet"
        )

    print(f"Building libtealet from source in {LIBTEALET_DIR}...", file=sys.stderr)
    cflags = f"{DEFAULT_C_STD_FLAGS} -g -O0 -fPIC" if LIBTEALET_DEBUG else f"{DEFAULT_C_STD_FLAGS} -g -O2 -fPIC"

    # Force-include centralized build config for libtealet compilation units.
    if os.path.exists(PYTEALET_BUILD_CONFIG_HEADER):
        cflags += f" -include {PYTEALET_BUILD_CONFIG_HEADER}"

    subprocess.run(["make", "-C", LIBTEALET_DIR, "clean"], check=True)
    subprocess.run(
        ["make", "-C", LIBTEALET_DIR, f"CFLAGS={cflags}", "bin/libtealet.a"],
        check=True,
    )

    abi_result = subprocess.run(
        ["make", "-C", LIBTEALET_DIR, "--no-print-directory", "abiname"],
        capture_output=True,
        text=True,
        check=True,
    )
    abi = abi_result.stdout.strip()
    src_lib = os.path.join(LIBTEALET_DIR, "bin", "libtealet.a")
    if not os.path.exists(src_lib):
        raise RuntimeError(f"Built library not found: {src_lib}")
    return src_lib, abi


# Get the ABI name and library path.
if BUILD_LIBTEALET_FROM_SOURCE:
    libtealet_static, abi_name = build_libtealet_from_source()
else:
    abi_name = get_abi_name()
    lib_dir = os.path.join(LIBTEALET_BIN_DIR, "lib", abi_name)
    if not os.path.exists(lib_dir):
        raise RuntimeError(f"Pre-built libraries not found for ABI: {abi_name} at {lib_dir}")

    libtealet_static = os.path.join(lib_dir, "libtealet.a")
    if not os.path.exists(libtealet_static):
        raise RuntimeError(f"Static library not found: {libtealet_static}")

    # Update header paths for pre-built binaries.
    LIBTEALET_HEADERS = os.path.join(LIBTEALET_BIN_DIR, "tealet")
    STACKMAN_HEADERS = os.path.join(LIBTEALET_BIN_DIR, "stackman")

print(f"Building for ABI: {abi_name}", file=sys.stderr)
if BUILD_LIBTEALET_FROM_SOURCE:
    print(f"Using libtealet built from source: {libtealet_static}", file=sys.stderr)
else:
    print(f"Using pre-built libtealet: {libtealet_static}", file=sys.stderr)

# Source files for the extension (only pytealet.c, link against pre-built libtealet)
sources = [
    "src/_tealet/pytealet.c",
    "src/_tealet/frame_info.c",
    "src/_tealet/tstate_state.c",
]

# Include directories
include_dirs = [
    "src/_tealet",
    LIBTEALET_HEADERS,
    STACKMAN_HEADERS,
]

# Compiler flags
extra_compile_args = []
extra_link_args = []

# Link statically against libtealet
extra_objects = [libtealet_static]

if platform.system() != "Windows":
    # GCC/Clang flags
    extra_compile_args.extend([
        "-std=c17",
        "-pedantic-errors",
        "-Wall",
        "-Wno-unused-function",
    ])

    # Force-include centralized build config for extension compilation.
    if os.path.exists(PYTEALET_BUILD_CONFIG_HEADER):
        extra_compile_args.extend([
            "-include",
            PYTEALET_BUILD_CONFIG_HEADER,
        ])

    if PYTEALET_EXT_DEBUG:
        extra_compile_args.extend([
            "-g",
            "-O0",
            "-UNDEBUG",
        ])

# Define the extension
_tealet_ext = Extension(
    name="_tealet",
    sources=sources,
    include_dirs=include_dirs,
    extra_objects=extra_objects,
    extra_compile_args=extra_compile_args,
    extra_link_args=extra_link_args,
    language="c",
)

# Run setup
setup(
    ext_modules=[_tealet_ext],
)
