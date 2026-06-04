"""Setup script for building the _tealet C extension."""

import os
import platform
import struct
import subprocess
import sys
import sysconfig
from setuptools import Extension, setup


def read_project_version(pyproject_path="pyproject.toml"):
    """Read [project].version from pyproject.toml without extra deps."""
    in_project = False
    with open(pyproject_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("[") and line.endswith("]"):
                in_project = line == "[project]"
                continue
            if in_project and line.startswith("version"):
                _, value = line.split("=", 1)
                version = value.strip().strip('"').strip("'")
                if not version:
                    raise RuntimeError("Empty project version in pyproject.toml")
                return version
    raise RuntimeError("Could not find [project].version in pyproject.toml")

# Paths to libtealet
LIBTEALET_RELEASE_DIR = "src/_tealet/libtealet"
LIBTEALET_SOURCE_DIR = os.environ.get(
    "LIBTEALET_SOURCE_DIR",
    "src/_tealet/libtealet-src",
)
LIBTEALET_HEADERS = os.path.join(LIBTEALET_RELEASE_DIR, "tealet")
STACKMAN_HEADERS = os.path.join(LIBTEALET_RELEASE_DIR, "stackman")
PYTEALET_BUILD_CONFIG_HEADER = os.path.abspath("src/_tealet/pytealet_build_config.h")
DEFAULT_C_STD_FLAGS = "-std=c17 -pedantic-errors"
PROJECT_VERSION = read_project_version()

# Default to release archive builds. Set BUILD_LIBTEALET_FROM_SOURCE=1 to use
# a local source checkout in src/_tealet/libtealet-src.
BUILD_LIBTEALET_FROM_SOURCE = os.environ.get("BUILD_LIBTEALET_FROM_SOURCE", "0") == "1"
LIBTEALET_DEBUG = os.environ.get("LIBTEALET_DEBUG", "1") == "1"
PYTEALET_EXT_DEBUG = os.environ.get("PYTEALET_EXT_DEBUG", "0") == "1"

def get_abi_name(abiname_dir):
    """Determine the ABI name for pre-built libraries using libtealet's abiname utility."""
    system = platform.system()
    plat = (sysconfig.get_platform() or "").lower().replace("_", "-")

    # On Windows, prefer target-Python-driven detection over make/host probes.
    # cibuildwheel can build 32-bit wheels on 64-bit hosts, and host probing may
    # incorrectly select non-Windows or wrong-arch archives.
    if system == "Windows":
        # Cross-compilation toolchains expose explicit target architecture
        # variables (for example via VS developer command prompts).
        target_arch = (
            os.environ.get("VSCMD_ARG_TGT_ARCH", "")
            or os.environ.get("Platform", "")
        ).lower()

        if target_arch in ("arm64", "aarch64"):
            return "win_arm64"
        if target_arch in ("x86", "win32"):
            return "win_x86"
        if target_arch in ("x64", "amd64"):
            return "win_x64"

        # cibuildwheel may expose the resolved build identifier for each wheel.
        build_id = os.environ.get("CIBW_BUILD_IDENTIFIER", "").lower()
        if "win_arm64" in build_id:
            return "win_arm64"
        if "win32" in build_id:
            return "win_x86"
        if "win_amd64" in build_id:
            return "win_x64"

        machine = platform.machine().lower()
        ptr_bits = struct.calcsize("P") * 8

        if "arm64" in plat or machine in ("arm64", "aarch64"):
            return "win_arm64"
        if "win32" in plat or ptr_bits == 32:
            return "win_x86"
        return "win_x64"

    # On macOS, also prefer target-Python detection to avoid host-arch mismatches
    # when cross-building (e.g. x86_64 wheels on Apple Silicon runners).
    if system == "Darwin":
        archflags = os.environ.get("ARCHFLAGS", "").lower()

        if "x86_64" in plat or "x86_64" in archflags:
            return "darwin_x86_64"
        if "arm64" in plat or "aarch64" in plat or "arm64" in archflags:
            return "darwin_arm64"

    # Use the Makefile's abiname target to detect platform
    try:
        result = subprocess.run(
            ["make", "-C", abiname_dir, "--no-print-directory", "abiname"],
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
    if not os.path.exists(LIBTEALET_SOURCE_DIR):
        raise RuntimeError(
            f"libtealet source not found at {LIBTEALET_SOURCE_DIR}\n"
            "Populate it with a local checkout for debugging, for example:\n"
            "  git clone --depth 1 https://github.com/kristjanvalur/libtealet.git "
            "src/_tealet/libtealet-src"
        )

    print(f"Building libtealet from source in {LIBTEALET_SOURCE_DIR}...", file=sys.stderr)
    cflags = f"{DEFAULT_C_STD_FLAGS} -g -O0 -fPIC" if LIBTEALET_DEBUG else f"{DEFAULT_C_STD_FLAGS} -g -O2 -fPIC"

    # Force-include centralized build config for libtealet compilation units.
    if os.path.exists(PYTEALET_BUILD_CONFIG_HEADER):
        cflags += f" -include {PYTEALET_BUILD_CONFIG_HEADER}"

    subprocess.run(["make", "-C", LIBTEALET_SOURCE_DIR, "clean"], check=True)
    subprocess.run(
        ["make", "-C", LIBTEALET_SOURCE_DIR, f"CFLAGS={cflags}", "bin/libtealet.a"],
        check=True,
    )

    abi_result = subprocess.run(
        ["make", "-C", LIBTEALET_SOURCE_DIR, "--no-print-directory", "abiname"],
        capture_output=True,
        text=True,
        check=True,
    )
    abi = abi_result.stdout.strip()
    src_lib = os.path.join(LIBTEALET_SOURCE_DIR, "bin", "libtealet.a")
    if not os.path.exists(src_lib):
        raise RuntimeError(f"Built library not found: {src_lib}")
    return src_lib, abi


# Get the ABI name and library path.
extra_objects = []

if BUILD_LIBTEALET_FROM_SOURCE:
    libtealet_static, abi_name = build_libtealet_from_source()
    LIBTEALET_HEADERS = os.path.join(LIBTEALET_SOURCE_DIR, "src")
    STACKMAN_HEADERS = os.path.join(LIBTEALET_SOURCE_DIR, "stackman")
    extra_objects = [libtealet_static]
else:
    if not os.path.exists(LIBTEALET_RELEASE_DIR):
        raise RuntimeError(f"libtealet release archive not found at: {LIBTEALET_RELEASE_DIR}")

    abi_name = get_abi_name(LIBTEALET_RELEASE_DIR)
    lib_dir = os.path.join(LIBTEALET_RELEASE_DIR, "lib", abi_name)
    if not os.path.exists(lib_dir):
        raise RuntimeError(f"Pre-built libraries not found for ABI: {abi_name} at {lib_dir}")

    # Archive layouts differ by platform:
    # - Unix-like static: libtealet.a
    # - Windows static: tealet_static.lib
    release_lib_candidates = [
        os.path.join(lib_dir, "libtealet.a"),
        os.path.join(lib_dir, "tealet_static.lib"),
    ]
    libtealet_static = None
    for candidate in release_lib_candidates:
        if os.path.exists(candidate):
            libtealet_static = candidate
            break
    if libtealet_static is None:
        raise RuntimeError(
            "No supported libtealet archive found in "
            f"{lib_dir}. Expected one of: {', '.join(release_lib_candidates)}"
        )

    extra_objects = [libtealet_static]

    LIBTEALET_HEADERS = os.path.join(LIBTEALET_RELEASE_DIR, "tealet")
    STACKMAN_HEADERS = os.path.join(LIBTEALET_RELEASE_DIR, "stackman")

print(f"Building for ABI: {abi_name}", file=sys.stderr)
if BUILD_LIBTEALET_FROM_SOURCE:
    print(f"Using libtealet built from source: {libtealet_static}", file=sys.stderr)
else:
    print(f"Using pre-built libtealet: {libtealet_static}", file=sys.stderr)

# Source files for the extension (only pytealet.c, link against pre-built libtealet)
sources = [
    "src/_tealet/pytealet.c",
    "src/_tealet/pytealet_lineage.c",
    "src/_tealet/pytealet_throw.c",
    "src/_tealet/pytealet_module.c",
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
    define_macros=[("PYTEALET_VERSION", f'"{PROJECT_VERSION}"')],
    extra_objects=extra_objects,
    extra_compile_args=extra_compile_args,
    extra_link_args=extra_link_args,
    language="c",
)

# Validation client extension that consumes the _tealet capsule C API.
_tealet_capi_client_ext = Extension(
    name="_tealet_capi_client",
    sources=["src/_tealet/pytealet_capi_client.c"],
    include_dirs=include_dirs,
    extra_compile_args=extra_compile_args,
    extra_link_args=extra_link_args,
    language="c",
)

# Run setup
setup(
    ext_modules=[_tealet_ext, _tealet_capi_client_ext],
)
